import json
import os
from typing import List, Set, Tuple

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMConfig,
    LLMExtractionStrategy,
)

from models.property import Property
from utils.data_utils import is_complete_property, is_duplicate_property


def get_browser_config() -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    # https://docs.crawl4ai.com/core/browser-crawler-config/
    return BrowserConfig(
        browser_type="chromium",  # Type of browser to simulate
        headless=False,  # Whether to run in headless mode (no GUI)
        verbose=True,  # Enable verbose logging
    )


def get_llm_strategy() -> LLMExtractionStrategy:
    """
    Returns the configuration for the language model extraction strategy.

    Returns:
        LLMExtractionStrategy: The settings for how to extract data using LLM.
    """
    # https://docs.crawl4ai.com/api/strategies/#llmextractionstrategy
    return LLMExtractionStrategy(
        llm_config=LLMConfig(
            provider="openrouter/google/gemini-2.5-flash-lite",  # Name of the LLM provider
            api_token=os.getenv("LLM_API_KEY"),  # API token for authentication
        ),
        schema=Property.model_json_schema(),  # JSON schema of the data model
        extraction_type="schema",  # Type of extraction to perform
        instruction=(
            """
            Extract all real estate properties containing city, neighborhood, bedrooms quantity,
            garages quantity, bathrooms quantity, area in square feet, rent price in BRL, condo fee
            in BRL, other fees in BRL, full address, URL of the property, list of image URLs, and
            description.
            """
        ),  # Instructions for the LLM
        input_format="markdown",  # Format of the input content
        chunk_token_threshold=8000,  # Larger chunks to capture more properties
        overlap_rate=0.1,  # 10% overlap between chunks
        apply_chunking=True,  # Enable chunking
        verbose=True,  # Enable verbose logging
    )



async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    base_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: List[str],
    seen_names: Set[str],
) -> List[dict]:
    """
    Fetches and processes property data from the initial page load.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        base_url (str): The base URL of the website.
        css_selector (str): The CSS selector to target the content.
        llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
        session_id (str): The session identifier.
        required_keys (List[str]): List of required keys in the property data.
        seen_names (Set[str]): Set of property names that have already been seen.

    Returns:
        List[dict]: A list of processed properties from the page.
    """
    url = f"{base_url}"
    print("Loading initial page...")

    # Fetch page content with the extraction strategy
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,  # Do not use cached data
            extraction_strategy=llm_strategy,  # Strategy for data extraction
            css_selector=css_selector,  # Target specific content on the page
            session_id=session_id,  # Unique session ID for the crawl
            wait_for="""js:() => {
                return window.allButtonsClicked === true;
            }""",  # Wait for all load-more buttons to be clicked
            js_code="""
            (async () => {
                console.log("Starting sequential load-more button clicking...");

                // Wait for property container to appear
                const waitForPropertyContainer = () => {
                    return new Promise(resolve => {
                        const check = setInterval(() => {
                            if (document.querySelector('.property-component')) {
                                clearInterval(check);
                                resolve();
                            }
                        }, 200);
                    });
                };

                await waitForPropertyContainer();
                console.log('.property-container found. Starting sequential clicks.');

                // Click load-more buttons sequentially until none remain
                let clickCount = 0;
                while (true) {
                    const btn = document.querySelector('.load-more');
                    if (!btn) {
                        console.log(`No more .load-more buttons found after ${clickCount} clicks.`);
                        break;
                    }

                    console.log(`Clicking load-more button #${clickCount + 1}...`);
                    btn.click();
                    clickCount++;

                    // Wait for new content to load (adjust timing as needed)
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }

                // Wait a bit more for final content to render
                await new Promise(resolve => setTimeout(resolve, 3000));

                // Count properties in DOM
                const propertyCount = document.querySelectorAll('[class^="property-component"]').length;
                console.log(`Total properties found in DOM: ${propertyCount}`);

                // Signal completion
                window.allButtonsClicked = true;
                console.log('All load-more buttons clicked. Ready for extraction.');
            })();
            """,  # JS code to click all load-more buttons before extraction
        ),
    )

    if not (result.success and result.extracted_content):
        print(f"Error fetching page: {result.error_message}")
        return []

    # Parse extracted content
    extracted_data = json.loads(result.extracted_content)

    # Save raw extracted JSON to file
    json_output_path = "extracted_data.json"
    with open(json_output_path, "w", encoding="utf-8") as f:
        json.dump(extracted_data, f, indent=2, ensure_ascii=False)
    print(f"Saved raw extracted JSON to '{json_output_path}'")

    if not extracted_data:
        print("\n=== Filtering Summary ===")
        print("Total extracted: 0")
        print("No properties found on the page.")
        return []

    total_extracted = len(extracted_data)

    # Process properties
    complete_properties = []
    for property in extracted_data:
        # Ignore the 'error' key if it's False
        if property.get("error") is False:
            property.pop("error", None)  # Remove the 'error' key if it's False

        if not is_complete_property(property, required_keys):
            continue  # Skip incomplete properties

        if is_duplicate_property(property["full_address"], seen_names):
            print(f"Duplicate property '{property['full_address']}' found. Skipping.")
            continue  # Skip duplicate properties

        # Add property to the list
        seen_names.add(property["full_address"])
        complete_properties.append(property)

    # Print filtering summary
    print("\n=== Filtering Summary ===")
    print(f"Total extracted: {total_extracted}")
    print(f"After filtering: {len(complete_properties)}")
    print(f"Removed:         {total_extracted - len(complete_properties)}")

    return complete_properties
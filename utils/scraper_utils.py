import json
import re
from typing import List, Optional, Set, Union

from bs4 import BeautifulSoup
from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    JsonCssExtractionStrategy,
    LLMExtractionStrategy,
)

from config.site_config import SiteConfig
from utils.data_utils import (
    get_property_unique_key,
    is_complete_property,
    is_duplicate_property,
)


def get_browser_config(site_config: Optional[SiteConfig] = None) -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Args:
        site_config: Optional site configuration. If provided, uses its browser settings.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    if site_config and site_config.browser:
        browser = site_config.browser
        config_kwargs = {
            "browser_type": browser.browser_type,
            "headless": browser.headless,
            "verbose": browser.verbose,
        }
        if browser.viewport_width:
            config_kwargs["viewport_width"] = browser.viewport_width
        if browser.viewport_height:
            config_kwargs["viewport_height"] = browser.viewport_height
        return BrowserConfig(**config_kwargs)

    # Default configuration
    return BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=True,
    )


def get_cache_mode(site_config: SiteConfig) -> CacheMode:
    """
    Get the cache mode from site configuration.

    Args:
        site_config: The site configuration.

    Returns:
        CacheMode: The cache mode to use.
    """
    # Check listing_scraping.setup.cache_mode
    if (
        site_config.listing_scraping
        and site_config.listing_scraping.setup
        and site_config.listing_scraping.setup.cache_mode
    ):
        cache_mode_str = site_config.listing_scraping.setup.cache_mode
    else:
        cache_mode_str = "bypass"

    mode_map = {
        "enabled": CacheMode.ENABLED,
        "disabled": CacheMode.DISABLED,
        "bypass": CacheMode.BYPASS,
        "read_only": CacheMode.READ_ONLY,
        "write_only": CacheMode.WRITE_ONLY,
    }
    return mode_map.get(cache_mode_str, CacheMode.BYPASS)


def parse_number(text: str) -> float:
    """
    Parses a Brazilian number format to float.
    E.g., "R$ 3.100,00" -> 3100.00, "69 m²" -> 69.0

    Args:
        text: Text containing a number in Brazilian format.

    Returns:
        float: The parsed number, or 0.0 if parsing fails.
    """
    if not text:
        return 0.0
    # Remove currency symbols and unit markers (R$, m², m2)
    cleaned = re.sub(r"R\$|m²|m2", "", text, flags=re.IGNORECASE)
    # Keep only digits, dots, and commas
    cleaned = re.sub(r"[^\d.,]", "", cleaned)
    # Convert Brazilian format (1.000,00) to standard (1000.00)
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_integer(text: str) -> int:
    """
    Extracts the first integer from text.
    E.g., "2 quartos" -> 2, "1 vaga" -> 1

    Args:
        text: Text containing an integer.

    Returns:
        int: The parsed integer, or 0 if parsing fails.
    """
    if not text:
        return 0
    match = re.search(r"\d+", text)
    return int(match.group()) if match else 0


def transform_property(
    raw_property: dict, site_config: Optional[SiteConfig] = None
) -> dict:
    """
    Transforms raw CSS-extracted data into the final property format.

    Args:
        raw_property: Raw property data from CSS extraction.
        site_config: Optional site configuration for custom transforms.

    Returns:
        dict: Transformed property with correct types and field names.
    """
    # Apply custom transforms if configured in output.transform
    if (
        site_config
        and site_config.listing_scraping
        and site_config.listing_scraping.output
        and site_config.listing_scraping.output.transform
    ):
        return _apply_custom_transforms(raw_property, site_config)

    # Default transformation logic (for backward compatibility)
    return _default_transform(raw_property)


def _default_transform(raw_property: dict) -> dict:
    """Apply the default property transformation."""
    # Use already-extracted fields if available, otherwise parse from address_others
    neighborhood = raw_property.get("neighborhood", "")
    city = raw_property.get("city", "")

    # Fallback: parse from address_others (format: "Neighborhood, City")
    if not neighborhood or not city:
        address_others = raw_property.get("address_others", "")
        parts = [p.strip() for p in address_others.split(",") if p.strip()]
        if not neighborhood:
            neighborhood = parts[0] if parts else ""
        if not city:
            city = parts[1] if len(parts) > 1 else ""

    street = raw_property.get("street", "")
    full_address = f"{street}, {neighborhood}, {city}".strip(", ")

    # Parse numeric fields
    rent_price = parse_number(raw_property.get("rent_price_text", ""))
    condo_fee = parse_number(raw_property.get("condo_fee_text", ""))
    area = parse_number(raw_property.get("area_text", ""))
    bedrooms = parse_integer(raw_property.get("bedrooms_text", ""))
    bathrooms = parse_integer(raw_property.get("bathrooms_text", ""))
    garages = parse_integer(raw_property.get("garages_text", ""))

    # Get image URLs - normalize to list and deduplicate
    image_urls = raw_property.get("image_urls", [])
    if isinstance(image_urls, str):
        image_urls = [image_urls] if image_urls else []
    elif isinstance(image_urls, list):
        image_urls = list(dict.fromkeys(image_urls))  # Remove duplicates, preserve order
    else:
        image_urls = []

    return {
        "city": city,
        "neighborhood": neighborhood,
        "bedrooms": bedrooms,
        "garages": garages,
        "bathrooms": bathrooms,
        "area_sqft": area,
        "rent_price_brl": rent_price,
        "condo_fee_brl": condo_fee,
        "other_fees_brl": 0.0,  # Not available in listing
        "full_address": full_address,
        "property_url": raw_property.get("property_url", ""),
        "image_urls": image_urls,
        "description": raw_property.get(
            "property_type", ""
        ),  # Use property type as basic description
    }


def _apply_custom_transforms(raw_property: dict, site_config: SiteConfig) -> dict:
    """Apply custom transformations from site configuration."""
    result = dict(raw_property)
    transform_list = site_config.listing_scraping.output.transform

    # The transform list can contain transformation rules
    # For now, fall back to default transform since the structure is simplified
    return _default_transform(raw_property)


def _extract_images_from_html(
    html: str, base_selector: str, image_selector: str
) -> List[List[str]]:
    """Extract images from HTML using BeautifulSoup.

    Workaround for crawl4ai's JsonCssExtractionStrategy not handling multiple: true correctly.

    Args:
        html: The page HTML content.
        base_selector: The CSS selector for property cards.
        image_selector: The CSS selector for images within each card.

    Returns:
        List of image URL lists, one per property card.
    """
    soup = BeautifulSoup(html, "html.parser")
    property_cards = soup.select(base_selector)

    all_images = []
    for card in property_cards:
        images = card.select(image_selector)
        urls = []
        for img in images:
            src = img.get("src") or img.get("data-lazy") or img.get("data-src")
            if src and not src.startswith("data:"):
                urls.append(src)
        # Deduplicate while preserving order
        urls = list(dict.fromkeys(urls))
        all_images.append(urls)

    return all_images


async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    url: str,
    css_selector: str,
    extraction_strategy: Union[JsonCssExtractionStrategy, LLMExtractionStrategy],
    session_id: str,
    required_keys: List[str],
    seen_names: Set[str],
    site_config: Optional[SiteConfig] = None,
) -> List[dict]:
    """
    Fetches and processes property data from the initial page load.

    Args:
        crawler: The web crawler instance.
        url: The URL to crawl.
        css_selector: The CSS selector to target the content.
        extraction_strategy: The extraction strategy to use.
        session_id: The session identifier.
        required_keys: List of required keys in the property data.
        seen_names: Set of property names that have already been seen.
        site_config: Optional site configuration for custom behavior.

    Returns:
        List[dict]: A list of processed properties from the page.
    """
    print(f"Loading page: {url[:80]}...")

    # Build crawler run config
    config_kwargs = {
        "cache_mode": get_cache_mode(site_config) if site_config else CacheMode.BYPASS,
        "extraction_strategy": extraction_strategy,
        "css_selector": css_selector,
        "session_id": session_id,
    }

    # Get listing scraping config
    listing_config = site_config.listing_scraping if site_config else None
    setup_config = listing_config.setup if listing_config else None
    pagination_config = listing_config.pagination if listing_config else None

    # Add wait_for from setup config
    if setup_config and setup_config.wait_for:
        wait_for = setup_config.wait_for
        if wait_for.css:
            config_kwargs["wait_for"] = f"css:{wait_for.css}"
        elif wait_for.js:
            config_kwargs["wait_for"] = f"js:{wait_for.js}"
        elif wait_for.time:
            config_kwargs["wait_for"] = f"time:{wait_for.time}"

    # Add page_timeout from setup config
    if setup_config:
        config_kwargs["page_timeout"] = setup_config.page_timeout

    # For JS-based pagination, use js_code and wait_for from pagination config
    if pagination_config and pagination_config.type == "js":
        if pagination_config.js_code:
            config_kwargs["js_code"] = pagination_config.js_code
        # Override wait_for with pagination's wait_for (required for JS pagination)
        if pagination_config.wait_for:
            wait_for = pagination_config.wait_for
            if wait_for.css:
                config_kwargs["wait_for"] = f"css:{wait_for.css}"
            elif wait_for.js:
                config_kwargs["wait_for"] = f"js:{wait_for.js}"
            elif wait_for.time:
                config_kwargs["wait_for"] = f"time:{wait_for.time}"

    # Run pre-extraction interactions from setup config
    if setup_config and setup_config.interactions:
        js_code_parts = []
        for interaction in setup_config.interactions:
            if interaction.type == "click" and interaction.selector:
                js_code_parts.append(
                    f"document.querySelector('{interaction.selector}')?.click();"
                )
                if interaction.wait_after_ms > 0:
                    js_code_parts.append(
                        f"await new Promise(r => setTimeout(r, {interaction.wait_after_ms}));"
                    )
            elif interaction.type == "js" and interaction.code:
                js_code_parts.append(interaction.code)
                if interaction.wait_after_ms > 0:
                    js_code_parts.append(
                        f"await new Promise(r => setTimeout(r, {interaction.wait_after_ms}));"
                    )

        if js_code_parts:
            # If there's already js_code from pagination, prepend interactions
            existing_js = config_kwargs.get("js_code", "")
            interaction_js = (
                "(async () => {\n" + "\n".join(js_code_parts) + "\n})();"
            )
            if existing_js:
                config_kwargs["js_code"] = interaction_js + "\n" + existing_js
            else:
                config_kwargs["js_code"] = interaction_js

    # Fetch page content
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(**config_kwargs),
    )

    if not (result.success and result.extracted_content):
        # Check if it's a wait_for timeout (likely means no results on page)
        if result.error_message and "Wait condition failed" in result.error_message:
            return []  # Silently return empty - pagination will stop
        print(f"Error fetching page: {result.error_message}")
        return []

    # Parse extracted content
    extracted_data = json.loads(result.extracted_content)

    # Workaround for crawl4ai's multiple:true bug - extract images separately using BeautifulSoup
    if listing_config and listing_config.extraction.type == "css":
        base_selector = listing_config.extraction.base_selector
        # Find the image field configuration
        for field in listing_config.extraction.fields:
            if field.name == "image_urls" and field.multiple:
                image_lists = _extract_images_from_html(
                    result.html, base_selector, field.selector
                )
                # Merge images back into extracted data
                for i, images in enumerate(image_lists):
                    if i < len(extracted_data):
                        extracted_data[i]["image_urls"] = images
                break


    if not extracted_data:
        print("\n=== Filtering Summary ===")
        print("Total extracted: 0")
        print("No properties found on the page.")
        return []

    total_extracted = len(extracted_data)

    # Transform and process properties
    complete_properties = []
    for raw_property in extracted_data:
        # Transform raw CSS data to final format
        property_data = transform_property(raw_property, site_config)

        if not is_complete_property(property_data, required_keys):
            continue

        if is_duplicate_property(property_data, seen_names):
            continue

        seen_names.add(get_property_unique_key(property_data))
        complete_properties.append(property_data)

    # Print filtering summary
    print("\n=== Filtering Summary ===")
    print(f"Total extracted: {total_extracted}")
    print(f"After filtering: {len(complete_properties)}")
    print(f"Removed:         {total_extracted - len(complete_properties)}")

    return complete_properties

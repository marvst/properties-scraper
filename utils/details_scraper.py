import asyncio
import json
from typing import Dict, List
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig
from rich.console import Console

from config.site_config import SiteConfig
from utils.extraction_factory import create_extraction_strategy
from utils.scraper_utils import get_browser_config, parse_integer, parse_number

console = Console()


def _post_process_llm_extracted_details(details: Dict, property_data: Dict) -> Dict:
    """
    Post-process LLM-extracted details data.

    Converts text fee values to numeric values and maps fields to the
    expected property model format.

    Args:
        details: The LLM-extracted details dictionary.
        property_data: The original property data from listing page.

    Returns:
        Enhanced property dictionary with processed fee data.
    """
    enhanced = {**property_data, **details}

    # Parse fee text values to numeric
    if details.get("condo_fee_text"):
        enhanced["condo_fee_brl"] = parse_number(details["condo_fee_text"])

    if details.get("iptu_text"):
        enhanced["iptu_brl"] = parse_number(details["iptu_text"])

    if details.get("fire_insurance_text"):
        # Add fire insurance to other_fees_brl
        fire_insurance = parse_number(details["fire_insurance_text"])
        existing_other_fees = enhanced.get("other_fees_brl", 0.0)
        if isinstance(existing_other_fees, (int, float)):
            enhanced["other_fees_brl"] = existing_other_fees + fire_insurance
        else:
            enhanced["other_fees_brl"] = fire_insurance

    # Parse area values if provided
    if details.get("total_area_text"):
        area_value = parse_number(details["total_area_text"])
        if area_value > 0:
            enhanced["area_sqft"] = area_value

    if details.get("private_area_text"):
        enhanced["private_area_sqft"] = parse_number(details["private_area_text"])

    if details.get("area_text"):
        area_value = parse_number(details["area_text"])
        if area_value > 0:
            enhanced["area_sqft"] = area_value

    # Parse room counts if provided (override only if current value is 0 or missing)
    if details.get("bedrooms_text"):
        bedrooms_value = parse_integer(details["bedrooms_text"])
        if bedrooms_value > 0 and enhanced.get("bedrooms", 0) == 0:
            enhanced["bedrooms"] = bedrooms_value

    if details.get("bathrooms_text"):
        bathrooms_value = parse_integer(details["bathrooms_text"])
        if bathrooms_value > 0 and enhanced.get("bathrooms", 0) == 0:
            enhanced["bathrooms"] = bathrooms_value

    if details.get("garages_text"):
        garages_value = parse_integer(details["garages_text"])
        if garages_value > 0 and enhanced.get("garages", 0) == 0:
            enhanced["garages"] = garages_value

    # Override address if details page has better data
    if details.get("full_address"):
        full_addr = details["full_address"]
        enhanced["full_address"] = full_addr

        # Try to parse address components from full_address
        # Format is typically: "Street, Number - Neighborhood, City - State"
        addr_parts = full_addr.split(" - ")
        if len(addr_parts) >= 2:
            # Last part is usually "City - State" or just neighborhood/city
            location_part = addr_parts[-1] if len(addr_parts) > 1 else ""
            location_parts = [p.strip() for p in location_part.split(",")]

            if location_parts:
                # Try to extract city (usually after the neighborhood)
                if len(location_parts) >= 2:
                    enhanced["neighborhood"] = location_parts[0]
                    enhanced["city"] = location_parts[1]
                elif len(addr_parts) >= 2:
                    # Might be: "Street - Neighborhood, City"
                    neighborhood_city = addr_parts[1].split(",")
                    if len(neighborhood_city) >= 2:
                        enhanced["neighborhood"] = neighborhood_city[0].strip()
                        enhanced["city"] = neighborhood_city[1].strip()

    # Handle description
    if details.get("full_description"):
        enhanced["description"] = details["full_description"]

    # Handle amenities (convert list to comma-separated string if needed)
    if details.get("amenities"):
        amenities = details["amenities"]
        if isinstance(amenities, list):
            enhanced["amenities"] = ", ".join(amenities)
        else:
            enhanced["amenities"] = str(amenities)

    return enhanced


class PropertyDetailsScraper:
    """
    Scrapes detailed information from individual property pages.

    This class takes a list of property URLs and scrapes detailed information
    from each property's details page, then merges this data with existing
    property information.
    """

    def __init__(self, site_config: SiteConfig):
        """
        Initialize the details scraper.

        Args:
            site_config: The site configuration containing details scraping settings.
        """
        self.site_config = site_config
        self.details_config = site_config.details_scraping

        if not self.details_config or not self.details_config.enabled:
            raise ValueError("Details scraping is not enabled in site configuration")

        if not self.details_config.extraction:
            raise ValueError("Details scraping extraction config is required")

        # Type assertion since we checked above
        assert self.details_config is not None

        # Initialize browser and extraction settings
        self.browser_config = get_browser_config(site_config)
        self.extraction_strategy = create_extraction_strategy(
            self.details_config.extraction
        )

        # Get concurrency settings from setup config
        self.setup_config = self.details_config.setup
        if self.setup_config and self.setup_config.concurrency:
            self.max_concurrent_requests = self.setup_config.concurrency.max_requests
            self.request_delay_ms = self.setup_config.concurrency.delay_ms
            self.timeout_per_page = self.setup_config.concurrency.timeout_per_page
        else:
            # Defaults
            self.max_concurrent_requests = 2
            self.request_delay_ms = 1000
            self.timeout_per_page = 30000

        # Get cache mode from setup config
        if self.setup_config:
            self.cache_mode = self._get_cache_mode(self.setup_config.cache_mode)
            if self.setup_config.interactions:
                console.print(f"[dim green]Loaded {len(self.setup_config.interactions)} interactions from config[/dim green]")
        else:
            self.cache_mode = CacheMode.BYPASS

    def _get_cache_mode(self, mode_str: str) -> CacheMode:
        """Convert string cache mode to CacheMode enum."""
        mode_map = {
            "enabled": CacheMode.ENABLED,
            "disabled": CacheMode.DISABLED,
            "bypass": CacheMode.BYPASS,
            "read_only": CacheMode.READ_ONLY,
            "write_only": CacheMode.WRITE_ONLY,
        }
        return mode_map.get(mode_str, CacheMode.BYPASS)

    def _extract_all_images_from_html(self, html: str) -> List[str]:
        """Extract all images from details page HTML, including lazy-loaded.

        Uses the image selectors from the extraction config (images field).
        Supports two modes:
        1. CSS selector mode: selector + attribute
        2. Regex mode: pattern (extracts URLs matching regex from raw HTML)

        Args:
            html: The raw HTML content of the details page.

        Returns:
            List of image URLs, deduplicated and in order of appearance.
        """
        import re

        soup = BeautifulSoup(html, "html.parser")
        urls = []

        # Get image selectors from extraction config (paired array format)
        if (
            self.details_config.extraction
            and self.details_config.extraction.images
        ):
            for image_config in self.details_config.extraction.images:
                # Regex mode: extract URLs matching pattern from raw HTML
                if image_config.pattern:
                    matches = re.findall(image_config.pattern, html)
                    console.print(f"[dim blue]Regex '{image_config.pattern[:50]}...': found {len(matches)} matches[/dim blue]")
                    for match in matches:
                        if match and match not in urls:
                            urls.append(match)
                # CSS selector mode: use selector + attribute
                elif image_config.selector:
                    selector = image_config.selector
                    attribute = image_config.attribute

                    elements = soup.select(selector)
                    console.print(f"[dim blue]Selector '{selector}' attr '{attribute}': found {len(elements)} elements[/dim blue]")
                    for el in elements:
                        src = el.get(attribute)
                        if src and not src.startswith("data:") and src not in urls:
                            urls.append(src)
        else:
            console.print("[dim red]No image selectors configured[/dim red]")

        return urls

    async def scrape_property_details(
        self, properties: List[Dict], session_id: str = "details_scraping"
    ) -> List[Dict]:
        """
        Scrape detailed information for multiple properties.

        Args:
            properties: List of property dictionaries with 'property_url' keys.
            session_id: Session identifier for the scraping operation.

        Returns:
            List of enhanced property dictionaries with details data merged in.
        """
        if not properties:
            console.print(
                "[yellow]No properties provided for details scraping.[/yellow]"
            )
            return properties

        # Filter properties that have valid URLs
        valid_properties = [
            prop
            for prop in properties
            if prop.get("property_url") and isinstance(prop.get("property_url"), str)
        ]

        if not valid_properties:
            console.print(
                "[yellow]No properties with valid URLs found for details scraping.[/yellow]"
            )
            return properties

        console.print(
            f"[blue]Starting details scraping for {len(valid_properties)} properties...[/blue]"
        )

        # Create semaphore for concurrent request limiting
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        # Process properties concurrently with rate limiting
        enhanced_properties = await self._scrape_properties_concurrent(
            valid_properties, semaphore, session_id
        )

        # Merge back with original properties (preserving order and non-scraped properties)
        result_map = {prop["property_url"]: prop for prop in enhanced_properties}
        final_properties = []

        for original_prop in properties:
            url = original_prop.get("property_url")
            if url in result_map:
                final_properties.append(result_map[url])
            else:
                final_properties.append(original_prop)

        console.print(
            f"[green]Details scraping completed. Enhanced {len(enhanced_properties)} properties.[/green]"
        )
        return final_properties

    async def _scrape_properties_concurrent(
        self, properties: List[Dict], semaphore: asyncio.Semaphore, session_id: str
    ) -> List[Dict]:
        """
        Scrape property details concurrently with rate limiting.
        """

        async def scrape_single_property(prop: Dict) -> Dict:
            async with semaphore:
                try:
                    enhanced_prop = await self._scrape_single_property(prop, session_id)
                    # Add delay between requests
                    await asyncio.sleep(self.request_delay_ms / 1000)
                    return enhanced_prop
                except Exception as e:
                    console.print(
                        f"[red]Error scraping {prop.get('property_url')}: {e}[/red]"
                    )
                    return prop  # Return original if scraping fails

        # Create tasks for concurrent scraping
        tasks = [scrape_single_property(prop) for prop in properties]

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and return successful results
        enhanced_properties: List[Dict] = []
        for result in results:
            if not isinstance(result, Exception):
                enhanced_properties.append(result)

        return enhanced_properties

    async def _scrape_single_property(
        self, property_data: Dict, session_id: str
    ) -> Dict:
        """
        Scrape detailed information from a single property page.

        Args:
            property_data: Property dictionary with 'property_url'.
            session_id: Session identifier.

        Returns:
            Enhanced property dictionary with details merged in.
        """
        url = property_data["property_url"]

        # Make URL absolute if it's relative
        if not url.startswith(("http://", "https://")):
            parsed_site_url = urlparse(self.site_config.url)
            base_url = (
                self.site_config.base_url
                or f"{parsed_site_url.scheme}://{parsed_site_url.netloc}"
            )
            url = urljoin(base_url, url)

        console.print(f"[dim]Scraping details: {url[:60]}...[/dim]")

        # Configure crawler run
        run_config = CrawlerRunConfig(
            cache_mode=self.cache_mode,
            extraction_strategy=self.extraction_strategy,
            session_id=f"{session_id}_{hash(url)}",
        )

        # Add page timeout from setup config
        if self.setup_config:
            run_config.page_timeout = self.setup_config.page_timeout

        # Add wait_for setting from setup config
        if self.setup_config and self.setup_config.wait_for:
            wait_for = self.setup_config.wait_for
            if wait_for.css:
                run_config.wait_for = f"css:{wait_for.css}"
            elif wait_for.js:
                run_config.wait_for = f"js:{wait_for.js}"
            elif wait_for.time:
                run_config.wait_for = f"time:{wait_for.time}"

        # Run pre-extraction interactions if configured
        if self.setup_config and self.setup_config.interactions:
            js_code_parts = []
            for interaction in self.setup_config.interactions:
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
                run_config.js_code = "(async () => {\n" + "\n".join(js_code_parts) + "\n})();"
                console.print(f"[dim yellow]Executing JS interactions: {run_config.js_code[:200]}...[/dim yellow]")

        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            result = await crawler.arun(url=url, config=run_config)

            if not result.success:
                console.print(
                    f"[red]Failed to scrape {url}: {result.error_message}[/red]"
                )
                return property_data

            if not result.extracted_content:
                console.print(f"[yellow]No content extracted from {url}[/yellow]")
                return property_data

            # Parse extracted details
            try:
                details_data = json.loads(result.extracted_content)

                # Debug: show what LLM extracted
                console.print(
                    f"[dim cyan]LLM extracted: {json.dumps(details_data, indent=2, ensure_ascii=False)[:500]}...[/dim cyan]"
                )

                # For now, assume single property extraction (not a list)
                if isinstance(details_data, list) and details_data:
                    details = details_data[0]
                elif isinstance(details_data, dict):
                    details = details_data
                else:
                    console.print(
                        f"[yellow]Unexpected extracted data format for {url}[/yellow]"
                    )
                    return property_data

                # Merge and post-process details into property data
                # Use post-processing for LLM-extracted data (handles fee parsing, address, etc.)
                enhanced_property = _post_process_llm_extracted_details(
                    details, property_data
                )

                # Debug: show key fields after post-processing
                console.print(
                    f"[dim magenta]After processing: condo_fee_brl={enhanced_property.get('condo_fee_brl')}, iptu_brl={enhanced_property.get('iptu_brl')}, neighborhood={enhanced_property.get('neighborhood')}, city={enhanced_property.get('city')}[/dim magenta]"
                )

                # Extract images from raw HTML (handles lazy-loaded images)
                if result.html:
                    # Debug: save HTML to file for inspection
                    debug_html_path = "/tmp/claude/-home-marcos-repos-marcos-vou-pra-curitiba-scraper/ba5fbb34-9d19-4786-9002-98e5de4925e6/scratchpad/debug_page.html"
                    import os
                    os.makedirs(os.path.dirname(debug_html_path), exist_ok=True)
                    with open(debug_html_path, "w", encoding="utf-8") as f:
                        f.write(result.html)
                    console.print(f"[dim yellow]Saved HTML to {debug_html_path}[/dim yellow]")

                    all_images = self._extract_all_images_from_html(result.html)
                    console.print(f"[dim cyan]Found {len(all_images)} images from HTML[/dim cyan]")
                    if all_images:
                        enhanced_property["additional_images"] = all_images
                        console.print(f"[dim cyan]Sample images: {all_images[:3]}[/dim cyan]")

                console.print(f"[dim green]Enhanced property: {url[:60]}...[/dim green]")
                return enhanced_property

            except json.JSONDecodeError as e:
                console.print(
                    f"[red]Failed to parse extracted content from {url}: {e}[/red]"
                )
                return property_data

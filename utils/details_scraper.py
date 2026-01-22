import asyncio
import json
from typing import Dict, List

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from rich.console import Console

from config.site_config import SiteConfig
from utils.extraction_factory import create_extraction_strategy
from utils.scraper_utils import get_browser_config, get_cache_mode

console = Console()


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
        self.extraction_strategy = create_extraction_strategy(self.details_config.extraction)
        self.cache_mode = get_cache_mode(site_config)

    def _extract_all_images_from_html(self, html: str) -> List[str]:
        """Extract all images from details page HTML, including lazy-loaded.

        Args:
            html: The raw HTML content of the details page.

        Returns:
            List of image URLs, deduplicated and in order of appearance.
        """
        soup = BeautifulSoup(html, 'html.parser')
        urls = []

        # Use configured selectors, or fallback to extraction config
        selectors = self.details_config.image_selectors
        if not selectors and self.details_config.extraction and self.details_config.extraction.css:
            # Get selector from CSS extraction config for additional_images field
            for field in self.details_config.extraction.css.fields:
                if field.name == "additional_images":
                    selectors = [field.selector]
                    break

        if not selectors:
            return urls  # No selectors configured

        # Get configured attributes or use defaults
        attributes = self.details_config.image_attributes or ["src", "data-lazy", "data-src"]

        for selector in selectors:
            elements = soup.select(selector)
            for el in elements:
                for attr in attributes:
                    src = el.get(attr)
                    if src and not src.startswith('data:') and src not in urls:
                        urls.append(src)
                        break  # Found a URL for this element, move to next

        return urls

    async def scrape_property_details(
        self,
        properties: List[Dict],
        session_id: str = "details_scraping"
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
            console.print("[yellow]No properties provided for details scraping.[/yellow]")
            return properties

        # Filter properties that have valid URLs
        valid_properties = [
            prop for prop in properties
            if prop.get('property_url') and isinstance(prop.get('property_url'), str)
        ]

        if not valid_properties:
            console.print("[yellow]No properties with valid URLs found for details scraping.[/yellow]")
            return properties

        console.print(f"[blue]Starting details scraping for {len(valid_properties)} properties...[/blue]")

        # Create semaphore for concurrent request limiting
        semaphore = asyncio.Semaphore(self.details_config.max_concurrent_requests)

        # Process properties concurrently with rate limiting
        enhanced_properties = await self._scrape_properties_concurrent(
            valid_properties, semaphore, session_id
        )

        # Merge back with original properties (preserving order and non-scraped properties)
        result_map = {prop['property_url']: prop for prop in enhanced_properties}
        final_properties = []

        for original_prop in properties:
            url = original_prop.get('property_url')
            if url in result_map:
                final_properties.append(result_map[url])
            else:
                final_properties.append(original_prop)

        console.print(f"[green]Details scraping completed. Enhanced {len(enhanced_properties)} properties.[/green]")
        return final_properties

    async def _scrape_properties_concurrent(
        self,
        properties: List[Dict],
        semaphore: asyncio.Semaphore,
        session_id: str
    ) -> List[Dict]:
        """
        Scrape property details concurrently with rate limiting.
        """
        async def scrape_single_property(prop: Dict) -> Dict:
            async with semaphore:
                try:
                    enhanced_prop = await self._scrape_single_property(prop, session_id)
                    # Add delay between requests
                    await asyncio.sleep(self.details_config.request_delay_ms / 1000)
                    return enhanced_prop
                except Exception as e:
                    console.print(f"[red]Error scraping {prop.get('property_url')}: {e}[/red]")
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
        self,
        property_data: Dict,
        session_id: str
    ) -> Dict:
        """
        Scrape detailed information from a single property page.

        Args:
            property_data: Property dictionary with 'property_url'.
            session_id: Session identifier.

        Returns:
            Enhanced property dictionary with details merged in.
        """
        url = property_data['property_url']
        console.print(f"[dim]Scraping details: {url[:60]}...[/dim]")

        # Configure crawler run
        run_config = CrawlerRunConfig(
            cache_mode=self.cache_mode,
            extraction_strategy=self.extraction_strategy,
            session_id=f"{session_id}_{hash(url)}",
        )

        # Add timing settings if configured
        if self.site_config.timing:
            timing = self.site_config.timing
            run_config.page_timeout = timing.page_timeout
            if timing.delay_before_return_html > 0:
                run_config.delay_before_return_html = timing.delay_before_return_html

        # Add wait_for setting if configured
        if self.details_config.wait_for:
            run_config.wait_for = self.details_config.wait_for

        # Add js_code setting if configured
        if self.details_config.js_code:
            run_config.js_code = self.details_config.js_code

        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            result = await crawler.arun(url=url, config=run_config)

            if not result.success:
                console.print(f"[red]Failed to scrape {url}: {result.error_message}[/red]")
                return property_data

            if not result.extracted_content:
                console.print(f"[yellow]No content extracted from {url}[/yellow]")
                return property_data

            # Parse extracted details
            try:
                details_data = json.loads(result.extracted_content)

                # For now, assume single property extraction (not a list)
                if isinstance(details_data, list) and details_data:
                    details = details_data[0]
                elif isinstance(details_data, dict):
                    details = details_data
                else:
                    console.print(f"[yellow]Unexpected extracted data format for {url}[/yellow]")
                    return property_data

                # Merge details into property data
                enhanced_property = {**property_data, **details}

                # Extract images from raw HTML (handles lazy-loaded images)
                if result.html:
                    all_images = self._extract_all_images_from_html(result.html)
                    if all_images:
                        enhanced_property['additional_images'] = all_images

                console.print(f"[dim green]Enhanced property: {url[:60]}...[/dim green]")
                return enhanced_property

            except json.JSONDecodeError as e:
                console.print(f"[red]Failed to parse extracted content from {url}: {e}[/red]")

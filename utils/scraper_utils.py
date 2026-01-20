import json
import re
from datetime import datetime
from pathlib import Path
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
from utils.data_utils import get_property_unique_key, is_complete_property, is_duplicate_property


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
    if site_config.cache is None:
        return CacheMode.BYPASS

    mode_map = {
        "enabled": CacheMode.ENABLED,
        "disabled": CacheMode.DISABLED,
        "bypass": CacheMode.BYPASS,
        "read_only": CacheMode.READ_ONLY,
        "write_only": CacheMode.WRITE_ONLY,
    }
    return mode_map.get(site_config.cache.mode, CacheMode.BYPASS)


def get_css_extraction_schema() -> dict:
    """
    Returns the CSS extraction schema for property data.
    This is kept for backward compatibility.

    Returns:
        dict: Schema mapping CSS selectors to property fields.
    """
    return {
        "name": "properties",
        "baseSelector": ".property-component",
        "fields": [
            {"name": "property_type", "selector": ".property-type", "type": "text"},
            {"name": "street", "selector": ".property-street", "type": "text"},
            {"name": "address_others", "selector": ".property-address-others", "type": "text"},
            {"name": "garages_text", "selector": ".feature.car", "type": "text"},
            {"name": "bedrooms_text", "selector": ".feature.bed", "type": "text"},
            {"name": "area_text", "selector": ".feature.ruler", "type": "text"},
            {"name": "bathrooms_text", "selector": ".feature.bw", "type": "text"},
            {"name": "rent_price_text", "selector": ".property-current-price", "type": "text"},
            {"name": "condo_fee_text", "selector": ".property-codominum-price", "type": "text"},
            {"name": "property_url", "selector": "a.info-area-wrapper", "type": "attribute", "attribute": "href"},
            {
                "name": "image_urls",
                "selector": ".slick-slide:not(.slick-cloned) img",
                "type": "attribute",
                "attribute": "src",
                "multiple": True,
            },
        ],
    }


def get_css_extraction_strategy() -> JsonCssExtractionStrategy:
    """
    Returns the CSS extraction strategy for property data.
    This is kept for backward compatibility.

    Returns:
        JsonCssExtractionStrategy: The strategy for extracting data using CSS selectors.
    """
    return JsonCssExtractionStrategy(schema=get_css_extraction_schema())


def parse_number(text: str) -> float:
    """
    Parses a Brazilian number format to float.
    E.g., "R$ 3.100,00" -> 3100.00, "69 m2" -> 69.0

    Args:
        text: Text containing a number in Brazilian format.

    Returns:
        float: The parsed number, or 0.0 if parsing fails.
    """
    if not text:
        return 0.0
    # Remove currency symbols, unit markers, and text
    cleaned = re.sub(r"[R$m2\s]", "", text)
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


def transform_property(raw_property: dict, site_config: Optional[SiteConfig] = None) -> dict:
    """
    Transforms raw CSS-extracted data into the final property format.

    Args:
        raw_property: Raw property data from CSS extraction.
        site_config: Optional site configuration for custom transforms.

    Returns:
        dict: Transformed property with correct types and field names.
    """
    # Apply custom transforms if configured
    if site_config and site_config.transform and site_config.transform.enabled:
        return _apply_custom_transforms(raw_property, site_config)

    # Default transformation logic (for backward compatibility)
    return _default_transform(raw_property)


def _default_transform(raw_property: dict) -> dict:
    """Apply the default property transformation."""
    # Parse address components
    address_others = raw_property.get("address_others", "")
    parts = [p.strip() for p in address_others.replace(",", " ").split() if p.strip()]
    neighborhood = parts[0] if parts else ""
    city = parts[-1] if len(parts) > 1 else parts[0] if parts else ""

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
        "description": raw_property.get("property_type", ""),  # Use property type as basic description
    }


def _apply_custom_transforms(raw_property: dict, site_config: SiteConfig) -> dict:
    """Apply custom transformations from site configuration."""
    result = dict(raw_property)
    transform = site_config.transform

    # Apply numeric field transformations
    for num_field in transform.numeric_fields:
        source_value = raw_property.get(num_field.source, "")
        if num_field.format == "brazilian_currency":
            result[num_field.name] = parse_number(source_value)
        elif num_field.format == "integer":
            result[num_field.name] = parse_integer(source_value)
        else:  # float
            result[num_field.name] = parse_number(source_value)

    # Apply computed fields
    for computed in transform.computed_fields:
        try:
            result[computed.name] = computed.template.format(**result)
        except KeyError:
            result[computed.name] = ""

    # Deduplicate specified fields
    for field_name in transform.deduplicate_fields:
        if field_name in result and isinstance(result[field_name], list):
            result[field_name] = list(dict.fromkeys(result[field_name]))

    return result


def _extract_images_from_html(html: str, base_selector: str, image_selector: str) -> List[List[str]]:
    """Extract images from HTML using BeautifulSoup.

    Workaround for crawl4ai's JsonCssExtractionStrategy not handling multiple: true correctly.

    Args:
        html: The page HTML content.
        base_selector: The CSS selector for property cards.
        image_selector: The CSS selector for images within each card.

    Returns:
        List of image URL lists, one per property card.
    """
    soup = BeautifulSoup(html, 'html.parser')
    property_cards = soup.select(base_selector)

    all_images = []
    for card in property_cards:
        images = card.select(image_selector)
        urls = []
        for img in images:
            src = img.get('src') or img.get('data-lazy') or img.get('data-src')
            if src and not src.startswith('data:'):
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

    # Add interaction settings from config
    if site_config and site_config.interaction:
        interaction = site_config.interaction
        if interaction.wait_for:
            config_kwargs["wait_for"] = interaction.wait_for
        if interaction.js_code:
            config_kwargs["js_code"] = interaction.js_code

    # Add timing settings
    if site_config and site_config.timing:
        timing = site_config.timing
        config_kwargs["page_timeout"] = timing.page_timeout
        if timing.delay_before_return_html > 0:
            config_kwargs["delay_before_return_html"] = timing.delay_before_return_html

    # Fetch page content
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(**config_kwargs),
    )

    if not (result.success and result.extracted_content):
        print(f"Error fetching page: {result.error_message}")
        return []

    # Parse extracted content
    extracted_data = json.loads(result.extracted_content)

    # Workaround for crawl4ai's multiple:true bug - extract images separately using BeautifulSoup
    if site_config and site_config.extraction.css:
        base_selector = site_config.extraction.css.base_selector
        # Find the image field configuration
        for field in site_config.extraction.css.fields:
            if field.name == "image_urls" and field.multiple:
                image_lists = _extract_images_from_html(result.html, base_selector, field.selector)
                # Merge images back into extracted data
                for i, images in enumerate(image_lists):
                    if i < len(extracted_data):
                        extracted_data[i]["image_urls"] = images
                break

    # Save raw extracted JSON to extractions folder with timestamp
    extractions_dir = Path("extractions")
    extractions_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    site_name = site_config.name if site_config else "unknown"
    json_output_path = extractions_dir / f"{site_name}_{timestamp}.json"

    with open(json_output_path, "w", encoding="utf-8") as f:
        json.dump(extracted_data, f, indent=2, ensure_ascii=False)
    print(f"Saved raw extracted JSON to '{json_output_path}'")

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

"""Data models for syncing to vou-pra-curitiba Rails database."""

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urljoin


@dataclass
class RailsProperty:
    """Property model matching the Rails schema."""

    external_id: str
    source: str
    city: Optional[str] = None
    neighborhood: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    parking_spaces: Optional[int] = None
    area_sqm: Optional[float] = None
    rent_price: Optional[float] = None
    condo_fee: Optional[float] = None
    total_price: Optional[float] = None
    address: Optional[str] = None
    original_url: Optional[str] = None
    main_image_url: Optional[str] = None
    description: Optional[str] = None
    raw_data: dict = field(default_factory=dict)
    status: str = "active"


def _generate_external_id(data: dict, source: str) -> str:
    """Generate a unique external_id for a property.

    Uses SHA256 hash of source:url:area:price, truncated to 32 chars.
    """
    url = data.get("property_url", "")
    area = str(data.get("area_sqft", ""))
    price = str(data.get("rent_price_brl", ""))

    hash_input = f"{source}:{url}:{area}:{price}"
    hash_bytes = hashlib.sha256(hash_input.encode()).hexdigest()
    return hash_bytes[:32]


def from_procrawl(data: dict, source: str, base_url: str) -> RailsProperty:
    """Convert a procrawl property dict to a RailsProperty.

    Args:
        data: Property dict from procrawl extraction
        source: Source name (e.g., "apolar", "galvao")
        base_url: Base URL for resolving relative URLs

    Returns:
        RailsProperty instance ready for database insertion
    """
    # Generate external_id
    external_id = _generate_external_id(data, source)

    # Handle URL - add base if relative
    property_url = data.get("property_url", "")
    if property_url and not property_url.startswith(("http://", "https://")):
        original_url = urljoin(base_url, property_url)
    else:
        original_url = property_url

    # Get image URLs - handle both string and list formats
    raw_image_urls = data.get("image_urls", [])
    if isinstance(raw_image_urls, str):
        image_urls = [raw_image_urls] if raw_image_urls else []
    else:
        image_urls = raw_image_urls or []
    main_image_url = image_urls[0] if image_urls else None

    # Calculate total price
    rent_price = data.get("rent_price_brl")
    condo_fee = data.get("condo_fee_brl")
    total_price = None
    if rent_price is not None:
        total_price = rent_price + (condo_fee or 0)

    # Build raw_data with image_urls and additional_images
    raw_data = {}
    if image_urls:
        raw_data["image_urls"] = image_urls
    additional_images = data.get("additional_images", [])
    if additional_images:
        raw_data["additional_images"] = additional_images

    return RailsProperty(
        external_id=external_id,
        source=source,
        city=data.get("city"),
        neighborhood=data.get("neighborhood"),
        bedrooms=_to_int(data.get("bedrooms")),
        bathrooms=_to_int(data.get("bathrooms")),
        parking_spaces=_to_int(data.get("garages")),
        area_sqm=_to_float(data.get("area_sqft")),
        rent_price=_to_float(rent_price),
        condo_fee=_to_float(condo_fee),
        total_price=_to_float(total_price),
        address=data.get("full_address"),
        original_url=original_url,
        main_image_url=main_image_url,
        description=data.get("description"),
        raw_data=raw_data,
        status="active",
    )


def _to_int(value: Any) -> Optional[int]:
    """Convert a value to int, returning None if not possible."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _to_float(value: Any) -> Optional[float]:
    """Convert a value to float, returning None if not possible."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

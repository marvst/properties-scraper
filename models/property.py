from typing import Optional

from pydantic import BaseModel


class Property(BaseModel):
    """
    Represents the data structure of a Property.
    """

    city: str
    neighborhood: str
    bedrooms: int
    garages: int
    bathrooms: int
    area_sqft: float
    rent_price_brl: float
    condo_fee_brl: float
    other_fees_brl: float
    full_address: str
    property_url: str
    image_urls: list[str]
    description: Optional[str] = None

    # Additional fields from details scraping
    full_description: Optional[str] = None
    amenities: Optional[list[str]] = None
    additional_images: Optional[list[str]] = None
    year_built: Optional[int] = None
    floor_number: Optional[int] = None
    total_floors: Optional[int] = None
    property_tax_brl: Optional[float] = None
    iptu_brl: Optional[float] = None
    accepts_pets: Optional[bool] = None
    furnished: Optional[bool] = None
    virtual_tour_url: Optional[str] = None
    floor_plan_url: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None


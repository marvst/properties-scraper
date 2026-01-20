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
    description: str


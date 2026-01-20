import csv

from models.property import Property


def get_property_unique_key(property_data: dict) -> str:
    """
    Creates a unique key for a property based on address, price, and area.
    This allows different units at the same address to be kept.
    """
    address = property_data.get("full_address", "")
    price = property_data.get("rent_price_brl", 0)
    area = property_data.get("area_sqft", 0)
    return f"{address}|{price}|{area}"


def is_duplicate_property(property_data: dict, seen_keys: set) -> bool:
    key = get_property_unique_key(property_data)
    return key in seen_keys


def is_complete_property(property: dict, required_keys: list) -> bool:
    return all(key in property for key in required_keys)


def save_results_to_csv(properties: list, filename: str):
    if not properties:
        print("No properties to save.")
        return

    # Use field names from the Venue model
    fieldnames = Property.model_fields.keys()

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(properties)
    print(f"Saved {len(properties)} properties to '{filename}'.")
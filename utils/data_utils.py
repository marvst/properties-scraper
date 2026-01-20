import csv

from models.property import Property


def is_duplicate_property(property_address: str, seen_addresses: set) -> bool:
    return property_address in seen_addresses


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
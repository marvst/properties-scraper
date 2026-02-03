"""API-based synchronization for vou-pra-curitiba Rails database."""

import os
import time
from typing import Any

import requests

from database.models import from_procrawl


class ApiSync:
    """Syncs procrawl results to vou-pra-curitiba via HTTP API."""

    def __init__(self, source: str, base_url: str):
        """Initialize the API syncer.

        Args:
            source: Source name (e.g., "apolar", "galvao")
            base_url: Base URL for resolving relative property URLs
        """
        self.source = source
        self.base_url = base_url
        self.api_url = os.environ.get("VPC_API_URL")
        self.api_key = os.environ.get("VPC_API_KEY")

        if not self.api_url:
            raise ValueError("VPC_API_URL environment variable is not set")
        if not self.api_key:
            raise ValueError("VPC_API_KEY environment variable is not set")

    def sync_properties(self, properties: list[dict]) -> dict:
        """Sync a list of properties to the database via API.

        Args:
            properties: List of property dicts from procrawl extraction

        Returns:
            Dict with sync statistics: {added, updated, found, removed}
        """
        # Convert properties to API format
        api_properties = []
        for prop_data in properties:
            rails_prop = from_procrawl(prop_data, self.source, self.base_url)
            api_properties.append(self._property_to_dict(rails_prop))

        # Send to API
        payload = {
            "source": self.source,
            "base_url": self.base_url,
            "properties": api_properties,
        }

        response = self._send_with_retry(payload)
        response.raise_for_status()

        result = response.json()
        if result.get("status") == "error":
            raise RuntimeError(f"API sync failed: {result.get('error')}")

        stats = result.get("statistics", {})
        return {
            "added": stats.get("added", 0),
            "updated": stats.get("updated", 0),
            "found": stats.get("found", 0),
            "removed": stats.get("removed", 0),
        }

    def _property_to_dict(self, prop: Any) -> dict:
        """Convert a RailsProperty to a dict for API transmission."""
        return {
            "external_id": prop.external_id,
            "city": prop.city,
            "neighborhood": prop.neighborhood,
            "bedrooms": prop.bedrooms,
            "bathrooms": prop.bathrooms,
            "parking_spaces": prop.parking_spaces,
            "area_sqm": prop.area_sqm,
            "rent_price": prop.rent_price,
            "condo_fee": prop.condo_fee,
            "total_price": prop.total_price,
            "address": prop.address,
            "original_url": prop.original_url,
            "main_image_url": prop.main_image_url,
            "description": prop.description,
            "raw_data": prop.raw_data,
        }

    def _send_with_retry(
        self,
        payload: dict,
        max_retries: int = 3,
        initial_delay: float = 1.0,
    ) -> requests.Response:
        """Send payload to API with exponential backoff on 5xx errors.

        Args:
            payload: JSON payload to send
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds between retries

        Returns:
            Response object from successful request

        Raises:
            requests.HTTPError: If all retries fail or non-retryable error
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        delay = initial_delay
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                response = requests.post(
                    self.api_url,
                    json=payload,
                    headers=headers,
                    timeout=120,
                )

                # Don't retry on client errors (4xx)
                if 400 <= response.status_code < 500:
                    response.raise_for_status()

                # Retry on server errors (5xx)
                if response.status_code >= 500:
                    if attempt < max_retries:
                        time.sleep(delay)
                        delay *= 2
                        continue
                    response.raise_for_status()

                return response

            except requests.RequestException as e:
                last_exception = e
                if attempt < max_retries:
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise

        raise last_exception  # type: ignore

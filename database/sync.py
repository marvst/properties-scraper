"""Database synchronization for vou-pra-curitiba Rails database."""

import json
from datetime import datetime
from typing import Any

from database.connection import close_connection, get_connection
from database.models import RailsProperty, from_procrawl


class DatabaseSync:
    """Syncs procrawl results to vou-pra-curitiba SQLite database."""

    def __init__(self, source: str, base_url: str):
        """Initialize the database syncer.

        Args:
            source: Source name (e.g., "apolar", "galvao")
            base_url: Base URL for resolving relative property URLs
        """
        self.source = source
        self.base_url = base_url
        self.conn = None
        self.sync_log_id = None

    def sync_properties(self, properties: list[dict]) -> dict:
        """Sync a list of properties to the database.

        Args:
            properties: List of property dicts from procrawl extraction

        Returns:
            Dict with sync statistics: {added, updated, found}
        """
        stats = {"added": 0, "updated": 0, "found": len(properties)}
        seen_external_ids = []

        try:
            self.conn = get_connection()
            self._start_sync_log()

            for prop_data in properties:
                rails_prop = from_procrawl(prop_data, self.source, self.base_url)
                seen_external_ids.append(rails_prop.external_id)
                result = self._upsert_property(rails_prop)
                stats[result] += 1

            # Mark properties not seen in this sync as removed
            self._mark_removed_properties(seen_external_ids)

            self._finish_sync_log("completed", None, stats)
            self.conn.commit()

        except Exception as e:
            if self.conn:
                self.conn.rollback()
            self._finish_sync_log("failed", str(e), stats)
            raise

        finally:
            if self.conn:
                close_connection(self.conn)

        return stats

    def _start_sync_log(self) -> None:
        """Create a sync_logs entry with status='running'."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor = self.conn.execute(
            """
            INSERT INTO sync_logs (source, status, started_at, created_at, updated_at)
            VALUES (?, 'running', ?, ?, ?)
            """,
            (self.source, now, now, now),
        )
        self.sync_log_id = cursor.lastrowid

    def _finish_sync_log(self, status: str, error: str | None, stats: dict) -> None:
        """Complete the sync_logs entry."""
        if not self.sync_log_id or not self.conn:
            return

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.conn.execute(
            """
            UPDATE sync_logs
            SET status = ?, finished_at = ?, updated_at = ?,
                properties_found = ?, properties_added = ?, properties_updated = ?,
                error_message = ?
            WHERE id = ?
            """,
            (
                status,
                now,
                now,
                stats.get("found", 0),
                stats.get("added", 0),
                stats.get("updated", 0),
                error,
                self.sync_log_id,
            ),
        )

    def _upsert_property(self, prop: RailsProperty) -> str:
        """Insert or update a property based on external_id and source.

        Returns:
            'added' if inserted, 'updated' if updated
        """
        cursor = self.conn.execute(
            """
            SELECT id, rent_price, condo_fee
            FROM properties
            WHERE external_id = ? AND source = ?
            """,
            (prop.external_id, prop.source),
        )
        existing = cursor.fetchone()

        if existing:
            self._update_property(existing, prop)
            return "updated"
        else:
            self._insert_property(prop)
            return "added"

    def _insert_property(self, prop: RailsProperty) -> None:
        """Insert a new property with first_seen_at=now."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raw_data_json = json.dumps(prop.raw_data) if prop.raw_data else None

        self.conn.execute(
            """
            INSERT INTO properties (
                external_id, source, city, neighborhood, bedrooms, bathrooms,
                parking_spaces, area_sqm, rent_price, condo_fee, total_price,
                address, original_url, main_image_url, description, raw_data,
                status, first_seen_at, last_seen_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                prop.external_id,
                prop.source,
                prop.city,
                prop.neighborhood,
                prop.bedrooms,
                prop.bathrooms,
                prop.parking_spaces,
                prop.area_sqm,
                prop.rent_price,
                prop.condo_fee,
                prop.total_price,
                prop.address,
                prop.original_url,
                prop.main_image_url,
                prop.description,
                raw_data_json,
                prop.status,
                now,
                now,
                now,
                now,
            ),
        )

    def _update_property(self, existing: Any, prop: RailsProperty) -> None:
        """Update an existing property with last_seen_at=now.

        Records price history if price changed.
        """
        property_id = existing["id"]
        old_rent = existing["rent_price"]
        old_condo = existing["condo_fee"]

        # Check for price changes and record history
        if self._price_changed(old_rent, old_condo, prop.rent_price, prop.condo_fee):
            old_total = (old_rent or 0) + (old_condo or 0)
            self._record_price_history(property_id, old_rent, old_condo, old_total)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raw_data_json = json.dumps(prop.raw_data) if prop.raw_data else None

        self.conn.execute(
            """
            UPDATE properties SET
                city = ?, neighborhood = ?, bedrooms = ?, bathrooms = ?,
                parking_spaces = ?, area_sqm = ?, rent_price = ?, condo_fee = ?,
                total_price = ?, address = ?, original_url = ?, main_image_url = ?,
                description = ?, raw_data = ?, status = ?, last_seen_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                prop.city,
                prop.neighborhood,
                prop.bedrooms,
                prop.bathrooms,
                prop.parking_spaces,
                prop.area_sqm,
                prop.rent_price,
                prop.condo_fee,
                prop.total_price,
                prop.address,
                prop.original_url,
                prop.main_image_url,
                prop.description,
                raw_data_json,
                "active",
                now,
                now,
                property_id,
            ),
        )

    def _price_changed(
        self,
        old_rent: float | None,
        old_condo: float | None,
        new_rent: float | None,
        new_condo: float | None,
    ) -> bool:
        """Check if price has changed."""
        return old_rent != new_rent or old_condo != new_condo

    def _record_price_history(
        self,
        property_id: int,
        rent_price: float | None,
        condo_fee: float | None,
        total_price: float | None,
    ) -> None:
        """Insert a price history record."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.conn.execute(
            """
            INSERT INTO price_histories (
                property_id, rent_price, condo_fee, total_price,
                recorded_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (property_id, rent_price, condo_fee, total_price, now, now, now),
        )

    def _mark_removed_properties(self, seen_external_ids: list[str]) -> None:
        """Mark properties not seen in this sync as 'removed'."""
        if not seen_external_ids:
            return

        placeholders = ",".join("?" * len(seen_external_ids))
        self.conn.execute(
            f"""
            UPDATE properties
            SET status = 'removed'
            WHERE source = ? AND status = 'active'
            AND external_id NOT IN ({placeholders})
            """,
            [self.source] + seen_external_ids,
        )

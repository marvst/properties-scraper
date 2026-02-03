"""Database module for syncing to vou-pra-curitiba Rails database."""

import os

from database.sync import DatabaseSync


def get_syncer(source: str, base_url: str):
    """Returns ApiSync if VPC_API_URL is set, else DatabaseSync.

    Args:
        source: Source name (e.g., "apolar", "galvao")
        base_url: Base URL for resolving relative property URLs

    Returns:
        Either ApiSync or DatabaseSync instance
    """
    if os.environ.get("VPC_API_URL"):
        from database.api_sync import ApiSync

        return ApiSync(source=source, base_url=base_url)
    return DatabaseSync(source=source, base_url=base_url)


__all__ = ["DatabaseSync", "get_syncer"]

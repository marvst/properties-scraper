from pathlib import Path
from typing import List, Optional

import yaml

from config.site_config import (
    BrowserConfig,
    ConcurrencyConfig,
    DefaultsConfig,
    DetailsScrapingConfig,
    DetailsSetupConfig,
    ListingScrapingConfig,
    OutputConfig,
    PaginationConfig,
    SetupConfig,
    SiteConfig,
)

DEFAULT_SITES_DIR = "sites"


def load_sites_config(sites_dir: Optional[str] = None) -> List[SiteConfig]:
    """
    Load and validate all site configurations from YAML files in the sites directory.

    Args:
        sites_dir: Path to the directory containing site YAML files. Defaults to 'sites'.

    Returns:
        List[SiteConfig]: List of validated site configuration objects.

    Raises:
        FileNotFoundError: If the sites directory doesn't exist.
        ValueError: If no valid site configurations are found.
    """
    sites_path = Path(sites_dir) if sites_dir else Path(DEFAULT_SITES_DIR)

    if not sites_path.exists():
        raise FileNotFoundError(f"Sites directory not found: {sites_path}")

    if not sites_path.is_dir():
        raise ValueError(f"Path is not a directory: {sites_path}")

    site_configs = []

    # Load all .yaml and .yml files from the sites directory
    for yaml_file in sites_path.glob("*.yaml"):
        try:
            with open(yaml_file, encoding="utf-8") as f:
                raw_config = yaml.safe_load(f)

            if not raw_config:
                continue

            # Each file should contain a complete site configuration
            site_config = SiteConfig(**raw_config)
            site_configs.append(site_config)

        except Exception as e:
            raise ValueError(f"Error loading site config from {yaml_file}: {e}")

    if not site_configs:
        raise ValueError(f"No valid site configurations found in {sites_path}")

    return site_configs


def get_site_config(site_name: str, sites_dir: Optional[str] = None) -> SiteConfig:
    """
    Get configuration for a specific site by name.

    Args:
        site_name: The name/identifier of the site.
        sites_dir: Path to the directory containing site YAML files.

    Returns:
        SiteConfig: The site configuration with defaults merged in.

    Raises:
        ValueError: If the site is not found or is disabled.
    """
    sites_path = Path(sites_dir) if sites_dir else Path(DEFAULT_SITES_DIR)
    site_file = sites_path / f"{site_name}.yaml"

    if not site_file.exists():
        # List available sites for error message
        available_sites = list_sites(sites_dir)
        available_names = [site["name"] for site in available_sites]
        raise ValueError(
            f"Site '{site_name}' not found. Available sites: {available_names}"
        )

    try:
        with open(site_file, encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)

        if not raw_config:
            raise ValueError(f"Empty configuration file: {site_file}")

        # Extract defaults if present
        defaults_config = None
        if "defaults" in raw_config:
            defaults_config = DefaultsConfig(**raw_config["defaults"])
            # Remove defaults from site config
            site_raw_config = {k: v for k, v in raw_config.items() if k != "defaults"}
        else:
            site_raw_config = raw_config

        site = SiteConfig(**site_raw_config)

        if not site.enabled:
            raise ValueError(f"Site '{site_name}' is disabled.")

        # Apply defaults merging
        return _merge_defaults(site, defaults_config)

    except Exception as e:
        raise ValueError(f"Error loading site config for '{site_name}': {e}")


def _merge_defaults(site: SiteConfig, defaults: Optional[DefaultsConfig]) -> SiteConfig:
    """
    Merge default configuration values into a site configuration.

    Args:
        site: The site configuration.
        defaults: The default configuration values.

    Returns:
        SiteConfig: Site configuration with defaults applied.
    """
    site_data = site.model_dump()

    # Merge browser defaults
    if site.browser is None:
        if defaults and defaults.browser is not None:
            site_data["browser"] = defaults.browser.model_dump()
        else:
            site_data["browser"] = BrowserConfig().model_dump()

    # Apply defaults for listing_scraping sub-sections
    if site.listing_scraping:
        listing_data = site_data["listing_scraping"]

        # Ensure setup exists with defaults
        if site.listing_scraping.setup is None:
            listing_data["setup"] = SetupConfig().model_dump()

        # Ensure pagination exists with defaults (type="none" by default)
        if site.listing_scraping.pagination is None:
            listing_data["pagination"] = PaginationConfig().model_dump()

        # Ensure output exists with defaults
        if site.listing_scraping.output is None:
            listing_data["output"] = OutputConfig().model_dump()

    # Apply defaults for details_scraping sub-sections
    if site.details_scraping is None:
        site_data["details_scraping"] = DetailsScrapingConfig().model_dump()
    elif site.details_scraping.setup is None:
        details_data = site_data["details_scraping"]
        details_data["setup"] = DetailsSetupConfig().model_dump()
        # Ensure concurrency defaults
        if details_data["setup"].get("concurrency") is None:
            details_data["setup"]["concurrency"] = ConcurrencyConfig().model_dump()

    return SiteConfig(**site_data)


def list_sites(sites_dir: Optional[str] = None) -> list[dict]:
    """
    List all available sites from the sites directory.

    Args:
        sites_dir: Path to the directory containing site YAML files.

    Returns:
        list[dict]: List of site info dicts with name, enabled status, and URL.
    """
    sites_path = Path(sites_dir) if sites_dir else Path(DEFAULT_SITES_DIR)

    if not sites_path.exists():
        return []

    sites = []

    # Load all .yaml files from the sites directory
    for yaml_file in sites_path.glob("*.yaml"):
        try:
            with open(yaml_file, encoding="utf-8") as f:
                raw_config = yaml.safe_load(f)

            if not raw_config:
                continue

            # Extract basic info without full validation
            site_info = {
                "name": raw_config.get("name", yaml_file.stem),
                "enabled": raw_config.get("enabled", True),
                "url": raw_config.get("url", ""),
            }

            # Truncate URL if too long
            if len(site_info["url"]) > 60:
                site_info["url"] = site_info["url"][:60] + "..."

            sites.append(site_info)

        except Exception:
            # Skip files that can't be parsed
            continue

    return sites

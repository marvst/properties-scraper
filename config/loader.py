from pathlib import Path
from typing import Optional

import yaml

from config.site_config import (
    BrowserConfig,
    CacheConfig,
    DataConfig,
    DefaultsConfig,
    InteractionConfig,
    SiteConfig,
    SitesConfig,
    TimingConfig,
    TransformConfig,
)

DEFAULT_CONFIG_FILE = "sites.yaml"


def load_sites_config(config_path: Optional[str] = None) -> SitesConfig:
    """
    Load and validate the sites configuration from a YAML file.

    Args:
        config_path: Path to the YAML config file. Defaults to 'sites.yaml'.

    Returns:
        SitesConfig: Validated configuration object.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        ValueError: If the config file is invalid.
    """
    path = Path(config_path) if config_path else Path(DEFAULT_CONFIG_FILE)

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path, encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    if not raw_config:
        raise ValueError(f"Empty configuration file: {path}")

    return SitesConfig(**raw_config)


def get_site_config(
    site_name: str, config_path: Optional[str] = None
) -> SiteConfig:
    """
    Get configuration for a specific site by name.

    Args:
        site_name: The name/identifier of the site.
        config_path: Path to the YAML config file.

    Returns:
        SiteConfig: The site configuration with defaults merged in.

    Raises:
        ValueError: If the site is not found or is disabled.
    """
    config = load_sites_config(config_path)

    # Find the site
    site = None
    for s in config.sites:
        if s.name == site_name:
            site = s
            break

    if site is None:
        available = [s.name for s in config.sites]
        raise ValueError(
            f"Site '{site_name}' not found. Available sites: {available}"
        )

    if not site.enabled:
        raise ValueError(f"Site '{site_name}' is disabled.")

    # Merge defaults into site config
    return _merge_defaults(site, config.defaults)


def _merge_defaults(site: SiteConfig, defaults: Optional[DefaultsConfig]) -> SiteConfig:
    """
    Merge default configuration values into a site configuration.

    Args:
        site: The site configuration.
        defaults: The default configuration values.

    Returns:
        SiteConfig: Site configuration with defaults applied.
    """
    if defaults is None:
        # Apply hardcoded defaults if no defaults section
        return _apply_hardcoded_defaults(site)

    # Create a copy of the site config data
    site_data = site.model_dump()

    # Merge browser defaults
    if site.browser is None and defaults.browser is not None:
        site_data["browser"] = defaults.browser.model_dump()
    elif site.browser is None:
        site_data["browser"] = BrowserConfig().model_dump()

    # Merge timing defaults
    if site.timing is None and defaults.timing is not None:
        site_data["timing"] = defaults.timing.model_dump()
    elif site.timing is None:
        site_data["timing"] = TimingConfig().model_dump()

    # Apply other hardcoded defaults
    if site.cache is None:
        site_data["cache"] = CacheConfig().model_dump()

    if site.data is None:
        site_data["data"] = DataConfig().model_dump()

    if site.transform is None:
        site_data["transform"] = TransformConfig().model_dump()

    if site.interaction is None:
        site_data["interaction"] = InteractionConfig().model_dump()

    return SiteConfig(**site_data)


def _apply_hardcoded_defaults(site: SiteConfig) -> SiteConfig:
    """Apply hardcoded defaults when no defaults section exists."""
    site_data = site.model_dump()

    if site.browser is None:
        site_data["browser"] = BrowserConfig().model_dump()

    if site.timing is None:
        site_data["timing"] = TimingConfig().model_dump()

    if site.cache is None:
        site_data["cache"] = CacheConfig().model_dump()

    if site.data is None:
        site_data["data"] = DataConfig().model_dump()

    if site.transform is None:
        site_data["transform"] = TransformConfig().model_dump()

    if site.interaction is None:
        site_data["interaction"] = InteractionConfig().model_dump()

    return SiteConfig(**site_data)


def list_sites(config_path: Optional[str] = None) -> list[dict]:
    """
    List all available sites from the configuration.

    Args:
        config_path: Path to the YAML config file.

    Returns:
        list[dict]: List of site info dicts with name, enabled status, and URL.
    """
    config = load_sites_config(config_path)

    return [
        {
            "name": site.name,
            "enabled": site.enabled,
            "url": site.url[:60] + "..." if len(site.url) > 60 else site.url,
        }
        for site in config.sites
    ]

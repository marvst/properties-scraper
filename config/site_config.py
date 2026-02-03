from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator


class BrowserConfig(BaseModel):
    """Browser configuration settings."""

    browser_type: Literal["chromium", "firefox", "webkit"] = "chromium"
    headless: bool = True
    verbose: bool = False
    viewport_width: Optional[int] = None
    viewport_height: Optional[int] = None


# --- New Flat Structure Models ---


class WaitForConfig(BaseModel):
    """Wait condition - at least one must be set."""

    css: Optional[str] = None
    js: Optional[str] = None
    time: Optional[int] = None


class InteractionAction(BaseModel):
    """Single pre-extraction interaction."""

    type: Literal["click", "js"]
    selector: Optional[str] = None  # For click type
    code: Optional[str] = None  # For js type
    wait_after_ms: int = 0


class SetupConfig(BaseModel):
    """Setup configuration for listing scraping."""

    wait_for: Optional[WaitForConfig] = None
    page_timeout: int = 60000
    cache_mode: Literal["enabled", "disabled", "bypass", "read_only", "write_only"] = (
        "bypass"
    )
    interactions: list[InteractionAction] = Field(default_factory=list)


class CssField(BaseModel):
    """CSS field extraction definition."""

    name: str
    selector: str
    type: Literal["text", "attribute"] = "text"
    attribute: Optional[str] = None
    multiple: bool = False


class ImageSelector(BaseModel):
    """Image selector with attribute for LLM extraction.

    Supports two modes:
    1. CSS selector mode: selector + attribute (default)
    2. Regex mode: pattern (extracts URLs matching regex from raw HTML)
    """

    selector: Optional[str] = None
    attribute: str = "src"
    pattern: Optional[str] = None  # Regex pattern for extracting URLs from HTML


class ExtractionConfig(BaseModel):
    """Extraction configuration (flat structure)."""

    type: Literal["css", "llm"] = "css"
    # CSS extraction fields
    base_selector: Optional[str] = None
    fields: list[CssField] = Field(default_factory=list)
    # LLM extraction fields
    provider: Optional[str] = None
    api_token_env: str = "LLM_API_KEY"
    input_format: Optional[Literal["markdown", "html", "fit_markdown"]] = None
    instruction: Optional[str] = None
    images: list[ImageSelector] = Field(
        default_factory=list
    )  # LLM only: images for vision model


class PaginationConfig(BaseModel):
    """Pagination configuration - supports URL, JS, or none."""

    type: Literal["url", "js", "none"] = "none"
    # URL-based pagination fields
    start_page: int = 1
    max_pages: Optional[int] = None
    page_template: str = "?page={page}"
    # JS-based pagination fields
    js_code: Optional[str] = None
    wait_for: Optional[WaitForConfig] = None  # REQUIRED for type="js"

    @model_validator(mode="after")
    def validate_js_pagination(self):
        if self.type == "js" and self.wait_for is None:
            raise ValueError("wait_for is required for JS-based pagination")
        return self


class OutputFilesConfig(BaseModel):
    """Output file paths."""

    csv: Optional[str] = None
    json_file: Optional[str] = None


class OutputConfig(BaseModel):
    """Output configuration."""

    required_fields: list[str] = Field(default_factory=list)
    unique_key: list[str] = Field(default_factory=list)
    files: Optional[OutputFilesConfig] = None
    transform: list = Field(default_factory=list)


class ListingScrapingConfig(BaseModel):
    """Complete listing scraping configuration."""

    setup: Optional[SetupConfig] = None
    pagination: Optional[PaginationConfig] = None
    extraction: ExtractionConfig
    output: Optional[OutputConfig] = None


class ConcurrencyConfig(BaseModel):
    """Concurrency settings for details scraping."""

    max_requests: int = 2
    delay_ms: int = 1000
    timeout_per_page: int = 30000


class DetailsSetupConfig(BaseModel):
    """Setup configuration for details scraping."""

    wait_for: Optional[WaitForConfig] = None
    page_timeout: int = 60000
    cache_mode: Literal["enabled", "disabled", "bypass", "read_only", "write_only"] = (
        "bypass"
    )
    concurrency: Optional[ConcurrencyConfig] = None
    interactions: list[InteractionAction] = Field(default_factory=list)


class DetailsScrapingConfig(BaseModel):
    """Complete details scraping configuration."""

    enabled: bool = False
    setup: Optional[DetailsSetupConfig] = None
    extraction: Optional[ExtractionConfig] = None


class SiteConfig(BaseModel):
    """Complete site configuration (new structure)."""

    name: str
    enabled: bool = True
    url: str
    source: Optional[str] = None  # e.g., "apolar" - defaults to name.split("_")[0]
    base_url: Optional[str] = None  # e.g., "https://www.apolar.com.br"

    browser: Optional[BrowserConfig] = None
    listing_scraping: ListingScrapingConfig
    details_scraping: Optional[DetailsScrapingConfig] = None


class DefaultsConfig(BaseModel):
    """Default configuration values."""

    browser: Optional[BrowserConfig] = None


class SitesConfig(BaseModel):
    """Root configuration containing defaults and sites."""

    defaults: Optional[DefaultsConfig] = None
    sites: list[SiteConfig]

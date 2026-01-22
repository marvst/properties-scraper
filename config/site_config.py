from typing import Literal, Optional

from pydantic import BaseModel, Field


class BrowserConfig(BaseModel):
    """Browser configuration settings."""

    browser_type: Literal["chromium", "firefox", "webkit"] = "chromium"
    headless: bool = True
    verbose: bool = False
    viewport_width: Optional[int] = None
    viewport_height: Optional[int] = None


class CssField(BaseModel):
    """CSS field extraction definition."""

    name: str
    selector: str
    type: Literal["text", "attribute"] = "text"
    attribute: Optional[str] = None
    multiple: bool = False


class CssExtractionConfig(BaseModel):
    """CSS-based extraction configuration."""

    base_selector: str
    fields: list[CssField]


class LlmExtractionConfig(BaseModel):
    """LLM-based extraction configuration."""

    provider: str = "openai/gpt-4o-mini"
    api_token_env: str = "LLM_API_KEY"
    instruction: str
    input_format: Literal["markdown", "html", "fit_markdown"] = "markdown"


class ExtractionConfig(BaseModel):
    """Extraction strategy configuration."""

    type: Literal["css", "llm"] = "css"
    css: Optional[CssExtractionConfig] = None
    llm: Optional[LlmExtractionConfig] = None


class InteractionConfig(BaseModel):
    """Page interaction configuration."""

    css_selector: Optional[str] = None
    wait_for: Optional[str] = None
    js_code: Optional[str] = None


class TimingConfig(BaseModel):
    """Timing configuration."""

    page_timeout: int = 60000
    delay_before_return_html: int = 0
    wait_until: Literal["load", "domcontentloaded", "networkidle"] = "networkidle"


class CacheConfig(BaseModel):
    """Cache configuration."""

    mode: Literal["enabled", "disabled", "bypass", "read_only", "write_only"] = "bypass"


class NumericFieldTransform(BaseModel):
    """Numeric field transformation configuration."""

    name: str
    source: str
    format: Literal["brazilian_currency", "integer", "float"] = "float"


class ComputedField(BaseModel):
    """Computed field configuration."""

    name: str
    template: str


class TransformConfig(BaseModel):
    """Data transformation configuration."""

    enabled: bool = False
    numeric_fields: list[NumericFieldTransform] = Field(default_factory=list)
    computed_fields: list[ComputedField] = Field(default_factory=list)
    deduplicate_fields: list[str] = Field(default_factory=list)


class DetailsExtractionConfig(BaseModel):
    """Extraction strategy configuration for property details."""

    type: Literal["css", "llm"] = "css"
    css: Optional[CssExtractionConfig] = None
    llm: Optional[LlmExtractionConfig] = None


class DetailsScrapingConfig(BaseModel):
    """Configuration for property details page scraping."""

    enabled: bool = False
    max_concurrent_requests: int = 3
    request_delay_ms: int = 1000
    timeout_per_property: int = 30000
    wait_for: Optional[str] = None
    js_code: Optional[str] = None  # JS to run before extraction
    image_selectors: list[str] = Field(default_factory=list)  # CSS selectors to try for images
    image_attributes: list[str] = Field(default_factory=lambda: ["src", "data-lazy", "data-src"])
    extraction: Optional[DetailsExtractionConfig] = None


class DataConfig(BaseModel):
    """Data output configuration."""

    required_keys: list[str] = Field(default_factory=list)
    unique_key_fields: list[str] = Field(default_factory=list)
    output_file: str = "results.csv"
    json_output_file: str = "extracted.json"


class SiteConfig(BaseModel):
    """Complete site configuration."""

    name: str
    enabled: bool = True
    url: str

    browser: Optional[BrowserConfig] = None
    extraction: ExtractionConfig
    interaction: Optional[InteractionConfig] = None
    timing: Optional[TimingConfig] = None
    cache: Optional[CacheConfig] = None
    data: Optional[DataConfig] = None
    transform: Optional[TransformConfig] = None
    details_scraping: Optional[DetailsScrapingConfig] = None


class DefaultsConfig(BaseModel):
    """Default configuration values."""

    browser: Optional[BrowserConfig] = None
    timing: Optional[TimingConfig] = None


class SitesConfig(BaseModel):
    """Root configuration containing defaults and sites."""

    defaults: Optional[DefaultsConfig] = None
    sites: list[SiteConfig]

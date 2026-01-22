import os
from typing import Union

from crawl4ai import JsonCssExtractionStrategy, LLMExtractionStrategy

from config.site_config import DetailsExtractionConfig, ExtractionConfig


def create_extraction_strategy(
    extraction_config: Union[ExtractionConfig, DetailsExtractionConfig],
) -> Union[JsonCssExtractionStrategy, LLMExtractionStrategy]:
    """
    Create an extraction strategy from configuration.

    Args:
        extraction_config: The extraction configuration from YAML.

    Returns:
        Either JsonCssExtractionStrategy or LLMExtractionStrategy.

    Raises:
        ValueError: If the configuration is invalid.
    """
    if extraction_config.type == "css":
        return _create_css_strategy(extraction_config)
    elif extraction_config.type == "llm":
        return _create_llm_strategy(extraction_config)
    else:
        raise ValueError(f"Unknown extraction type: {extraction_config.type}")


def _create_css_strategy(
    extraction_config: ExtractionConfig,
) -> JsonCssExtractionStrategy:
    """Create a CSS-based extraction strategy."""
    if extraction_config.css is None:
        raise ValueError("CSS extraction config required when type is 'css'")

    css_config = extraction_config.css

    # Build the schema
    schema = {
        "name": "extracted_data",
        "baseSelector": css_config.base_selector,
        "fields": [],
    }

    for field in css_config.fields:
        field_def = {
            "name": field.name,
            "selector": field.selector,
            "type": field.type,
        }

        if field.type == "attribute" and field.attribute:
            field_def["attribute"] = field.attribute

        if field.multiple:
            field_def["multiple"] = True

        schema["fields"].append(field_def)

    return JsonCssExtractionStrategy(schema=schema)


def _create_llm_strategy(
    extraction_config: ExtractionConfig,
) -> LLMExtractionStrategy:
    """Create an LLM-based extraction strategy."""
    if extraction_config.llm is None:
        raise ValueError("LLM extraction config required when type is 'llm'")

    llm_config = extraction_config.llm

    # Get API token from environment variable
    api_token = os.environ.get(llm_config.api_token_env)
    if not api_token:
        raise ValueError(
            f"API token not found in environment variable: {llm_config.api_token_env}"
        )

    return LLMExtractionStrategy(
        provider=llm_config.provider,
        api_token=api_token,
        instruction=llm_config.instruction,
        input_format=llm_config.input_format,
    )

import os
from typing import Union

from crawl4ai import JsonCssExtractionStrategy, LLMConfig, LLMExtractionStrategy

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

    yaml_llm_config = extraction_config.llm

    # Get API token from environment variable
    api_token = os.environ.get(yaml_llm_config.api_token_env)
    if not api_token:
        raise ValueError(
            f"API token not found in environment variable: {yaml_llm_config.api_token_env}"
        )

    # Use new LLMConfig API
    llm_config = LLMConfig(
        provider=yaml_llm_config.provider,
        api_token=api_token,
    )

    # Define schema for structured extraction
    schema = {
        "type": "object",
        "properties": {
            "full_address": {"type": "string", "description": "Complete address with street, number, neighborhood, city, state"},
            "condo_fee_text": {"type": "string", "description": "Monthly condo/condominium fee (Condomínio) as shown, e.g. 'R$ 455,00'"},
            "iptu_text": {"type": "string", "description": "IPTU property tax as shown, e.g. 'R$ 69,00'"},
            "fire_insurance_text": {"type": "string", "description": "Fire insurance (Seguro incêndio) as shown, e.g. 'R$ 40,00'"},
            "total_monthly_cost_text": {"type": "string", "description": "Total monthly cost as shown"},
            "full_description": {"type": "string", "description": "Full property description text"},
            "amenities": {"type": "array", "items": {"type": "string"}, "description": "List of amenities and features"},
            "total_area_text": {"type": "string", "description": "Total area in m²"},
            "private_area_text": {"type": "string", "description": "Private area in m²"},
        },
        "required": ["full_address"],
    }

    return LLMExtractionStrategy(
        llm_config=llm_config,
        instruction=yaml_llm_config.instruction,
        schema=schema,
        extraction_type="schema",
        input_format=yaml_llm_config.input_format,
        verbose=True,
    )

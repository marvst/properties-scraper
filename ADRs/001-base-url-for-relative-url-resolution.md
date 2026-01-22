# ADR-001: base_url Field for Relative URL Resolution

**Date:** 2026-01-22

**Status:** Accepted

## Context

The `DatabaseSync` class requires a `base_url` parameter, which is passed to `from_procrawl()` in `database/models.py`. This parameter is used to resolve relative property URLs to absolute URLs before storing them in the database.

The relevant code in `database/models.py:62-66`:

```python
if property_url and not property_url.startswith(("http://", "https://")):
    original_url = urljoin(base_url, property_url)
else:
    original_url = property_url
```

However, upon inspection of actual extracted data from Apolar, the `property_url` values are already absolute URLs:

```
https://www.apolar.com.br/alugar/curitiba/pilarzinho/alugar-residencial-apartamento-curitiba-pilarzinho-108609?
```

This means `base_url` is never used for Apolar because the `href` attributes in the HTML already contain full URLs.

## Decision

Keep `base_url` as a configurable field in `SiteConfig` with automatic derivation from the site URL's origin as a fallback.

Rationale:
1. **Defensive design**: Some websites may use relative `href` attributes (e.g., `/imovel/123`), even if current targets don't
2. **Low overhead**: The field is optional and auto-derived, so it doesn't burden YAML configuration
3. **Future-proofing**: New sites can be added without code changes if they happen to use relative URLs

## Consequences

### Positive
- Handles both absolute and relative URLs without site-specific code
- No configuration required for sites with absolute URLs (auto-derived default works)
- Explicit override available via `base_url` in YAML if needed

### Negative
- The `base_url` parameter is passed around even when not needed
- Slight code complexity for a feature that may never be used
- Could mislead developers into thinking it's always necessary

### Neutral
- For Apolar (and likely most modern sites), `base_url` is effectively dead code at runtime

#!/usr/bin/env python
"""Standalone sync command to sync properties from JSON file to database."""

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from database import get_syncer


def infer_source_and_base_url(filename: str) -> tuple[str, str]:
    """Infer source name and base URL from extraction filename.

    Args:
        filename: Extraction filename (e.g., "apolar_apartments_20260203_210336.json")

    Returns:
        Tuple of (source, base_url)
    """
    stem = Path(filename).stem
    source = stem.split("_")[0]

    # Known base URLs for sources
    base_urls = {
        "apolar": "https://www.apolar.com.br",
        "galvao": "https://www.imobiliariagalvao.com.br",
        "chaves": "https://www.chavesnamao.com.br",
    }

    base_url = base_urls.get(source, "")
    return source, base_url


def main():
    parser = argparse.ArgumentParser(
        description="Sync properties from JSON extraction file to database"
    )
    parser.add_argument("file", help="Path to extraction JSON file")
    parser.add_argument(
        "--source",
        help="Source name (default: inferred from filename, e.g., 'apolar')",
    )
    parser.add_argument(
        "--base-url",
        help="Base URL for property links (default: inferred from source)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of properties per API request (default: 50)",
    )
    args = parser.parse_args()

    # Validate file exists
    file_path = Path(args.file)
    if not file_path.exists():
        print(f"Error: File not found: {args.file}")
        sys.exit(1)

    # Load properties
    print(f"Loading properties from {args.file}...")
    with open(file_path) as f:
        properties = json.load(f)

    if not properties:
        print("No properties found in file.")
        sys.exit(0)

    print(f"Loaded {len(properties)} properties")

    # Infer source and base_url if not provided
    inferred_source, inferred_base_url = infer_source_and_base_url(file_path.name)
    source = args.source or inferred_source
    base_url = args.base_url or inferred_base_url

    if not source:
        print("Error: Could not infer source from filename. Please provide --source")
        sys.exit(1)

    print(f"Source: {source}")
    print(f"Base URL: {base_url or '(none)'}")
    print(f"Batch size: {args.batch_size}")
    print()

    # Sync with batching
    try:
        syncer = get_syncer(source=source, base_url=base_url)
        stats = syncer.sync_properties(properties, batch_size=args.batch_size)
        print()
        print(f"Sync complete!")
        print(f"  Added: {stats['added']}")
        print(f"  Updated: {stats['updated']}")
        print(f"  Found (unchanged): {stats['found']}")
        print(f"  Removed: {stats['removed']}")
    except ValueError as e:
        print(f"Error: {e}")
        print("Make sure VPC_API_URL and VPC_API_KEY environment variables are set.")
        sys.exit(1)
    except Exception as e:
        print(f"Sync failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

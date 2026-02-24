import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from crawl4ai import AsyncWebCrawler
from rich.console import Console

load_dotenv()

EXTRACTIONS_DIR = Path("extractions")

from config import get_site_config, list_sites
from database import get_syncer
from utils.details_scraper import PropertyDetailsScraper
from utils.extraction_factory import create_extraction_strategy
from utils.scraper_utils import fetch_and_process_page, get_browser_config

console = Console()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Procrawl - YAML-configured web scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
 Examples:
   python main.py --list                         List available sites
   python main.py --config sites/guaratingueta   Crawl all sites in a folder
   python main.py apolar_apartments              Crawl a specific site
   python main.py kenlo --config sites/guaratingueta  Crawl one site from a folder
         """,
    )

    parser.add_argument(
        "site",
        nargs="?",
        help="Name of the site to crawl (from sites/ directory)",
    )
    parser.add_argument(
        "--config",
        "-c",
        default="sites",
        help="Path to directory containing site YAML files (default: sites)",
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List available sites and exit",
    )
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override headless mode (--headless or --no-headless). Overrides YAML config.",
    )

    return parser.parse_args()


def print_sites_list(config_path: str):
    """Print a formatted list of available sites."""
    try:
        sites = list_sites(config_path)
    except FileNotFoundError:
        print(f"Error: Sites directory not found: {config_path}")
        print("Create a sites/ directory with site YAML files or specify a different directory with --config")
        sys.exit(1)

    if not sites:
        print("No sites configured.")
        return

    print("\nAvailable sites:")
    print("-" * 70)
    for site in sites:
        status = "enabled" if site["enabled"] else "disabled"
        print(f"  {site['name']:<20} [{status}]")
        print(f"    URL: {site['url']}")
    print("-" * 70)


async def crawl(site_name: str, config_path: str, headless: bool | None = None, quiet: bool = False):
    """
    Main function to crawl data from a configured site.

    Args:
        site_name: The name of the site to crawl.
        config_path: Path to the directory containing site YAML files.
        headless: Override headless mode. None means use YAML config value.
        quiet: Suppress low-level operational logs.
    """
    with console.status("[bold blue]Loading configuration...") as status:
        # Load site configuration
        try:
            site_config = get_site_config(site_name, config_path)
        except FileNotFoundError:
            console.print(f"[red]Error: Sites directory not found: {config_path}[/red]")
            sys.exit(1)
        except ValueError as e:
            console.print(f"[red]Error: {e}[/red]")
            sys.exit(1)

        if headless is not None and site_config.browser:
            site_config.browser.headless = headless

        if not quiet:
            console.print(f"\n[bold]Crawling site:[/bold] {site_config.name}")
            console.print(f"[bold]URL:[/bold] {site_config.url}")

        # Initialize configurations
        status.update("[bold blue]Initializing browser configuration...")
        browser_config = get_browser_config(site_config)

        # Get extraction config from listing_scraping
        listing_config = site_config.listing_scraping
        extraction_strategy = create_extraction_strategy(listing_config.extraction)
        session_id = f"crawl_{site_config.name}"

        # Get CSS selector from extraction config
        css_selector = ""
        if listing_config.extraction.type == "css" and listing_config.extraction.base_selector:
            css_selector = listing_config.extraction.base_selector

        # Get required keys from output config
        required_keys = []
        if listing_config.output:
            required_keys = listing_config.output.required_fields

        # Initialize state variables
        all_results = []
        seen_names = set()

    # Start the web crawler context - stop spinner during crawl to avoid event loop interference
    if not quiet:
        console.print("[bold blue]Starting browser...")
    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Check pagination type
        pagination = listing_config.pagination
        if pagination and pagination.type == "url":
            # URL-based pagination
            current_page = pagination.start_page
            max_pages = pagination.max_pages
            base_url = site_config.url

            while True:
                # Generate URL for current page
                if current_page == 1:
                    page_url = base_url
                else:
                    page_url = base_url + pagination.page_template.format(page=current_page)

                if not quiet:
                    console.print(f"[bold blue]Fetching page {current_page}: {page_url}[/bold blue]")

                results = await fetch_and_process_page(
                    crawler,
                    page_url,
                    css_selector,
                    extraction_strategy,
                    session_id,
                    required_keys,
                    seen_names,
                    site_config,
                    quiet=quiet,
                )

                if not results:
                    if not quiet:
                        console.print(f"[yellow]No results on page {current_page}. Stopping pagination.[/yellow]")
                    break

                if not quiet:
                    console.print(f"[green]Found {len(results)} results on page {current_page}[/green]")
                all_results.extend(results)

                # Check if we've reached max_pages
                if max_pages and current_page >= max_pages:
                    if not quiet:
                        console.print(f"[yellow]Reached max_pages ({max_pages}). Stopping pagination.[/yellow]")
                    break

                current_page += 1

        elif pagination and pagination.type == "js":
            # JS-based pagination (load all content with JS, then extract once)
            if not quiet:
                console.print("[bold blue]Fetching page with JS-based loading...")
            results = await fetch_and_process_page(
                crawler,
                site_config.url,
                css_selector,
                extraction_strategy,
                session_id,
                required_keys,
                seen_names,
                site_config,
                quiet=quiet,
            )

            if not results:
                if not quiet:
                    console.print("[yellow]No results extracted from the page.[/yellow]")

            all_results.extend(results)

        else:
            # Single page scraping (type="none" or no pagination)
            if not quiet:
                console.print("[bold blue]Fetching page and extracting data...")
            results = await fetch_and_process_page(
                crawler,
                site_config.url,
                css_selector,
                extraction_strategy,
                session_id,
                required_keys,
                seen_names,
                site_config,
                quiet=quiet,
            )

            if not results:
                if not quiet:
                    console.print("[yellow]No results extracted from the page.[/yellow]")

            all_results.extend(results)

        # Scrape property details if enabled
        if site_config.details_scraping and site_config.details_scraping.enabled and all_results:
            if not quiet:
                console.print("[bold blue]Scraping property details...[/bold blue]")
            try:
                details_scraper = PropertyDetailsScraper(site_config)
                all_results = await details_scraper.scrape_property_details(
                    all_results, session_id
                )
            except Exception as e:
                console.print(f"[red]Details scraping failed: {e}[/red]")
                console.print("[yellow]Continuing with listing data only.[/yellow]")

    with console.status("[bold green]Saving results...") as status:
        # Save final results to JSON
        if all_results:
            status.update("[bold green]Saving JSON...")
            EXTRACTIONS_DIR.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_output_path = EXTRACTIONS_DIR / f"{site_config.name}_{timestamp}.json"

            with open(json_output_path, "w", encoding="utf-8") as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)
            console.print(f"[green]Saved {len(all_results)} properties to '{json_output_path}'[/green]")

        # Sync to vou-pra-curitiba database
        if all_results:
            status.update("[bold green]Syncing to database...")
            # Use explicit config or derive sensible defaults
            parsed_url = urlparse(site_config.url)
            source_name = site_config.source or site_config.name.split("_")[0]
            base_url = site_config.base_url or f"{parsed_url.scheme}://{parsed_url.netloc}"
            syncer = get_syncer(source=source_name, base_url=base_url)
            try:
                stats = syncer.sync_properties(all_results)
                console.print(
                    f"[green]Database sync: {stats['added']} added, {stats['updated']} updated[/green]"
                )
            except FileNotFoundError as e:
                console.print(f"[yellow]Database sync skipped: {e}[/yellow]")
            except Exception as e:
                console.print(f"[red]Database sync failed: {e}[/red]")
        else:
            console.print("[yellow]No results were found during the crawl.[/yellow]")


async def crawl_all(config_path: str, headless: bool | None = None):
    """Crawl all enabled sites in a config directory sequentially."""
    sites = list_sites(config_path)
    enabled_sites = [s for s in sites if s["enabled"]]

    if not enabled_sites:
        console.print(f"[yellow]No enabled sites found in '{config_path}'.[/yellow]")
        return

    console.print(f"[bold]Found {len(enabled_sites)} enabled site(s) in '{config_path}'[/bold]")

    failed = []
    for i, site in enumerate(enabled_sites, 1):
        site_name = site["name"]
        site_stem = site.get("stem", site_name)
        console.print(f"\n[bold blue]━━━ [{i}/{len(enabled_sites)}] {site_name} ━━━[/bold blue]")
        try:
            await crawl(site_stem, config_path, headless=headless, quiet=True)
        except Exception as e:
            console.print(f"[red]Failed to crawl '{site_name}': {e}[/red]")
            failed.append(site_name)

    console.print(f"\n[bold]Done.[/bold] {len(enabled_sites) - len(failed)}/{len(enabled_sites)} site(s) succeeded.")
    if failed:
        console.print(f"[red]Failed: {', '.join(failed)}[/red]")


async def main():
    """Entry point of the script."""
    args = parse_args()

    if args.list:
        print_sites_list(args.config)
        return

    if args.site:
        await crawl(args.site, args.config, headless=args.headless)
    else:
        await crawl_all(args.config, headless=args.headless)


if __name__ == "__main__":
    asyncio.run(main())

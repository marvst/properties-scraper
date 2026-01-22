import argparse
import asyncio
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
from database import DatabaseSync
from utils.data_utils import save_results_to_csv
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
  python main.py --list                    List available sites
  python main.py apolar_apartments         Crawl a specific site
  python main.py my_site --config custom.yaml  Use custom config file
        """,
    )

    parser.add_argument(
        "site",
        nargs="?",
        help="Name of the site to crawl (from sites.yaml)",
    )
    parser.add_argument(
        "--config",
        "-c",
        default="sites.yaml",
        help="Path to YAML config file (default: sites.yaml)",
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List available sites and exit",
    )

    return parser.parse_args()


def print_sites_list(config_path: str):
    """Print a formatted list of available sites."""
    try:
        sites = list_sites(config_path)
    except FileNotFoundError:
        print(f"Error: Config file not found: {config_path}")
        print("Create a sites.yaml file or specify a different config with --config")
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


async def crawl(site_name: str, config_path: str):
    """
    Main function to crawl data from a configured site.

    Args:
        site_name: The name of the site to crawl.
        config_path: Path to the YAML config file.
    """
    with console.status("[bold blue]Loading configuration...") as status:
        # Load site configuration
        try:
            site_config = get_site_config(site_name, config_path)
        except FileNotFoundError:
            console.print(f"[red]Error: Config file not found: {config_path}[/red]")
            sys.exit(1)
        except ValueError as e:
            console.print(f"[red]Error: {e}[/red]")
            sys.exit(1)

        console.print(f"\n[bold]Crawling site:[/bold] {site_config.name}")
        console.print(f"[bold]URL:[/bold] {site_config.url}")

        # Initialize configurations
        status.update("[bold blue]Initializing browser configuration...")
        browser_config = get_browser_config(site_config)
        extraction_strategy = create_extraction_strategy(site_config.extraction)
        session_id = f"crawl_{site_config.name}"

        # Get CSS selector and required keys from config
        css_selector = ""
        if site_config.interaction and site_config.interaction.css_selector:
            css_selector = site_config.interaction.css_selector
        elif site_config.extraction.css:
            css_selector = site_config.extraction.css.base_selector

        required_keys = []
        if site_config.data:
            required_keys = site_config.data.required_keys

        # Initialize state variables
        all_results = []
        seen_names = set()

    # Start the web crawler context - stop spinner during crawl to avoid event loop interference
    console.print("[bold blue]Starting browser...")
    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Check if pagination is enabled
        pagination = site_config.pagination
        if pagination and pagination.enabled:
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
                )

                if not results:
                    console.print(f"[yellow]No results on page {current_page}. Stopping pagination.[/yellow]")
                    break

                console.print(f"[green]Found {len(results)} results on page {current_page}[/green]")
                all_results.extend(results)

                # Check if we've reached max_pages
                if max_pages and current_page >= max_pages:
                    console.print(f"[yellow]Reached max_pages ({max_pages}). Stopping pagination.[/yellow]")
                    break

                current_page += 1
        else:
            # Single page scraping (original behavior)
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
            )

            if not results:
                console.print("[yellow]No results extracted from the page.[/yellow]")

            # Add the results to the total list
            all_results.extend(results)

        # Scrape property details if enabled
        if site_config.details_scraping and site_config.details_scraping.enabled and all_results:
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

        # Save the collected results to a CSV file
        if all_results:
            status.update("[bold green]Saving results...")
            # Ensure extractions directory exists
            EXTRACTIONS_DIR.mkdir(exist_ok=True)

            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = EXTRACTIONS_DIR / f"{site_config.name}_{timestamp}.csv"

            save_results_to_csv(all_results, str(output_file))
            console.print(f"[green]Saved {len(all_results)} results to '{output_file}'.[/green]")

            # Sync to vou-pra-curitiba database
            status.update("[bold green]Syncing to database...")
            # Use explicit config or derive sensible defaults
            parsed_url = urlparse(site_config.url)
            source_name = site_config.source or site_config.name.split("_")[0]
            base_url = site_config.base_url or f"{parsed_url.scheme}://{parsed_url.netloc}"
            syncer = DatabaseSync(source=source_name, base_url=base_url)
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


async def main():
    """Entry point of the script."""
    args = parse_args()

    if args.list:
        print_sites_list(args.config)
        return

    if not args.site:
        print("Error: Please specify a site name or use --list to see available sites.")
        print("Usage: python main.py <site_name> [--config <config_file>]")
        sys.exit(1)

    await crawl(args.site, args.config)


if __name__ == "__main__":
    asyncio.run(main())

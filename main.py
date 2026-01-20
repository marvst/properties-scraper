import asyncio

from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv

from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS
from utils.data_utils import (
    save_results_to_csv,
)
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
)

load_dotenv()


async def crawl():
    """
    Main function to crawl venue data from the website.
    """
    # Initialize configurations
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy()
    session_id = "crawl_session"

    # Initialize state variables
    all_results = []
    seen_names = set()

    # Start the web crawler context
    # https://docs.crawl4ai.com/api/async-webcrawler/#asyncwebcrawler
    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Fetch and process data from the initial page
        results = await fetch_and_process_page(
            crawler,
            BASE_URL,
            CSS_SELECTOR,
            llm_strategy,
            session_id,
            REQUIRED_KEYS,
            seen_names,
        )

        if not results:
            print("No results extracted from the page.")

        # Add the results to the total list
        all_results.extend(results)

    # Save the collected results to a CSV file
    if all_results:
        save_results_to_csv(all_results, "complete_results.csv")
        print(f"Saved {len(all_results)} results to 'complete_results.csv'.")
    else:
        print("No results were found during the crawl.")

    # Display usage statistics for the LLM strategy
    llm_strategy.show_usage()


async def main():
    """
    Entry point of the script.
    """
    await crawl()


if __name__ == "__main__":
    asyncio.run(main())
from __future__ import annotations

import pendulum

from airflow.decorators import dag, task

# Define a constant for the output path to make it easily configurable
DATA_OUTPUT_PATH = "/opt/airflow/data/book_html/"


@dag(
    dag_id="book_scraper_taskflow_dag_perfected",
    start_date=pendulum.datetime(2025, 9, 20, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md="""
    ### Book Scraper DAG
    This DAG scrapes books.toscrape.com.
    - **get_all_book_urls**: Scrapes the site to find all individual book URLs.
    - **download_one_book_html**: Dynamically downloads the HTML for each book URL found.
    """,
    tags=["example", "scraper", "taskflow", "best-practice"],
)
def books_scraper_dag_perfected():
    """
    A DAG to scrape book information using isolated Python virtual environments.
    """

    @task.virtualenv(requirements=["httpx", "beautifulsoup4"])
    def get_all_book_urls() -> list[str]:
        """
        Scrapes books.toscrape.com to find all individual book URLs.
        """
        import logging
        import httpx
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin

        # ... function body is identical to the previous version ...
        logger = logging.getLogger(__name__)
        base_url = "https://books.toscrape.com/"
        catalogue_path = "catalogue/"
        client = httpx.Client(base_url=base_url)
        all_book_urls = []
        current_page_path = "index.html"
        logger.info("Starting to collect all book URLs...")
        while True:
            url_to_fetch = (
                urljoin(catalogue_path, current_page_path)
                if "catalogue" not in current_page_path
                else current_page_path
            )
            if current_page_path == "index.html":
                url_to_fetch = current_page_path
            logger.info(f"Scraping list page: {base_url}{url_to_fetch}")
            response = client.get(url_to_fetch)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            for article in soup.select("article.product_pod"):
                relative_book_url = article.h3.a["href"]
                book_full_url = urljoin(
                    f"{base_url}{catalogue_path}", relative_book_url
                )
                all_book_urls.append(book_full_url)
            next_button = soup.select_one("li.next a")
            if next_button:
                current_page_path = next_button["href"]
            else:
                break
        logger.info(f"Finished collecting {len(all_book_urls)} book URLs.")
        return all_book_urls

    # The only change is here: "pathlib" is removed from requirements
    @task.virtualenv(requirements=["httpx"])
    def download_one_book_html(book_url: str, output_path: str):
        """
        Downloads the HTML content for a single book URL and saves it.
        """
        import logging
        import httpx
        from pathlib import Path  # Standard library, no pip install needed

        # ... function body is identical to the previous version ...
        logger = logging.getLogger(__name__)
        logger.info(f"Downloading HTML for: {book_url}")
        try:
            response = httpx.get(book_url)
            response.raise_for_status()
            book_id = book_url.split("/")[-2]
            file_path = Path(output_path) / f"{book_id}.html"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.info(f"Successfully saved HTML for {book_url} to {file_path}")
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error for book {book_url}: {e}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error for book {book_url}: {e}")
            raise

    book_urls = get_all_book_urls()
    download_one_book_html.partial(output_path=DATA_OUTPUT_PATH).expand(
        book_url=book_urls
    )


books_scraper_dag_perfected()

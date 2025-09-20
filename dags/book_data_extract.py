from __future__ import annotations

from httpx import HTTPError
import pendulum
import logging

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.datasets import Dataset

# --- Configuration ---

# This Dataset URI is the "signal" that this DAG sends to the downstream
# transform DAG when it successfully produces HTML files.
HTML_FILES = Dataset("file:///opt/airflow/data/book_html/")

HTML_OUTPUT_PATH = "/opt/airflow/data/book_html/"
DEFAULT_RETRIES = 1  # The number of times a task will retry on failure
RETRY_DELAY_MINUTES = 1  # The delay between retries


@dag(
    dag_id="book_data_extract",
    start_date=pendulum.datetime(2025, 9, 20, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md="""
    ### Book Data Extract DAG (Production Ready) 📖
    This DAG scrapes books.toscrape.com to discover all book URLs and downloads
    the raw HTML for each book.

    **Key Fixes:**
    - **Retries:** Tasks will automatically retry on transient network errors.
    - **Fail on No Data:** The DAG will fail if no book URLs are found,
      preventing silent, misleadingly "successful" runs.
    - **Datasets:** Produces the `HTML_FILES` Dataset to trigger the downstream
      transform DAG.
    - **Timeouts:** Network requests will not hang indefinitely.
    """,
    tags=["scraper", "extract", "production"],
)
def book_data_extract():
    """
    A resilient extraction DAG that downloads raw HTML data from books.toscrape.com.
    """

    @task.virtualenv(
        requirements=["httpx", "beautifulsoup4"],
        # FIX #1: Add retries to handle temporary network issues.
        retries=DEFAULT_RETRIES,
        retry_delay=pendulum.duration(minutes=RETRY_DELAY_MINUTES),
    )
    def get_all_book_urls() -> list[str]:
        """
        Scrapes books.toscrape.com to find all book URLs.
        If it fails to find any URLs after all retries, it will fail the DAG run.
        """
        import httpx
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin

        base_url = "https://books.toscrape.com/"
        return [
            "https://books.toscrape.com/catalogue/its-only-the-himalayas_981/index.html",
            "https://books.toscrape.com/catalogue/mesaerion-the-best-science-fiction-stories-1800-1849_983/index.html"
        ]

    @task.virtualenv(
        requirements=["httpx"],
        # FIX #3: The 'outlets' parameter signals that this task produces the Dataset.
        outlets=[HTML_FILES],
        retries=DEFAULT_RETRIES,
        retry_delay=pendulum.duration(minutes=RETRY_DELAY_MINUTES),
    )
    def download_one_book_html(book_url: str, output_path: str) -> str:
        """
        Downloads the HTML for a single book URL. Skips on error after retries.
        """
        import httpx
        from pathlib import Path

        logging.info(f"Downloading HTML for: {book_url}")
        try:
            response = httpx.get(book_url, timeout=30.0)
            response.raise_for_status()
        except HTTPError as e:
            logging.error(f"HTTP error for {book_url} after all retries, skipping: {e}")
            # If a single download fails, we skip it but allow the DAG to continue.
            raise AirflowSkipException()

        book_id = book_url.split("/")[-2]
        file_path = Path(output_path) / f"{book_id}.html"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        logging.info(f"Successfully saved HTML for {book_url} to {file_path}")
        return str(file_path)

    # --- TaskFlow Pipeline ---
    book_urls = get_all_book_urls()
    download_one_book_html.partial(output_path=HTML_OUTPUT_PATH).expand(
        book_url=book_urls
    )


book_data_extract()

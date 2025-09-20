from __future__ import annotations

import pendulum
import json
import re

from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Define constants for the data paths
HTML_OUTPUT_PATH = "/opt/airflow/data/book_html/"
JSON_OUTPUT_PATH = "/opt/airflow/data/book_json/"

# Define a Dataset that represents the collection of HTML files
# This URI format tells Airflow we're tracking a location in the file system.
HTML_FILES = Dataset(f"file://{HTML_OUTPUT_PATH}")


@dag(
    dag_id="book_data_transform",
    start_date=pendulum.datetime(2025, 9, 20, tz="UTC"),
    # This DAG is scheduled to run whenever the HTML_FILES Dataset is produced.
    schedule=[HTML_FILES],
    catchup=False,
    doc_md="""
    ### Book Data Transform DAG 📖
    This DAG is triggered after the `book_data_extract` DAG produces new HTML files.
    It lists all available HTML files, reads them, parses the book data,
    and saves the structured data as individual JSON files.
    """,
    tags=["parser", "transform"],
)
def book_data_transform():
    """
    A transformation DAG that processes book HTML files into structured JSON data.
    """

    @task
    def get_html_files(path: str) -> list[str]:
        """
        Scans the specified directory for .html files and returns a list of their paths.
        """
        from pathlib import Path
        import logging

        logger = logging.getLogger(__name__)
        html_dir = Path(path)
        if not html_dir.exists():
            logger.warning(f"Directory does not exist: {path}")
            return []
        
        file_paths = [str(file) for file in html_dir.glob("*.html")]
        logger.info(f"Found {len(file_paths)} HTML files to process.")
        return file_paths

    @task
    def read_html_file(file_path: str) -> str:
        """
        Reads the content of an HTML file and returns it as a string.
        """
        from pathlib import Path
        return Path(file_path).read_text(encoding="utf-8")

    @task.virtualenv(requirements=["beautifulsoup4"])
    def parse_book_data(html_content: str) -> dict:
        """
        Parses the HTML of a book's product page to extract its details.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html_content, "html.parser")
        product_main = soup.select_one("div.product_main")
        title = product_main.find("h1").text
        price_str = product_main.select_one("p.price_color").text
        price = float(price_str.replace("£", ""))
        stock_text = product_main.select_one("p.instock.availability").text.strip()
        stock_match = re.search(r"\((\d+) available\)", stock_text)
        stock_count = int(stock_match.group(1)) if stock_match else 0
        rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
        star_rating_tag = product_main.select_one("p.star-rating")
        rating_text_class = next(
            (cls for cls in star_rating_tag.get("class", []) if cls != "star-rating"), None
        )
        rating = rating_map.get(rating_text_class, 0)
        description_tag = soup.select_one("#product_description + p")
        description = description_tag.text.strip() if description_tag else "No description."

        product_info = {}
        table = soup.find("table", class_="table-striped")
        for row in table.find_all("tr"):
            key = row.find("th").text.strip()
            value = row.find("td").text.strip()
            product_info[key] = value

        return {
            "title": title, "price": price, "stock_count": stock_count,
            "rating": rating, "description": description, "upc": product_info.get("UPC"),
            "price_excl_tax": float(product_info.get("Price (excl. tax)", "£0.00").replace("£", "")),
            "price_incl_tax": float(product_info.get("Price (incl. tax)", "£0.00").replace("£", "")),
            "tax": float(product_info.get("Tax", "£0.00").replace("£", "")),
            "number_of_reviews": int(product_info.get("Number of reviews", 0)),
        }

    @task
    def save_as_json(book_data: dict, output_path: str):
        """
        Saves a dictionary of book data as a JSON file, named after the book's UPC.
        """
        from pathlib import Path

        upc = book_data.get("upc")
        if not upc:
            raise ValueError("Cannot save book data without a UPC.")

        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"{upc}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(book_data, f, indent=4, ensure_ascii=False)

    # --- Define the TaskFlow Pipeline ---
    html_files = get_html_files(path=HTML_OUTPUT_PATH)
    html_contents = read_html_file.expand(file_path=html_files)
    parsed_data = parse_book_data.expand(html_content=html_contents)
    save_as_json.partial(output_path=JSON_OUTPUT_PATH).expand(book_data=parsed_data)

book_data_transform()

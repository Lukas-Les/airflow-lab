from datetime import datetime
from airflow.sdk import dag, task


@dag(
    schedule=None,
    dag_id="nld_opendata_extract",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
)
def nld_opendata_extract():
    @task.virtualenv(requirements=["httpx"])
    def download_data():
        import httpx
        from datetime import datetime

        with httpx.Client() as client:
            response = client.get(
                "https://opendata.rdw.nl/api/v3/views/m9d7-ebf2/query.json"
            )

        file_name = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        return {
            "file_name": file_name,
            "content": response.content,
        }

    @task
    def save_data(result: dict):
        import os

        file_name = result.get("file_name")
        with open(os.path.join("./data", f"{file_name}.json"), "rb") as f:
            f.write(result.get("content", b""))

    result = download_data()
    save_data(result)


nld_opendata_extract()

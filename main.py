from dotenv import dotenv_values
from gtfs import download_gtfs
from monitorar_onibus import monitorar_onibus


def main():
    config = dotenv_values(".env")
    download_gtfs(
        url=config.get("GTFS_URL"),
        login=config.get("LOGIN"),
        password=config.get("PASSWORD"),
        downloads_folder=config.get("DOWNLOADS_FOLDER"),
    )
    monitorar_onibus(
        token=config.get("TOKEN"),
        base_url=config.get("API_BASE_URL"),
        intervalo=int(config.get("INTERVALO")),
        downloads_folder=config.get("DOWNLOADS_FOLDER"),
    )


if __name__ == "__main__":
    main()

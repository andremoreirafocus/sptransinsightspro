from datetime import datetime
from dotenv import dotenv_values
from src.services.buses_positions import (
    extract_buses_positions,
    get_buses_positions_summary,
)
from src.infra.storage import save_data_to_json_file


def main():
    config = dotenv_values(".env")
    buses_positions = extract_buses_positions(
        token=config.get("TOKEN"),
        base_url=config.get("API_BASE_URL"),
    )
    horario_ref, total_veiculos = get_buses_positions_summary(buses_positions)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Ref SPTrans: {horario_ref} | Veículos Ativos: {total_veiculos}"
    )
    save_data_to_json_file(
        buses_positions,
        downloads_folder=config.get("DOWNLOADS_FOLDER"),
        file_name=f"buses_positions_{horario_ref}.json",
    )


if __name__ == "__main__":
    main()

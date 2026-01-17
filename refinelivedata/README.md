Este projeto faz:
- extrai as posicoes para uma linha e veiculo para um ano, mes e dia;
- calcula as trips para uma linha e veiculo para um ano, mes e dia;
- salva as trips calculadas

Configurações:
# TABLE_NAME=<table_name_including_schema> # where data will be written
FINISHED_TRIPS_TABLE_NAME=<table_name_for_finished_trips_including_schema>
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

Para executar: 
python ./main.py





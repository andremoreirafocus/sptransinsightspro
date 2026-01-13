Este projeto faz:
- download de arquivos GTFS do portal do desenvolvedor
- salva cada um dos arquivos em um prefixo de uma pasta raw no minio

Configurações:
GTFS_URL = "http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS"
LOGIN = <insira seu login>
PASSWORD = <insira sua senha>
LOCAL_DOWNLOADS_FOLDER = "gtfs_files"
RAW_BUCKET_NAME = "raw"
APP_FOLDER = "gtfs"
MINIO_ENDPOINT=<hostname:port> # format 
ACCESS_KEY=<key>
SECRET_KEY=<secret>


Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

Para executar: 
python ./main.py

Instruções adicionais:
- Para obter as credenciais cadastre-se no portal do desenvolvedor da SPTRANS
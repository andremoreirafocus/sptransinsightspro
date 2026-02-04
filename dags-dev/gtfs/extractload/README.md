## Objetivo deste subprojeto
Fazer a extração dos arquivos GTFS do portal do desenvolvedor da SPTrans para enriquecimento dod dados extraídos da API


## O que este subprojeto faz
- download de arquivos GTFS do portal do desenvolvedor da SPTrans utilizados para enriquecer dados das posicoes dos onibus obtidas via API
- salva cada um dos arquivos no prefixo gtfs da pasta raw no minio

## Pré-requisitos
- Obter as credenciais cadastre-se no portal do desenvolvedor da SPTRANS
- Disponibilidade de um bucket da camada raw previamente criado no serviço de storage, atualmente o Minio
- Criação de uma chave de acesso ao Minio cadastrada no arquivo de configurações com acesso de escrita no bucket da camada raw
- Criação do arquivo de configurações


## Configurações
GTFS_URL = "http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS"
LOGIN = <insira seu login>
PASSWORD = <insira sua senha>
LOCAL_DOWNLOADS_FOLDER = "gtfs_files"
RAW_BUCKET_NAME = "raw"
APP_FOLDER = "gtfs"
MINIO_ENDPOINT=<hostname:port> # format 
ACCESS_KEY=<key>
SECRET_KEY=<secret>

## Instruções para instalação
Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução 
python ./main.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima


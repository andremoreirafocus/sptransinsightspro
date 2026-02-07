## Objetivos deste subprojeto
Fazer a extração dos arquivos GTFS do portal do desenvolvedor da SPTrans para enriquecimento dos dados extraídos da API. 
A implementação final é feita via a DAG gtfs do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática - via arquivo config.py - de acordo com o ambiente de execução, seja produção, via Airflow, ou desenvolvimento, local.


## O que este subprojeto faz
- download de arquivos GTFS do portal do desenvolvedor da SPTrans utilizados para enriquecer dados das posicoes dos onibus obtidas via API
- salva cada um dos arquivos na "pasta" gtfs do bucket raw no serviço de object storage
- para cada arquivo gtfs relevante para as análises a serem feitas na camada refined, extraido e salvo no bucket "raw", efetua transformações uma tabela com o mesmo nome que o arquivo, no formato Parquet
- salva os dados transformados no bucket "trusted"
- a partir de joins efetuados entre as tabelas gtfs criadas na camada trusted, gera uma tabela de detalhes das viagens (trip_details), a ser utilizada  para enriquecer os dados de posição dos veículos durante sua transformação, implementada pelo subprojeto transformlivedata, visando as análises de dados efetuadas no subprojeto refinelivedata

## Pré-requisitos
- Obter as credenciais cadastre-se no portal do desenvolvedor da SPTRANS
- Disponibilidade de dois buckets: uma para a camada raw e outro para a camada trusted, previamente criados no serviço de object storage
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura e escrita aos bucket das camadas raw e trusted 
- Criação do arquivo de configurações

## Configurações
GTFS_URL = "http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS"
LOGIN = <insira seu login>
PASSWORD = <insira sua senha>
LOCAL_DOWNLOADS_FOLDER = "gtfs_files"
RAW_BUCKET = "raw"
APP_FOLDER = "gtfs"
TRUSTED_BUCKET = "trusted"
TRIP_DETAILS_TABLE_NAME = "trip_details"
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
python gtfs-v2.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima


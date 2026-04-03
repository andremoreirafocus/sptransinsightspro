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
As configurações são centralizadas em `config/config.py` e expostas como um único objeto com 1 seção:
- `general`

### Local/dev
- `general` vem do arquivo `dags-dev/gtfs/config/gtfs.json`
- `.env` em `dags-dev/gtfs/.env` é usado apenas para credenciais de conexão

Credenciais esperadas no `.env`:
GTFS_URL="http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS"
LOGIN=<insira seu login>
PASSWORD=<insira sua senha>
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>

Chaves esperadas em `general`
```json
{
  "extraction": {
    "local_downloads_folder": "gtfs_files"
  },
  "storage": {
    "app_folder": "sptrans",
    "gtfs_folder": "gtfs",
    "raw_bucket": "raw",
    "trusted_bucket": "trusted"
  },
  "tables": {
    "trip_details_table_name": "trip_details"
  }
}
```

### Airflow (produção)
- Variable `gtfs_general` (JSON)
- Credenciais via Connections (GTFS e MinIO)

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
python gtfs-v2.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima

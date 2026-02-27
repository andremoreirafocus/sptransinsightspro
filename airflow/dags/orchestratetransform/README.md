## Objetivo deste subprojeto
Checar quais arquivos de posições de ônibus extraídos da API da SPTrans já foram disponibilizados na camada raw pelo microserviço.extractloadlivedata mas ainda não foram procesadors pela DAG transformlivedata.
A implementação final é feita via a DAG orchestratetransform do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática - via arquivo config.py - de acordo com o ambiente de execução, seja produção, via Airflow, ou desenvolvimento, local.


## O que este subprojeto faz
- lê da tabela de arquivos da camada raw que contem metadados sobre os arquivos de posição de ônibus e identifica quais ainda não foram processados
- para cada arquivo ainda não processado inicia a DAG de transformação 

## Pré-requisitos
- Disponibilidade do banco de dados do Airflow que é utilizado para manter a tabela de arquivos processados 
- Criação do arquivo de configurações

## Configurações
RAW_EVENTS_TABLE_NAME = "to_be_processed.raw"
DB_HOST="localhost"
DB_PORT=5432
DB_DATABASE="sptrans_insights"
DB_USER=<user_airflow_postgres>
DB_PASSWORD=<password_airflow_postgres>
DB_SSLMODE="prefer"

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
python orchestratetransform-v1.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima

## Estrutura da tabela de posições instantâneas enriquecidas criadas neste subprojeto usando comando equivalente SQL:


## Objetivo deste subprojeto
Checar quais arquivos de posições de ônibus extraídos da API da SPTrans já foram disponibilizados na camada raw pelo microserviço extractloadlivedata mas ainda não foram procesadors pela DAG transformlivedata.
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
As configurações são centralizadas em `config/config.py` e expostas como um único objeto com 1 seção:
- `general`

### Local/dev
- `general` vem do arquivo `dags-dev/orchestratetransform/config/orchestratetransform.json`
- `.env` em `dags-dev/orchestratetransform/.env` é usado apenas para credenciais de conexão

Credenciais esperadas no `.env`:
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

Chaves esperadas em `general`
```json
{
  "orchestration": {
    "target_dag": "transformlivedata-v7",
    "wait_time_seconds": 15
  },
  "tables": {
    "raw_events_table_name": "to_be_processed.raw"
  }
}
```

### Airflow (produção)
No Airflow, as configurações e credenciais são gerenciadas utilzando-se os recursos de Variables e Connections que são armazenadas pelo próprio Airflow, conforme listado a seguir. Qualquer alteração nessas informações deve ser feitas via UI do Airflow ou via linha de comando conectando-se ao webserver do Airflow via comando docker exec.
- Variable `orchestratetransform_general` (JSON)
- Credenciais via Connection (Airflow Postgres)

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

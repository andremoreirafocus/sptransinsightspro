## Objetivos deste subprojeto
Fazer a extração dos arquivos GTFS do portal do desenvolvedor da SPTrans para enriquecimento dos dados extraídos da API. 
A implementação final é feita via a DAG gtfs do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática via `pipeline_configurator`, de acordo com o ambiente de execução, seja produção (Airflow) ou desenvolvimento local.


## O que este subprojeto faz
- download de arquivos GTFS do portal do desenvolvedor da SPTrans utilizados para enriquecer dados das posicoes dos onibus obtidas via API
- valida os arquivos extraídos antes da carga na camada raw verificando sua existência e se o formato é CSV, além de um número mínimo de linhas mínimas)
- salva cada um dos arquivos na "pasta" gtfs do bucket raw no serviço de object storage
- executa a **TRANSFORMATION STAGE** para as tabelas base do GTFS:
  - transforma CSV em Parquet
  - valida com Great Expectations quando houver suite configurada para a tabela
  - salva primeiro em staging na camada trusted
  - em caso de falha de validação: move artefatos staged para quarentena e gera diagnóstico consolidado
  - em caso de sucesso: move artefatos staged para o caminho final para cada tabela
- executa a **ENRICHMENT STAGE** para `trip_details` com abordagem staging-first:
  - cria `trip_details` em `trusted/<gtfs_folder>/<staging_subfolder>/trip_details.parquet`
  - valida `trip_details` com Great Expectations quando houver suite configurada
  - em caso de falha de validação: move para `trusted/<gtfs_folder>/<quarantined_subfolder>/trip_details.parquet`
  - em caso de sucesso: move para `trusted/<gtfs_folder>/trip_details/trip_details.parquet`
- gera um único relatório consolidado de qualidade por execução da pipeline (`EXTRACT & LOAD`, `TRANSFORMATION`, `ENRICHMENT`)
- inclui artefato de linhagem de colunas de `trip_details` com detecção de drift (`warning: "lineage drift detected"`) quando a saída divergir do mapeamento declarado

## Pré-requisitos
- Obter as credenciais cadastre-se no portal do desenvolvedor da SPTRANS
- Disponibilidade de dois buckets: uma para a camada raw e outro para a camada trusted, previamente criados no serviço de object storage
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura e escrita aos bucket das camadas raw e trusted 
- Criação do arquivo de configurações

## Configurações
As configurações são centralizadas no módulo `pipeline_configurator` e expostas como um objeto canônico com:
- `general`
- `connections`

### Local/dev
- `general` vem do arquivo `dags-dev/gtfs/config/gtfs_general.json`
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
    "metadata_bucket": "metadata",
    "quality_report_folder": "quality-reports",
    "quarantined_subfolder": "quarantined",
    "staging_subfolder": "staging",
    "trusted_bucket": "trusted"
  },
  "tables": {
    "trip_details_table_name": "trip_details"
  },
  "notifications": {
    "webhook_url": "disabled"
  },
  "data_validations": {
    "expectations_validation": {
      "expectations_suites": [
        "data_expectations_stops",
        "data_expectations_stop_times",
        "data_expectations_trip_details"
      ]
    }
  }
}
```

Artefatos de expectations carregados automaticamente via `pipeline_configurator`:
- `dags-dev/gtfs/config/gtfs_data_expectations_stops.json`
- `dags-dev/gtfs/config/gtfs_data_expectations_stop_times.json`
- `dags-dev/gtfs/config/gtfs_data_expectations_trip_details.json`

### Fluxo da TRANSFORMATION STAGE
- Tabelas processadas: `stops`, `stop_times`, `routes`, `trips`, `frequencies`, `calendar`
- Caminhos na camada trusted:
  - Staging: `trusted/<gtfs_folder>/<staging_subfolder>/<table>.parquet`
  - Quarentena: `trusted/<gtfs_folder>/<quarantined_subfolder>/<table>.parquet`
  - Final: `trusted/<gtfs_folder>/<table>/<table>.parquet`

### Fluxo da ENRICHMENT STAGE (`trip_details`)
- Caminho de staging: `trusted/<gtfs_folder>/<staging_subfolder>/trip_details.parquet`
- Caminho de quarentena: `trusted/<gtfs_folder>/<quarantined_subfolder>/trip_details.parquet`
- Caminho final: `trusted/<gtfs_folder>/trip_details/trip_details.parquet`
- A validação GX de `trip_details` usa a suite `data_expectations_trip_details` quando configurada.
- Em falhas após escrita em staging, a pipeline tenta quarentenar o artefato staged para evitar resíduos órfãos.

### Relatório consolidado de qualidade
- Há exatamente um relatório por execução de `gtfs-v3.py`, com `summary` + `details`.
- O relatório consolida o resultado das três fases:
  - `extract_load_files`
  - `transformation`
  - `enrichment`
- Caminho do relatório:
  - `<metadata_bucket>/<quality_report_folder>/gtfs/year=YYYY/month=MM/day=DD/hour=HH/quality-report-gtfs_<HHMM>_<execution_suffix>.json`

### Relato de falha e webhook
- Em falhas de qualquer fase, a pipeline gera e persiste um relatório consolidado com:
  - `failure_phase`
  - `failure_message`
  - resultados de cada fase em `details.stages`
  - `validated_items_count`, `error_details`, `relocation_status`, `relocation_error` por fase
  - artefatos de `column_lineage` no estágio de enrichment
- O resumo (`summary`) é enviado via webhook quando `notifications.webhook_url` não estiver como `disabled`/`none`/`null`.

### Airflow (produção)
No Airflow, as configurações e credenciais são gerenciadas utilzando-se os recursos de Variables e Connections que são armazenadas pelo próprio Airflow, conforme listado a seguir. Qualquer alteração nessas informações deve ser feitas via UI do Airflow ou via linha de comando conectando-se ao webserver do Airflow via comando docker exec.
- Variable `gtfs_general` (JSON)
- Credenciais via Connections (GTFS e MinIO)

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .venv
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
python gtfs-v3.py

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas acima

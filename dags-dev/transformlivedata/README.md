## Objetivo deste subprojeto
Fazer a transformação dos dados extraídos de posição dos ôibus da API da SPTrans, já disponibilizados na camada raw pelo microserviço extractloadlivedata, enriquecendo-os com os dados obtidos do GTFS da SPTrans, extraidos e transformados pelo processo gtfs.
A implementação final é feita via a DAG transformlivedata do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática via `pipeline_configurator`, de acordo com o ambiente de execução, seja produção (Airflow) ou desenvolvimento local.


## O que este subprojeto faz
- lê um arquivo JSON com as posicoes instantâneas dos onibus fornecidos pela sptrans armazenado no bucket da camada raw no serviço de object storage particionados por ano, mes e dia
- valida o JSON bruto com um schema configurável em arquivo JSON
- transforma os dados em uma big table consolidada em memória enriquecendo-os com dados da tabela de detalhes das viagens gerada pelo subprocesso gtfs
- salva os dados de posição instantânea transformados e enriquecidos, da tabela em memória para uma pasta (prefixo) sptrans no bucket da camada trusted no serviço de object storage, particionados por ano, mes, dia e hora para acelerar a busca e filtragem por timestamp de extração das posições instantâneas dos ônibus
- valida o resultado transformado com Great Expectations a partir de uma suite configurada externamente em um arquivo JSON
- cria quarentena para registros inválidos em um bucket com particionamento igual à da camada trusted e enriquecido com o motivo da quarentena de cada registro
- gera e salva um relatório de qualidade com contagens de registros, métricas de transformação, issues detectadas e resumo de expectativas (sucessos, violações e exceções) em um bucket de metadados
- inclui no relatório de qualidade a lineage das colunas, que é gerada automaticamente com base no schema JSON utilizado na validação do dado bruto, mapeando os caminhos do JSON para as colunas transformadas e adiciona ao lineage as colunas de cada passo da transformação.

## Relatório de qualidade e observabilidade
O pipeline gera um relatório de qualidade em formato JSON no bucket de metadados. Esse relatório possui duas seções:
- `summary`: bloco enxuto para observabilidade (status, falhas, taxa de aceitação e contagens resumidas)
- `details`: corpo completo com métricas, issues, validações e artefatos

### Validações orientadas a configuração
O processo de validação é orientado a configuração, não a código:
- JSON Schema: (validação do dado bruto) definido em `dags-dev/transformlivedata/config/transformlivedata_raw_data_json_schema.json`
- Great Expectations (validação pós-transformação) definido em `dags-dev/transformlivedata/config/transformlivedata_data_expectations.json`

Isso permite atualizar regras sem alterar o código e facilita o reuso das validações em outros projetos e pipelines.

### Métricas de transformação
O relatório inclui métricas geradas durante a transformação:
- total de veículos processados, válidos e inválidos
- total de linhas processadas
- issues detectadas (ex.: veículos inválidos, viagens inválidas, erros de cálculo de distância)

### Validação com Great Expectations (GX)
Após a transformação, a tabela é validada com uma suite GX:
- violações e exceções são registradas
- linhas inválidas são isoladas e reportadas

### Quarentena
Registros inválidos oriundos do processo de transformação ou de validação pelo Great Expecttaions são salvos na camada de quarentena.
O relatório também indica se a persistência da quarentena foi bem sucedida.

### Resumo (summary)
O bloco `summary` traz um diagnóstico rápido para monitoramento:
- `status` (`PASS` | `WARN` | `FAIL`)
- `failure_phase` e `failure_message` quando há erro
- `rows_failed` = inválidos da transformação + falhas do GX
- `acceptance_rate` = `(raw_records - rows_failed) / raw_records`

Em caso de falha que interrompa o pipeline, o relatório ainda é gerado com informações parciais das etapas concluídas até o ponto de erro.  
Isso permite identificar exatamente em qual fase o processamento parou e quais métricas já haviam sido calculadas, mesmo quando o pipeline não completa o fluxo inteiro.

- Na pasta [samples](./samples) há um exemplo curado manualmente do relatório consolidado de qualidade: [quality-report-positions_HHMM_uuid.json](./samples/quality-report-positions_HHMM_uuid.json).

### Estrutura simplificada (exemplo)
```json
{
  "summary": {
    "status": "WARN",
    "rows_failed": 9,
    "acceptance_rate": 0.999,
    "failure_phase": null
  },
  "details": {
    "transformation_row_counts": { ... },
    "transformation_metrics": { ... },
    "transformation_issues": { ... },
    "expectations_summary": { ... },
    "artifacts": { ... }
  }
}
```

### Notificação e observabilidade (alertservice)
O resumo (`summary`) do relatório é enviado via webhook para o microserviço `alertservice` quando este está habilitado.
O resumo contém informações de status, fases de falha e métricas de transformação para disparar alertas imediatos (FAIL) ou cumulativos (WARN) configurados no alertservice.
O alertservice envia notificações por e-mail com base em limiares configuráveis por pipeline:
- **FAIL (imediato)**: qualquer status FAIL gera e-mail imediato
- **WARN (cumulativo)**: alertas de warning são aggregados dentro de uma janela de tempo configurável (ex.: 24 horas) e enviados quando limiares são atingidos

## Pré-requisitos
- Disponibilidade de quatro buckets: para a camada raw, para a camada trusted, para os registros em quarentena e para os relatórios de qualidade, previamente criados no serviço de object storage
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura ao bucket da camada raw e leitura e escrita na camada trusted
- Arquivo `.env` com as credenciais necessárias
- Um template está disponível em `.env.example`
- Criação do arquivo de configurações

## Configurações
As configurações são centralizadas no módulo `pipeline_configurator` e expostas como um objeto canônico com:
- `general`
- `connections`
- `raw_data_json_schema` (carregado automaticamente via `data_validations`)
- `data_expectations` (carregado automaticamente via `data_validations`)

### Local/dev
- `general` vem do arquivo `dags-dev/transformlivedata/config/transformlivedata_general.json`
- `raw_data_json_schema` vem de `dags-dev/transformlivedata/config/transformlivedata_raw_data_json_schema.json`
- `data_expectations` vem de `dags-dev/transformlivedata/config/transformlivedata_data_expectations.json`
- os artefatos acima são carregados automaticamente pelo `pipeline_configurator` com base na seção `data_validations` do `general`
- `.env` em `dags-dev/transformlivedata/.env` é usado apenas para credenciais de conexão

Credenciais esperadas no `.env`:
MINIO_ENDPOINT=<hostname:port>
ACCESS_KEY=<key>
SECRET_KEY=<secret>
DB_HOST=<db_hostname>
DB_PORT=<PORT>
DB_DATABASE=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
DB_SSLMODE="prefer"

Chaves esperadas em `general`
```json
{
  "storage": {
    "raw_bucket": "raw",
    "trusted_bucket": "trusted",
    "quarantined_bucket": "quarantined",
    "metadata_bucket": "metadata",
    "app_folder": "sptrans",
    "gtfs_folder": "gtfs",
    "quality_report_folder": "quality-reports"
  },
  "tables": {
    "positions_table_name": "positions",
    "trip_details_table_name": "trip_details",
    "raw_events_table_name": "to_be_processed.raw"
  },
  "compression": {
    "raw_data_compression": true,
    "raw_data_compression_extension": ".zst"
  },
  "notifications": {
    "webhook_url": "http://localhost:8000/notify"  // URL do microserviço alertservice
  },
  "data_validations": {
    "json_validation": {
      "schemas": ["raw_data_json_schema"]
    },
    "expectations_validation": {
      "expectations_suites": ["data_expectations"]
    }
  }
}
```

Observação: `webhook_url` é a URL do microserviço `alertservice` e é obrigatório.  
Para desativar notificações para o alertservice, use o valor `"disabled"`.

## Testes unitários
Os testes unitários deste subprojeto estão restritos ao módulo `transform_positions.py` e cobrem o núcleo da lógica de transformação, incluindo:
- validação de payloads e estrutura mínima dos dados
- mapeamento e enriquecimento dos campos transformados
- cálculos e agregações aplicadas às posições dos veículos
- cenários de erro para dados ausentes ou inválidos


## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Configurações de Banco de dados que devem ser feitas antes da execução:
A tabela `to_be_processed.raw` cjá deve ter sido criada conforme instruções em [extractloadlivedata/README.md](../../extractloadlivedata/README.md).

### Airflow (produção)
No Airflow, as configurações e credenciais são gerenciadas utilzando-se os recursos de Variables e Connections que são armazenadas pelo próprio Airflow, conforme listado a seguir. Qualquer alteração nessas informações deve ser feitas via UI do Airflow ou via linha de comando conectando-se ao webserver do Airflow via comando docker exec.
- Variable `transformlivedata_general` (JSON, inclui a seção `data_validations`)
- Variable `transformlivedata_raw_data_json_schema` (JSON)
- Variable `transformlivedata_data_expectations` (JSON)
- Credenciais via Connections (MinIO e Postgres)

Antes da execução da DAG no Airflow, a tabela `to_be_processed.raw` já deve estar criada conforme instruções acima.

## Instruções para execução em modo local
Crie `dags-dev/transformlivedata/.env` com base em `.env.example` preenchendo todos os campos:
Com a tabela já criada conforme instruções acima, execute:

```shell
python ./transformlivedata-v<version number>.py
```

Exemplo: 
```shell
python ./transformlivedata-v2.py
```

Para reprocessamento pontual do pipeline, utilize o script [transformlivedata-backfill-v9.py](../transformlivedata-backfill-v9.py). 
Em modo local, o script percorre toda a janela entre `BACKFILL_START` e `BACKFILL_END` em passos de `BACKFILL_STEP_MINUTES` definidos no próprio script.

## Estrutura da tabela de posições instantâneas enriquecidas criadas neste subprojeto usando comando equivalente SQL:

Na pasta [samples](./samples) há um exemplo curado manualmente do artefato de saída `positions`: [positions_HHMM_0.parquet](./samples/positions_HHMM_0.parquet), apenas para referência documental.

```sql
CREATE TABLE trusted.positions (
    id BIGSERIAL PRIMARY KEY,
    extracao_ts TIMESTAMPTZ,       -- metadata.extracted_at: 
    veiculo_id INTEGER,            -- p: id do veiculo
    linha_lt TEXT,                 -- c: Letreiro completo
    linha_code INTEGER,            -- cl: Código linha
    linha_sentido INTEGER,         -- sl: Sentido
    lt_destino TEXT,               -- lt0: Destino
    lt_origem TEXT,                -- lt1: Origem
    veiculo_acessivel BOOLEAN,     -- a: Acessível
    veiculo_ts TIMESTAMPTZ,        -- ta: Timestamp UTC
    veiculo_lat DOUBLE PRECISION,  -- py: Latitude
    veiculo_long DOUBLE PRECISION,  -- px: Longitude
    is_circular BOOLEAN,
    first_stop_id INTEGER,
    first_stop_lat DOUBLE PRECISION,
    first_stop_lon DOUBLE PRECISION,
    last_stop_id INTEGER,
    last_stop_lat DOUBLE PRECISION,
    last_stop_lon DOUBLE PRECISION,
    distance_to_first_stop DOUBLE PRECISION,
    distance_to_last_stop DOUBLE PRECISION
);
```

## Estrutura da tabela antes do enriquecimento por trip_details
CREATE TABLE trusted.positions (
    id BIGSERIAL PRIMARY KEY,
    extracao_ts TIMESTAMPTZ,       -- metadata.extracted_at: 
    veiculo_id INTEGER,            -- p: id do veiculo
    linha_lt TEXT,                 -- c: Letreiro completo
    linha_code INTEGER,            -- cl: Código linha
    linha_sentido INTEGER,         -- sl: Sentido
    lt_destino TEXT,               -- lt0: Destino
    lt_origem TEXT,                -- lt1: Origem
    veiculo_acessivel BOOLEAN,     -- a: Acessível
    veiculo_ts TIMESTAMPTZ,        -- ta: Timestamp UTC
    veiculo_lat DOUBLE PRECISION,  -- py: Latitude
    veiculo_long DOUBLE PRECISION  -- px: Longitude
);

####################  EXPLORACAO POSITIONS ################

# exploracao para a camada refined
select count(*) from  trusted.positions

select linha_lt, count(1) from  trusted.positions as p
group by p.linha_lt
order by count(1) desc
6000-10	6396
2290-10	5272
3459-10	5013
5175-10	3973
4310-10	3675

-- '1015-10' é uma linha circular
-- '6000-10' é uma linha regular
select veiculo_id, count(1) from  trusted.positions as p
--where linha_lt ='6000-10'
--where linha_lt ='1015-10'
where linha_lt ='2290-10'
group by p.veiculo_id
order by count(1) desc

#melhor org das colunas
select veiculo_ts, veiculo_id,
       distance_to_first_stop, distance_to_last_stop,
       is_circular, linha_sentido, 
       veiculo_lat, veiculo_long, 
       first_stop_lat, first_stop_lon,  last_stop_lat, last_stop_lon, first_stop_id, last_stop_id, lt_destino, lt_origem
       from  trusted.positions
--where linha_lt = '6000-10' and veiculo_id = '63541'
--where linha_lt = '1015-10' and veiculo_id = '16450'
where linha_lt = '2290-10' and veiculo_id = '41539'
order by veiculo_ts 


-- ordem original das colunas
select extracao_ts, veiculo_id, linha_lt, linha_code, linha_sentido,
        lt_destino, lt_origem, veiculo_acessivel, veiculo_ts,
        veiculo_lat, veiculo_long, is_circular, first_stop_id, first_stop_lat,
        first_stop_lon, last_stop_id, last_stop_lat, last_stop_lon,
        distance_to_first_stop, distance_to_last_stop from trusted.positions as p
where linha_lt = '6000-10'-- '1015-10'
and veiculo_id = '63541'-- 16450

select count(*) from trusted.positions
order by veiculo_ts 

----------------------------------------------------

########################### EXPLORACAO ############################

"Linhas com mais posicoes "
SELECT linha_lt, COUNT(linha_lt)
FROM trusted.posicoes
group by linha_lt
order by  COUNT(linha_lt) desc;
2290-10	22631
6000-10	21545
3459-10	17448
5175-10	16661
4310-10	15434

"Veiculos com mais posicoes distintas de uma linha"
SELECT 
    veiculo_id, 
    COUNT(DISTINCT (veiculo_lat, veiculo_long)) AS unique_positions_count
FROM 
    trusted.posicoes
WHERE 
    linha_lt = '2290-10' 
    AND linha_sentido = 1
GROUP BY 
    veiculo_id
ORDER BY 
    unique_positions_count DESC;
41559	163
41514	161
41522	155
41580	153
41595	152

# Descobrindo as paradas dos terminais da linha
select * from trusted.stop_times
where trip_id = '2290-10-0';
2290-10-0	15:00:00	15:00:00	750006788	1
2290-10-0	17:42:00	17:42:00	800016547	69

select * from trusted.stop_times
where trip_id = '2290-10-1';
2290-10-1	17:00:00	17:00:00	800016547	1
2290-10-1	19:22:00	19:22:00	750006786	54

# Descobrindo a posicao dos terminal origem
select * from trusted.stops
where stop_id = 800016547;
800016547	Terminal Parque Dom Pedro II - Plat 06	Term. Parque Dom Pedro II - Plat 06 Ref.: Av Do Estado/ Vdto Antonio Nakashima - (pmv - 59)	-23.547014	-46.629795

# Tentando achar em que momento o veiculo estava no terminal origem
select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.546000 and veiculo_lat > -23.548000 ) and (veiculo_long < -46.629000 and veiculo_long > -46.631000)
order by veiculo_ts;

# Tentando achar em que momento o veiculo estava no terminal origem
select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.546500 and veiculo_lat > -23.547500 ) and (veiculo_long < -46.629500 and veiculo_long > -46.630500)
order by veiculo_ts;

# Descobrindo a posicao dos terminal destino
select * from trusted.stops
where stop_id = 750006788;
750006788	Terminal S. Mateus - Plat. D	Term. S. Mateus - Plat. D Ref.: Av Sapopemba/ Pc Felisberto Fernandes Da Silva	-23.613206	-46.476113

# Tentando achar em que momento o veiculo estava no terminal origem
select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.612000 and veiculo_lat > -23.614000 ) and (veiculo_long < -46.475000 and veiculo_long > -46.477000);


# Achando a distancia estimada em metros do veiculo em relaçao ao terminal de origem, ordenado pela distancia e horario
WITH constants AS (
    SELECT 
        -23.547014 AS lat_ref, 
        -46.629795 AS long_ref
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_ref, 2) + 
        POW(p.veiculo_long - c.long_ref, 2)
    ) * 106428 AS distance_meters
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
ORDER BY 
    distance_meters, veiculo_ts;

# Achando a distancia estimada em metros do veiculo em relaçao ao terminal de origem, ordenado pelo horario
WITH constants AS (
    SELECT 
        -23.547014 AS lat_ref, 
        -46.629795 AS long_ref
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_ref, 2) + 
        POW(p.veiculo_long - c.long_ref, 2)
    ) * 106428 AS distance_meters
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
ORDER BY 
    veiculo_ts;


# Achando a distancia estimada em metros do veiculo em relaçao ao terminal de origem, e ao terminal de destino ordenado pelo horario
WITH constants AS (
    SELECT 
        -23.547014 AS lat_tp, 
        -46.629795 AS long_tp,
        -23.613206 AS lat_ts,  	
        -46.476113 AS long_ts
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_tp, 2) + 
        POW(p.veiculo_long - c.long_tp, 2)
    ) * 106428 AS distance_meters_tp,
    SQRT(
        POW(p.veiculo_lat - c.lat_ts, 2) + 
        POW(p.veiculo_long - c.long_ts, 2)
    ) * 106428 AS distance_meters_ts
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
ORDER BY 
    veiculo_ts;



----------------------------------------------------
Queries de exploração:

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1
order by veiculo_ts;

select count(*) from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1;
8938

select count(*) from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 2;
6197

select distinct(veiculo_id) from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1;

select count(distinct(veiculo_id)) from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1;
64 veiculos

#Quantidade de posicoes por veiculo nesta linha e sentido
SELECT 
    veiculo_id, 
    COUNT(*) AS total_records
FROM 
    trusted.posicoes
WHERE 
    linha_lt = '2290-10' 
    AND linha_sentido = 1
GROUP BY 
    veiculo_id
ORDER BY 
    total_records DESC;
# o 41542 é o de maior número de posicoes

#As posicoes de um veiculo em um sentido
select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41542
order by veiculo_ts;

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41542 and veiculo_lat = -23.613206 and veiculo_long = -46.476113;

select * from trusted.stops
where stop_id = 750006788;
750006788	Terminal S. Mateus - Plat. D	Term. S. Mateus - Plat. D Ref.: Av Sapopemba/ Pc Felisberto Fernandes Da Silva	-23.613206	-46.476113

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41542 and (veiculo_lat < -23.612000 and veiculo_lat > -23.614000 ) and (veiculo_long < -46.475000 and veiculo_long > -46.477000);
3396743	2026-01-13 12:34:19.233 -0300	41542	2290-10	718	1	TERM. PQ. D. PEDRO II	TERM. SÃO MATEUS	41542	true	2026-01-13 15:33:50.000 -0300	-23.6123985	-46.4752955
3985518	2026-01-13 12:34:19.233 -0300	41542	2290-10	718	1	TERM. PQ. D. PEDRO II	TERM. SÃO MATEUS	41542	true	2026-01-13 15:33:50.000 -0300	-23.6123985	-46.4752955

select * from trusted.stops
where stop_id = 800016547;
800016547	Terminal Parque Dom Pedro II - Plat 06	Term. Parque Dom Pedro II - Plat 06 Ref.: Av Do Estado/ Vdto Antonio Nakashima - (pmv - 59)	-23.547014	-46.629795

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41542 and (veiculo_lat < -23.546000 and veiculo_lat > -23.548000 ) and (veiculo_long < -46.629000 and veiculo_long > -46.631000)
order by veiculo_ts;


"Veiculos com mais posicoes distintas"
SELECT 
    veiculo_id, 
    COUNT(DISTINCT (veiculo_lat, veiculo_long)) AS unique_positions_count
FROM 
    trusted.posicoes
WHERE 
    linha_lt = '2290-10' 
    AND linha_sentido = 1
GROUP BY 
    veiculo_id
ORDER BY 
    unique_positions_count DESC;
41559	163
41514	161
41522	155
41580	153
41595	152

select * from trusted.stops
where stop_id = 800016547;
800016547	Terminal Parque Dom Pedro II - Plat 06	Term. Parque Dom Pedro II - Plat 06 Ref.: Av Do Estado/ Vdto Antonio Nakashima - (pmv - 59)	-23.547014	-46.629795

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.546000 and veiculo_lat > -23.548000 ) and (veiculo_long < -46.629000 and veiculo_long > -46.631000)
order by veiculo_ts;

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.546500 and veiculo_lat > -23.547500 ) and (veiculo_long < -46.629500 and veiculo_long > -46.630500)
order by veiculo_ts;

select * from trusted.stops
where stop_id = 750006788;
750006788	Terminal S. Mateus - Plat. D	Term. S. Mateus - Plat. D Ref.: Av Sapopemba/ Pc Felisberto Fernandes Da Silva	-23.613206	-46.476113

select * from trusted.posicoes
where linha_lt = '2290-10' and linha_sentido = 1 and veiculo_id = 41559 and (veiculo_lat < -23.612000 and veiculo_lat > -23.614000 ) and (veiculo_long < -46.475000 and veiculo_long > -46.477000);


filtrado
WITH constants AS (
    SELECT 
        -23.547014 AS lat_ref, 
        -46.629795 AS long_ref
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_ref, 2) + 
        POW(p.veiculo_long - c.long_ref, 2)
    ) * 106428 AS distance_meters
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
    AND (p.veiculo_lat < -23.546500 AND p.veiculo_lat > -23.547500) 
    AND (p.veiculo_long < -46.629500 AND p.veiculo_long > -46.630500)
ORDER BY 
    p.veiculo_ts;


WITH constants AS (
    SELECT 
        -23.547014 AS lat_ref, 
        -46.629795 AS long_ref
)
SELECT 
    p.*,
    SQRT(
        POW(p.veiculo_lat - c.lat_ref, 2) + 
        POW(p.veiculo_long - c.long_ref, 2)
    ) * 106428 AS distance_meters
FROM 
    trusted.posicoes p, 
    constants c
WHERE 
    p.linha_lt = '2290-10' 
    AND p.linha_sentido = 1 
    AND p.veiculo_id = 41559 
ORDER BY 
    distance_meters, veiculo_ts;

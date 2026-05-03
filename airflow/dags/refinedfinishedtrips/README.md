## Objetivo deste subprojeto
Calcular as viagens finalizadas a partir do histórico de posições instantâneas dos ônibus e armazena o seu histórico consolidado para análise de eficiência.
A implementação final é feita via a DAG refinedfinishedtrips do Airflow.
O desenvolvimento é feito em uma pasta dag-dev que contem cada um dos subprojetos implementados via Airflow, aumentando a agilidade durante a experimentação.
As configurações são carregadas de forma automática via `pipeline_configurator`, de acordo com o ambiente de execução, seja produção (Airflow) ou desenvolvimento local.

## O que este subprojeto faz
Para cada linha e veículo: 
- lê as posições instantâneas armazenadas na tabela de posições armazendas sptrans no bucket da camada trusted no serviço de object storage, particionados por ano, mes, dia e hora, correspondentes a um período de tempo de análise
- verifica a qualidade dos dados de posição antes de processar as viagens, executando duas verificações:
  - **freshness**: valida se o timestamp mais recente dos veículos está dentro do limiar de atualização esperado
  - **gaps de extração**: valida se não há lacunas significativas entre os timestamps de extração na janela recente
- em caso de falha nas verificações de qualidade: interrompe o pipeline, salva um relatório de qualidade no bucket de metadata e notifica via webhook
- em caso de aviso nas verificações de qualidade: salva um relatório de qualidade no bucket de metadata, notifica via webhook e continua o processamento
- calcula as viagens finalizadas durante este período de tempo de análise
- verifica a qualidade da extração de viagens, executando duas verificações sobre a janela efetiva de extração (medida pelo intervalo entre o primeiro e o último `extracao_ts` do conjunto de dados):
  - **zero trips**: aviso se a janela efetiva de extração exceder o limiar configurado e nenhuma viagem for identificada
  - **low trip count**: aviso se a janela efetiva de extração exceder o limiar configurado e o número de viagens identificadas estiver abaixo do mínimo esperado
- em caso de falha durante a fase de extração de viagens: salva um relatório de falha com os resultados parciais já disponíveis, notifica via webhook e interrompe a execução
- salva as viagens finalizadas na camada refined implementada no banco de dados analítico de baixa latência, para consumo da camada de visualização
- verifica a qualidade da persistência, executando uma verificação:
  - **duplicatas**: aviso se todas as viagens da execução já estavam presentes no banco de dados (duplicatas)
- em caso de falha durante a fase de persistência: salva um relatório de falha com os resultados parciais já disponíveis, notifica via webhook e interrompe a execução
- ao final de cada execução bem-sucedida, salva um relatório de qualidade completo no bucket de metadata e notifica via webhook com o status consolidado das três fases (posições, extração de viagens e persistência)

## Algoritmo de extração de viagens
O algoritmo atual foi desenhado para produzir viagens com maior fidelidade operacional, principalmente em três dimensões centrais para análise:
- `trip_start_time`
- `trip_end_time`
- `duration`

Esses campos são especialmente sensíveis a ruídos comuns do dado de posição em produção, como:
- ônibus parado em terminal antes de iniciar a viagem
- mudança de `linha_sentido` em momentos de borda operacional
- amostras espaciais isoladas incompatíveis com a trajetória real
- janelas de processamento que começam ou terminam no meio de um ciclo operacional

Por isso, a extração não se apoia apenas em `linha_sentido`. A lógica usa principalmente geolocalização para identificar início e fim de viagem e usa `linha_sentido` apenas como sinal complementar quando necessário, especialmente em linhas circulares.

### 1. Sanitização espacial das posições
Antes de extrair viagens, o pipeline sanitiza a sequência de posições de cada combinação linha/veículo.

Essa sanitização remove amostras isoladas espacialmente inválidas, caracterizadas por:
- um ponto intermediário incompatível com o ponto anterior
- incompatível também com o ponto seguinte
- enquanto a ligação direta entre anterior e seguinte permanece plausível

Na prática, isso elimina "teletransportes" pontuais do ônibus sem descartar sequências inteiras de dados.
Esse passo é importante porque um único ponto espacial inválido pode:
- antecipar artificialmente um início de viagem
- encerrar uma viagem no ponto errado
- inflar ou reduzir incorretamente a duração observada

Com isso, a extração final fica mais estável e mais aderente ao deslocamento real do veículo.

### 2. Linhas não circulares
Para linhas não circulares, o algoritmo usa proximidade geográfica aos terminais de referência:
- `first_stop`
- `last_stop`

Uma viagem é formada quando:
- o veículo é observado próximo de um terminal
- depois se afasta desse terminal
- e posteriormente chega ao terminal oposto

O início da viagem é registrado como a última posição ainda próxima do terminal de partida antes do movimento real.
O fim da viagem é registrado quando o veículo entra na zona de proximidade do terminal de chegada.

Assim, a detecção depende da trajetória espacial observada, e não apenas de `linha_sentido`.
Essa estratégia é necessária porque:
- o ônibus pode permanecer parado no terminal por vários ciclos de coleta antes de partir
- o `linha_sentido` pode mudar em momentos de borda antes ou depois do deslocamento efetivo

Ao usar a fronteira espacial observada, o algoritmo melhora diretamente:
- a precisão do início da viagem
- a precisão do fim da viagem
- a qualidade da duração calculada

### 3. Linhas circulares
Para linhas circulares, o algoritmo usa uma composição de dois sinais de fronteira:
- proximidade ao terminal/âncora
- mudança de `linha_sentido`

Após a primeira sincronização do veículo com a região do terminal, o algoritmo passa a detectar viagens considerando que:
- uma viagem pode começar por saída do terminal
- uma viagem também pode começar por mudança de `linha_sentido`
- uma viagem pode terminar por retorno ao terminal
- uma viagem também pode terminar por mudança de `linha_sentido`

Isso evita perda de viagens quando a janela de análise começa no meio de um ciclo ou quando a mudança de sentido ocorre fora da área do terminal.
Esse comportamento é importante porque, em linhas circulares:
- a janela de processamento pode não capturar a saída do terminal
- uma viagem pode precisar ser encerrada por retorno ao terminal
- outra pode precisar ser segmentada por mudança operacional de `linha_sentido`

Sem essa composição de sinais, viagens circulares tenderiam a ficar subdetectadas ou com limites temporais imprecisos.

### 4. Remoção de tempo parado em terminal
Períodos em que o ônibus permanece parado aguardando início de operação não devem inflar a duração da viagem.

Por isso, o algoritmo:
- remove o dwell no terminal do início da viagem
- usa como início efetivo a última posição no terminal imediatamente anterior à saída real

Essa remoção é aplicada apenas em contexto de terminal, não durante o percurso em rota.
Isso é necessário para que a duração represente o tempo de operação da viagem, e não o tempo de espera do veículo estacionado antes da partida.

### 5. Validação de sentido
O campo `linha_sentido` não é usado como única base para descobrir viagens.

Ele é usado como sinal complementar:
- principalmente para segmentação de linhas circulares
- e para validação de consistência da viagem detectada

Na validação, o algoritmo desconsidera amostras de borda próximas ao terminal, pois nessas regiões o `linha_sentido` pode mudar antes ou depois do deslocamento real devido à granularidade temporal da coleta.

Em conjunto, essas decisões tornam a tabela de viagens finalizadas mais confiável para:
- análise de eficiência operacional
- comparação de duração entre viagens
- avaliação de ciclo e regularidade
- consumo analítico na camada de visualização

## Relato de qualidade e notificação
O pipeline produz relatórios estruturados para três fases:
- `positions`
- `trip_extraction`
- `persistence`

Os relatórios de falha preservam os resultados parciais disponíveis até o ponto da interrupção.
Assim, uma falha em `trip_extraction` ou `persistence` não perde o contexto já calculado nas fases anteriores.

No relatório final, a fase `trip_extraction` também expõe métricas operacionais agregadas da execução, incluindo:
- `trips_extracted`
- `source_sentido_discrepancies`
- `sanitization_dropped_points`
- `vehicle_line_groups_processed`
- `input_position_records`

O resumo (`summary`) segue o contrato comum consumido pelo `alertservice`.
O pipeline envia webhook para:
- falhas em `positions`
- avisos em `positions`
- falhas em `trip_extraction`
- falhas em `persistence`
- relatório final consolidado

O envio do webhook é explicitamente registrado em log, indicando se:
- a notificação foi enviada
- a notificação estava desabilitada
- a tentativa falhou

## Pré-requisitos
- Disponibilidade do buckets da camada trusted, previamente criado no serviço de object storage
- Disponibilidade do bucket da camada metadata no serviço de object storage para armazenamento dos relatórios de qualidade
- Criação de uma chave de acesso ao serviço de object storage cadastrada no arquivo de configurações com acesso de leitura ao bucket na camada trusted e escrita ao bucket na camada metadata
- Disponibilidade do serviço de banco de dados analítico, atualmente o PostgreSQL, para armazenamento dos dados na camada refined
- alertservice disponível e configurado para receber notificações de qualidade (configurável via `webhook_url`; use `"disabled"` para desativar)
- Arquivo `.env` com as credenciais necessárias
- Um template está disponível em `.env.example`
- Criação do arquivo de configurações

## Configurações
As configurações são centralizadas no módulo `pipeline_configurator` e expostas como um objeto canônico com:
- `general`
- `connections`

### Local/dev
- `general` vem do arquivo `dags-dev/refinedfinishedtrips/config/refinedfinishedtrips_general.json`
- `.env` em `dags-dev/refinedfinishedtrips/.env` é usado apenas para credenciais de conexão

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
  "analysis": {
    "hours_window": 3
  },
  "storage": {
    "app_folder": "sptrans",
    "trusted_bucket": "trusted",
    "metadata_bucket": "metadata",
    "quality_report_folder": "quality-reports"
  },
  "tables": {
    "positions_table_name": "positions",
    "finished_trips_table_name": "refined.finished_trips"
  },
  "quality": {
    "freshness_warn_staleness_minutes": 10,
    "freshness_fail_staleness_minutes": 30,
    "gaps_warn_gap_minutes": 5,
    "gaps_fail_gap_minutes": 15,
    "gaps_recent_window_minutes": 60,
    "trips_effective_window_threshold_minutes": 60,
    "trips_min_trips_threshold": 5
  },
  "notifications": {
    "webhook_url": "http://localhost:8000/notify"
  }
}
```

### Airflow (produção)
No Airflow, as configurações e credenciais são gerenciadas utilzando-se os recursos de Variables e Connections que são armazenadas pelo próprio Airflow, conforme listado a seguir. Qualquer alteração nessas informações deve ser feitas via UI do Airflow ou via linha de comando conectando-se ao webserver do Airflow via comando docker exec.
- Variable `refinedfinishedtrips_general` (JSON)
- Credenciais via Connections (MinIO e Postgres)

Para desativar notificações do `alertservice`, configure:
- `notifications.webhook_url = "disabled"`

## Instruções para instalação
Para instalar os requisitos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução em modo local
Crie `dags-dev/refinedfinishedtrips/.env` com base em `.env.example` preenchendo todos os campos.
Criar tabelas conforme instruções abaixo:

```shell
python ./refinedfinishedtrips-v4.py
```

## Configurações de Banco de dados que devem ser feitas antes da execução:
## Para criar as tabelas e índices necessários ao subprojeto:

Database commands:

```
docker exec -it postgres bash
psql -U postgres -W
```

```sql
CREATE DATABASE sptrans_insights;
```

\c sptrans_insights

Com particonamemto:

```sql
CREATE SCHEMA partman;
CREATE EXTENSION pg_partman SCHEMA partman;

CREATE SCHEMA refined;



CREATE TABLE refined.finished_trips (
    trip_id TEXT,
    vehicle_id INTEGER,
    trip_start_time TIMESTAMPTZ NOT NULL,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION,
    PRIMARY KEY (trip_start_time, vehicle_id, trip_id)
) PARTITION BY RANGE (trip_start_time);

-- 2. Initialize partitioning
-- This creates the first few partitions based on the current time
SELECT partman.create_parent(
    p_parent_table := 'refined.finished_trips',
    p_control := 'trip_start_time',
    p_interval := '1 hour',
    p_premake := 4
);

-- 3. Set the 24-hour "Automatic Purge" policy
UPDATE partman.part_config 
SET retention = '24 hours', 
    retention_keep_table = 'f' 
WHERE parent_table = 'refined.finished_trips';

-- Optimized Search Index for PowerBI
-- This supports searching for a specific route/direction 
-- and narrowing it down by bus.
CREATE INDEX idx_trip_lookup 
ON refined.finished_trips (trip_id, vehicle_id);

-- This will create future partitions and check if any are > 24h old to drop
SELECT partman.run_maintenance('refined.finished_trips');

-- to verify
SELECT 
    parent_table, 
    control, 
    partition_interval, 
    retention,
    automatic_maintenance
FROM partman.part_config
WHERE parent_table = 'refined.finished_trips';

-- To check existing partitions
SELECT * FROM partman.show_partitions('refined.finished_trips');


-- to check partitions usage
SELECT 
    nmsp_parent.nspname AS parent_schema,
    parent.relname AS parent_table,
    child.relname AS partition_name,
    pg_size_pretty(pg_total_relation_size(child.oid)) AS total_size,
    child.reltuples::bigint AS estimated_row_count
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
WHERE parent.relname = 'finished_trips'
ORDER BY child.relname DESC;
```

Deprecated
CREATE TABLE refined.finished_trips (
    trip_id TEXT,             -- e.g., '101A_0'
    vehicle_id INTEGER,       -- e.g., 505
    trip_start_time TIMESTAMPTZ,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION,
    -- This combination is guaranteed unique by your bus logic
    PRIMARY KEY (trip_start_time, vehicle_id, trip_id)
);



#Tabela usada apenas em testes de algoritmo experimental
```sql
CREATE TABLE trusted.ongoing_trips (
    id BIGSERIAL PRIMARY KEY,
    trip_id TEXT,
    vehicle_id INTEGER,
    trip_start_time TIMESTAMPTZ,
    trip_end_time TIMESTAMPTZ,
    duration INTERVAL,
    is_circular BOOLEAN,
    average_speed DOUBLE PRECISION
);

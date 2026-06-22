## Objetivo deste subprojeto
Automatizar as operações de implantação e promoção de código, garantindo que lint, SAST, type checking e testes unitários sejam executados antes de qualquer alteração em produção.

## O que este subprojeto faz
- valida a qualidade do código (lint via `ruff`, SAST via `bandit` e testes unitários via `pytest`) antes de qualquer operação
- valida tipagem estática com `mypy` antes de qualquer operação
- promove uma pipeline do ambiente de desenvolvimento (`dags-dev`) para o ambiente de produção (`airflow/dags`), sincronizando também os módulos compartilhados `infra`, `quality`, `observability` e `pipeline_configurator`
- realiza o build e o redeploy de um microserviço via `docker compose`

## Pré-requisitos
- Python 3.10+
- `ruff`, `bandit`, `mypy` e `pytest` instalados no ambiente Python utilizado para executar os scripts
- `rsync` instalado (para promoção de pipelines)
- Docker e Docker Compose instalados (para deploy de microserviços)
- Executar os scripts a partir da pasta `automation/` ou com o PATH correto para os módulos auxiliares

## Scripts disponíveis

### `platform_bootstrap_and_start.sh`
Sobe a plataforma com bootstrap prévio da infraestrutura e do Airflow para evitar falhas de inicialização por ausência de artefatos obrigatórios.

**O que faz, em ordem:**
1. Sobe `airflow_postgres`, `postgres` e `minio`
2. Aguarda os serviços de infraestrutura ficarem disponíveis
3. Executa `bootstrap_minio.sh`
4. Executa `bootstrap_airflow_postgres.sh`
5. Executa `bootstrap_postgres.sh`
6. Sobe `airflow_webserver` e `airflow_scheduler`
7. Executa `bootstrap_airflow_app.sh`
8. Executa `bootstrap_observability.sh`
9. Executa `bootstrap_extractloadlivedata.sh`
10. Sobe o restante da plataforma com `docker compose up -d`
11. Executa `bootstrap_metabase.sh`

**Uso:**
```bash
cd automation
./platform_bootstrap_and_start.sh
```

---

### `bootstrap_minio.sh`
Garante que a credencial de acesso da plataforma e os buckets necessários existam no MinIO.

**O que faz, em ordem:**
1. Aguarda o MinIO ficar disponível
2. Autentica com `MINIO_ROOT_USER` e `MINIO_ROOT_PASSWORD`
3. Garante a existência do usuário de acesso definido por `MINIO_PLATFORM_ACCESS_KEY` e `MINIO_PLATFORM_SECRET_KEY`
4. Anexa a policy `readwrite` no primeiro bootstrap desse usuário
5. Provisiona os buckets declarados em `minio_buckets.json` (operação idempotente)

**Uso:**
```bash
cd automation
./bootstrap_minio.sh
```

---

### `minio_buckets.json`
Declaração dos buckets que devem existir no MinIO. Lido por `bootstrap_minio.sh` durante o bootstrap.

Para adicionar um novo bucket, basta incluir uma entrada neste arquivo — nenhuma alteração no script é necessária.

```json
{
  "buckets": [
    { "name": "raw" },
    { "name": "trusted" },
    { "name": "quarantined" },
    { "name": "metadata" }
  ]
}
```

---

### `bootstrap_airflow_app.sh`
Garante o bootstrap da camada de aplicação do Airflow.

**O que faz, em ordem:**
1. Aguarda o CLI do Airflow ficar utilizável no `airflow_webserver`
2. Garante a existência do usuário admin definido no `.env`
3. Importa as Airflow Variables do bootstrap
4. Importa as Airflow Connections do bootstrap

**Uso:**
```bash
cd automation
./bootstrap_airflow_app.sh
```

---

### `bootstrap_observability.sh`
Garante o bootstrap da stack de observabilidade.

**O que faz, em ordem:**
1. Sobe `loki`, `promtail`, `grafana` e `alertmanager`
2. Aguarda `loki`, `grafana` e `alertmanager` via endpoints HTTP de readiness/health
3. Aguarda `promtail` pelo sinal de log `server listening on addresses`

**Uso:**
```bash
cd automation
./bootstrap_observability.sh
```

---

### `bootstrap_extractloadlivedata.sh`
Garante o build e a inicialização do serviço `extractloadlivedata`, com fallback automático quando o BuildKit falha.

**O que faz, em ordem:**
1. Tenta executar `docker compose build extractloadlivedata`
2. Se o build com BuildKit falhar, repete o build com `DOCKER_BUILDKIT=0`
3. Sobe o serviço com `docker compose up -d extractloadlivedata`

**Uso:**
```bash
cd automation
./bootstrap_extractloadlivedata.sh
```

---

### `bootstrap_metabase.sh`
Garante o bootstrap de infraestrutura **e** o provisionamento da aplicação Metabase, de forma idempotente, para que a plataforma suba do zero sem nenhum passo manual de UI.

**O que faz, em ordem:**
1. Aguarda o `postgres` ficar disponível
2. Executa o bootstrap SQL (`004_metabase.sql`): DB interno, usuário interno, usuário read-only e grants de `SELECT` no schema `refined`
3. Sobe o serviço `metabase` e aguarda o `GET /api/health`
4. **Provisiona a aplicação Metabase** (idempotente, via `curl` + `jq` contra `http://localhost:3001`):
   - cria o usuário admin e conclui o wizard (`POST /api/setup`), se ainda não configurado
   - define o timezone das consultas em duas camadas: default da sessão escopado ao role reader (`ALTER ROLE … IN DATABASE … SET timezone = 'America/Sao_Paulo'`, autoritativo para o SQL nativo) e o **Report Timezone** do Metabase
   - garante o datasource read-only `sptrans_insights` com escopo no schema `refined` (`host=postgres`)
   - dispara o sync de schema
   - aborta com código não-zero em qualquer resposta HTTP não-2xx; senhas nunca são logadas

**Variáveis de ambiente exigidas** (além das de DB/usuários do Metabase): `METABASE_ADMIN_EMAIL`, `METABASE_ADMIN_PASSWORD` (deve atender à política de senha do Metabase). Opcionais com default: `METABASE_ADMIN_FIRST_NAME` (`Admin`), `METABASE_ADMIN_LAST_NAME` (`User`), `METABASE_SITE_NAME` (`SPTrans Insights Pro`). Requer `curl` e `jq` no host.

**Uso:**
```bash
cd automation
./bootstrap_metabase.sh
```

---

### `promote_pipeline.py`
Promove uma pipeline do ambiente de desenvolvimento para produção. Requer obrigatoriamente uma das flags `--check` ou `--prod`.

**Flags**
- `--check`: executa apenas as validações (lint, SAST, testes, type checking). Sem sincronização.
- `--prod`: executa as validações e sincroniza para produção.

**O que faz, em ordem (ambas as flags):**
1. Verifica se a pasta da pipeline existe em `dags-dev/`
2. Executa lint com `ruff` na pasta da pipeline
3. Executa SAST com `bandit` (alta severidade) na pasta da pipeline
4. Executa os testes unitários (se a pasta `tests/` existir)
5. Executa type checking com `mypy` na pasta da pipeline

**Apenas com `--prod`:**
6. Sincroniza a pasta da pipeline para `airflow/dags/<pipeline>`, excluindo `__pycache__`, `.pytest_cache` e `tests/`
7. Sincroniza os módulos compartilhados `infra`, `quality`, `observability` e `pipeline_configurator`

**Uso:**
```bash
cd dags-dev
python3 ../automation/promote_pipeline.py <nome_da_pipeline> --check
python3 ../automation/promote_pipeline.py <nome_da_pipeline> --prod
```

**Exemplos:**
```bash
# Apenas validar
python3 ../automation/promote_pipeline.py transformlivedata --check
python3 ../automation/promote_pipeline.py gtfs --check

# Validar e promover para produção
python3 ../automation/promote_pipeline.py transformlivedata --prod
python3 ../automation/promote_pipeline.py gtfs --prod
```

---

### `deploy_service.py`
Realiza o build e redeploy de um microserviço Docker. Requer obrigatoriamente uma das flags `--check` ou `--prod`.

**Flags**
- `--check`: executa apenas as validações (lint, SAST, testes, type checking). Sem build ou deploy.
- `--prod`: executa as validações, build e deploy.

**O que faz, em ordem (ambas as flags):**
1. Verifica se a pasta do serviço existe
2. Executa lint com `ruff` na pasta do serviço
3. Executa SAST com `bandit` (alta severidade) na pasta do serviço
4. Executa os testes unitários (se a pasta `tests/` existir)
5. Executa type checking com `mypy` na pasta do serviço

**Apenas com `--prod`:**
6. Executa `docker compose build <serviço>`
7. Executa `docker compose up -d <serviço>`

**Uso:**
```bash
cd automation
python3 deploy_service.py <nome_no_docker_compose> <pasta_do_servico> --check
python3 deploy_service.py <nome_no_docker_compose> <pasta_do_servico> --prod
```

**Exemplos:**
```bash
# Apenas validar
python3 deploy_service.py extractloadlivedata extractloadlivedata --check

# Validar e fazer deploy
python3 deploy_service.py extractloadlivedata extractloadlivedata --prod
```

---

### `deploy_helpers.py`
Módulo auxiliar interno. Não é executado diretamente.

Expõe a função `run_code_validations(folder, label, step_offset, total_steps)` que executa lint, SAST e testes em sequência, retornando o número de steps consumidos. Utilizado por `promote_pipeline.py` e `deploy_service.py`.

Observação: quando existir `<folder>/.venv/bin/python`, este interpretador é utilizado automaticamente para `ruff`, `bandit`, `mypy` e `pytest`.

---

### `os_command_helper.py`
Módulo auxiliar interno. Não é executado diretamente.

Expõe a função `run_command(command, error_msg)` que executa subprocessos e interrompe a execução com mensagem de erro em caso de falha.

---

### `wait_helpers.sh`
Módulo auxiliar interno para evitar duplicação de lógica de espera por serviços.

Expõe funções reutilizadas pelos scripts de bootstrap:
- `wait_for_condition(label, timeout_seconds, interval_seconds, cmd...)`
- `check_http_url(url)`

## Fluxo típico de desenvolvimento

```
dags-dev/<pipeline>  →  promote_pipeline.py  →  airflow/dags/<pipeline>
```

1. Desenvolver e testar a pipeline em `dags-dev/<pipeline>/`
2. Garantir que `pytest <pipeline>/tests/` passa localmente
3. Executar `promote_pipeline.py <pipeline> --check` para validar
4. Executar `promote_pipeline.py <pipeline> --prod` para promover para produção

## Objetivo deste subprojeto
Automatizar as operações de implantação e promoção de código, garantindo que lint, SAST e testes unitários sejam executados antes de qualquer alteração em produção.

## O que este subprojeto faz
- valida a qualidade do código (lint via `ruff`, SAST via `bandit` e testes unitários via `pytest`) antes de qualquer operação
- promove uma pipeline do ambiente de desenvolvimento (`dags-dev`) para o ambiente de produção (`airflow/dags`), sincronizando também os módulos compartilhados `infra`, `quality` e `pipeline_configurator`
- realiza o build e o redeploy de um microserviço via `docker compose`

## Pré-requisitos
- Python 3.10+
- `ruff`, `bandit` e `pytest` instalados no ambiente Python utilizado para executar os scripts
- `rsync` instalado (para promoção de pipelines)
- Docker e Docker Compose instalados (para deploy de microserviços)
- Executar os scripts a partir da pasta `automation/` ou com o PATH correto para os módulos auxiliares

## Scripts disponíveis

### `platform_bootstrap_and_start.sh`
Sobe a plataforma com bootstrap prévio dos bancos de dados para evitar falhas de inicialização por ausência de schema, tabelas e índices obrigatórios.

**O que faz, em ordem:**
1. Sobe `airflow_postgres` e `postgres`
2. Aguarda ambos aceitarem conexões
3. Executa `bootstrap_airflow_postgres.sh`
4. Executa `bootstrap_postgres.sh`
5. Sobe o restante da plataforma com `docker compose up -d`

**Uso:**
```bash
cd automation
./platform_bootstrap_and_start.sh
```

---

### `promote_pipeline.py`
Promove uma pipeline do ambiente de desenvolvimento para produção.

**O que faz, em ordem:**
1. Verifica se a pasta da pipeline existe em `dags-dev/`
2. Executa lint com `ruff` na pasta da pipeline
3. Executa SAST com `bandit` (alta severidade) na pasta da pipeline
4. Executa os testes unitários (se a pasta `tests/` existir)
5. Sincroniza a pasta da pipeline para `airflow/dags/<pipeline>` excluindo `__pycache__`, `.pytest_cache` e `tests/`
6. Sincroniza os módulos compartilhados `infra`, `quality` e `pipeline_configurator`

**Uso:**
```bash
cd dags-dev
python3 ../automation/promote_pipeline.py <nome_da_pipeline>
```

**Exemplos:**
```bash
python3 ../automation/promote_pipeline.py transformlivedata
python3 ../automation/promote_pipeline.py gtfs
python3 ../automation/promote_pipeline.py updatelatestpositions
```

---

### `deploy_service.py`
Realiza o build e redeploy de um microserviço Docker.

**O que faz, em ordem:**
1. Verifica se a pasta do serviço existe
2. Executa lint com `ruff` na pasta do serviço
3. Executa SAST com `bandit` (alta severidade) na pasta do serviço
4. Executa os testes unitários (se a pasta `tests/` existir)
5. Executa `docker compose build <serviço>`
6. Executa `docker compose up -d <serviço>`

**Uso:**
```bash
cd automation
python3 deploy_service.py <nome_no_docker_compose> <pasta_do_servico>
```

**Exemplos:**
```bash
python3 deploy_service.py extractloadlivedata extractloadlivedata
python3 deploy_service.py alertservice alertservice
```

---

### `deploy_helpers.py`
Módulo auxiliar interno. Não é executado diretamente.

Expõe a função `run_code_validations(folder, label, step_offset)` que executa lint, SAST e testes em sequência, retornando o número de steps consumidos. Utilizado por `promote_pipeline.py` e `deploy_service.py`.

Observação: quando existir `<folder>/.venv/bin/python`, este interpretador é utilizado automaticamente para `ruff`, `bandit` e `pytest`.

---

### `os_command_helper.py`
Módulo auxiliar interno. Não é executado diretamente.

Expõe a função `run_command(command, error_msg)` que executa subprocessos e interrompe a execução com mensagem de erro em caso de falha.

## Fluxo típico de desenvolvimento

```
dags-dev/<pipeline>  →  promote_pipeline.py  →  airflow/dags/<pipeline>
```

1. Desenvolver e testar a pipeline em `dags-dev/<pipeline>/`
2. Garantir que `pytest <pipeline>/tests/` passa localmente
3. Executar `promote_pipeline.py <pipeline>` para promover para produção
4. O script valida, sincroniza e atualiza os módulos compartilhados automaticamente

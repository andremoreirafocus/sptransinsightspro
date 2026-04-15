## Objetivo deste subprojeto
Automatizar as operações de implantação e promoção de código, garantindo que lint e testes unitários sejam executados antes de qualquer alteração em produção.

## O que este subprojeto faz
- valida a qualidade do código (lint via `ruff` e testes unitários via `pytest`) antes de qualquer operação
- promove uma pipeline do ambiente de desenvolvimento (`dags-dev`) para o ambiente de produção (`airflow/dags`), sincronizando também os módulos compartilhados `infra` e `pipeline_configurator`
- realiza o build e o redeploy de um microserviço via `docker compose`

## Pré-requisitos
- Python 3.10+
- `ruff` instalado e acessível no PATH
- `rsync` instalado (para promoção de pipelines)
- Docker e Docker Compose instalados (para deploy de microserviços)
- Executar os scripts a partir da pasta `scripts/` ou com o PATH correto para os módulos auxiliares

## Scripts disponíveis

### `promote_pipeline.py`
Promove uma pipeline do ambiente de desenvolvimento para produção.

**O que faz, em ordem:**
1. Verifica se a pasta da pipeline existe em `dags-dev/`
2. Executa lint com `ruff` na pasta da pipeline
3. Executa os testes unitários (se a pasta `tests/` existir)
4. Sincroniza a pasta da pipeline para `airflow/dags/<pipeline>` excluindo `__pycache__`, `.pytest_cache` e `tests/`
5. Sincroniza os módulos compartilhados `infra` e `pipeline_configurator`

**Uso:**
```bash
cd dags-dev
python3 ../scripts/promote_pipeline.py <nome_da_pipeline>
```

**Exemplos:**
```bash
python3 ../scripts/promote_pipeline.py transformlivedata
python3 ../scripts/promote_pipeline.py gtfs
python3 ../scripts/promote_pipeline.py updatelatestpositions
```

---

### `deploy_service.py`
Realiza o build e redeploy de um microserviço Docker.

**O que faz, em ordem:**
1. Verifica se a pasta do serviço existe
2. Executa lint com `ruff` na pasta do serviço
3. Executa os testes unitários (se a pasta `tests/` existir)
4. Executa `docker compose build <serviço>`
5. Executa `docker compose up -d <serviço>`

**Uso:**
```bash
cd scripts
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

Expõe a função `run_code_validations(folder, label, step_offset)` que executa lint e testes em sequência, retornando o número de steps consumidos. Utilizado por `promote_pipeline.py` e `deploy_service.py`.

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

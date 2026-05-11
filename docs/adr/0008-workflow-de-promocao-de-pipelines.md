# ADR-0008: Workflow de promoção de pipelines (dags-dev → airflow/dags)

**Data:** 2026-04-15  
**Status:** Aceito

## Contexto

Os pipelines são desenvolvidos como código Python executável diretamente (sem Airflow) no diretório `dags-dev/`. O ambiente de produção é o Airflow, cujo `dags_folder` aponta para `airflow/dags/`.

Duas abordagens óbvias para manter esses dois ambientes sincronizados:

1. **Desenvolver diretamente em `airflow/dags/`**: sem fricção de sincronização, mas o código de produção fica constantemente exposto a mudanças em progresso — qualquer arquivo salvo é imediatamente carregado pelo Airflow scheduler, podendo causar erros em runtime.

2. **Symlinks ou volume compartilhado**: `airflow/dags/` aponta para `dags-dev/` via symlink ou Docker volume. Remove a necessidade de sincronização, mas elimina a separação entre dev e prod: arquivos de teste, fixtures e código incompleto ficam visíveis ao Airflow.

Ambas as abordagens eliminam o conceito de um "gateway de qualidade" antes de o código chegar à produção.

## Decisão

Adotar um **workflow de promoção explícita** com gate de qualidade automatizado:

- Todo desenvolvimento e teste ocorre em `dags-dev/`.
- A promoção para produção é feita exclusivamente via `automation/promote_pipeline.py <pipeline>`.
- O script executa automaticamente, em ordem:
  1. Lint com `ruff check` (falha se houver erros).
  2. Testes unitários com `pytest tests/` (falha se houver falhas, pulado se não houver `tests/`).
  3. `rsync` do subdiretório da pipeline para `airflow/dags/` (excluindo `tests/`, `__pycache__`, `.pytest_cache`).
  4. `rsync` dos módulos compartilhados (`infra/`, `pipeline_configurator/`) para produção.

O contador de steps exibido no output é ajustado automaticamente conforme a presença de testes, fornecendo feedback claro sobre o que foi executado.

## Alternativas consideradas

**Desenvolvimento direto em `airflow/dags/`:** Minimiza fricção, mas expõe produção a código em progresso. O Airflow carrega DAGs a cada `dag_file_processor_timeout` segundos — um erro de sintaxe causaria falha imediata no scheduler, visível a todos os usuários.

**Symlinks / volume compartilhado Docker:** Solução elegante que elimina o problema de sincronização, mas elimina também a separação dev/prod. Arquivos de teste (`conftest.py`, fixtures, `tests/`) seriam carregados pelo Airflow como potenciais DAGs, gerando warnings ou erros de parsing. A separação de ambientes é comprometida.

**CI/CD externo (GitHub Actions):** A solução correta para times e produção real — PRs disparam lint + test, merges a `main` disparam promoção automática. Para um projeto de desenvolvimento solo sem pipeline de CI configurado, o script local é o equivalente funcional com menor overhead de setup.

## Consequências

**Positivo:**
- Nenhum código sem lint e sem testes aprovados chega à produção: o gate é estrutural, não opcional.
- O ambiente de produção (`airflow/dags/`) contém apenas código de produção — sem arquivos de teste, fixtures ou stubs.
- A promoção é auditável: `rsync` com `--checksum` garante que apenas arquivos alterados são transferidos, e o output do script documenta cada passo executado.
- Módulos compartilhados (`infra/`, `pipeline_configurator/`) são sempre sincronizados junto com a pipeline promovida, evitando drift entre versões.

**Negativo / Tradeoffs:**
- A promoção é um passo manual: depende do desenvolvedor lembrar de executar o script. Sem CI/CD, não há garantia de que o código em `airflow/dags/` reflete o estado mais recente aprovado em `dags-dev/`.
- Risco de drift: se alterações forem feitas diretamente em `airflow/dags/` (ex: hotfix de emergência), elas não existirão em `dags-dev/` e serão sobrescritas na próxima promoção.
- A ausência de versionamento explícito dos artefatos promovidos dificulta rollbacks: reverter para uma versão anterior de produção exige `git checkout` manual do estado de `airflow/dags/`.

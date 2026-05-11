# ADR-0009: Design do alertservice (webhook, alertas cumulativos, SQLite)

**Data:** 2026-04-15  
**Status:** Aceito

## Contexto

O [ADR-0005](./0005-framework-de-qualidade-de-dados.md) estabelece que cada execução do `transformlivedata` gera um relatório de qualidade com status `PASS`, `WARN` ou `FAIL` e que esse relatório deve disparar notificações para permitir monitoramento proativo. O serviço responsável por receber esses relatórios e decidir se e como notificar precisa resolver três questões independentes:

1. **Como o pipeline entrega o relatório ao serviço de alertas** — push (webhook) ou pull (polling)?
2. **Quando um WARN deve gerar notificação** — imediatamente ou apenas quando acumulados?
3. **Onde persistir o histórico de eventos** — qual banco de dados?

## Decisão

### 1. Push via webhook (não polling)

O `transformlivedata` envia o `summary` do relatório de qualidade via `POST /notify` ao `alertservice` ao final de cada execução. O `alertservice` é um microserviço FastAPI independente, sem acesso direto ao MinIO ou ao Airflow.

A alternativa — o `alertservice` fazer polling periódico nos relatórios armazenados no bucket `metadata` do MinIO — foi descartada porque introduz latência variável (o serviço só detectaria um FAIL no próximo ciclo de polling), acoplamento ao MinIO, e lógica de controle de estado para rastrear quais relatórios já foram processados. Com webhook, a notificação é processada no momento em que o pipeline termina, sem estado adicional a gerenciar no lado do alertservice.

### 2. FastAPI como framework

O `alertservice` expõe um único endpoint (`POST /notify`). FastAPI foi escolhido por:
- Validação automática do payload de entrada via Pydantic (`NotificationPayload`), com retorno de 422 para payloads malformados sem código de validação manual.
- Startup hook (`@app.on_event("startup")`) para inicialização do banco e carregamento da configuração de pipelines uma única vez — sem recarregamento a cada request.
- Leveza: sem overhead de frameworks mais pesados (Django, Flask + extensões).

### 3. Alertas cumulativos para WARNs

FAILs geram notificação imediata. WARNs, porém, são esperados ocasionalmente — um único WARN em 24 horas pode ser ruído normal. Notificar a cada WARN individual geraria fadiga de alertas, levando à ignorância sistemática das notificações.

A decisão foi avaliar WARNs de forma cumulativa dentro de uma janela de tempo configurável por pipeline (`warning_window`). Um e-mail é enviado apenas se qualquer um dos thresholds configurados for excedido na janela:

- `max_failed_rows`: total de registros inválidos acumulados na janela supera o limite absoluto.
- `max_failed_ratio`: média da taxa de falha na janela supera o limite percentual.
- `max_consecutive_warn`: número de WARNs consecutivos (sem PASS intercalado) atinge o limite.

Os thresholds e a janela são definidos em `config/pipelines.yaml` por pipeline, permitindo calibração independente por contexto sem alterar código.

### 4. SQLite para persistência de eventos

O histórico de execuções (necessário para avaliação cumulativa de WARNs) é armazenado em SQLite local (`storage/alerts.db`).

PostgreSQL já existe no projeto mas foi descartado para este propósito por dois motivos:
- **Isolamento de falhas**: se o PostgreSQL estiver indisponível, o `alertservice` continuaria recebendo e avaliando notificações normalmente. Usar o mesmo banco de dados das pipelines tornaria o serviço de alertas indisponível exatamente nos momentos em que a infraestrutura está com problemas — quando as notificações são mais críticas.
- **Simplicidade operacional**: o `alertservice` é um microserviço com volume baixo de escrita (uma linha por execução do pipeline). SQLite é suficiente, sem necessidade de gerenciar conexões de pool, schema migrations ou dependências de rede.

## Consequências

**Positivo:**
- Notificações de FAIL chegam imediatamente ao final da execução do pipeline, sem latência de polling.
- WARNs individuais não geram ruído; apenas padrões de degradação sustentada disparam alertas.
- Os thresholds de WARN são configuráveis por pipeline sem redeployment.
- O `alertservice` permanece operacional mesmo se o PostgreSQL estiver indisponível — isolamento correto para um serviço de monitoramento.
- O histórico em SQLite é localmente consultável para debugging sem dependência de outros serviços.

**Negativo / Tradeoffs:**
- O webhook introduz acoplamento de runtime: se o `alertservice` estiver indisponível quando o `transformlivedata` terminar, o relatório não é entregue e nenhuma notificação é disparada (embora o pipeline em si continue). Não há retry automático no lado do remetente.
- SQLite não escala para múltiplas instâncias do `alertservice` (sem suporte a escritas concorrentes de processos distintos). Para horizontalização do serviço, o storage precisaria ser migrado para PostgreSQL ou outro banco centralizado.
- O histórico de eventos cresce indefinidamente sem política de retenção implementada — limpeza periódica precisaria ser adicionada manualmente.

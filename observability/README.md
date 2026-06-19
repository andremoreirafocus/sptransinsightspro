# Observabilidade

Este diretório contém a configuração da stack centralizada de observabilidade de logs da plataforma, baseada em **Grafana + Loki + Promtail + Alertmanager**.

A observabilidade é tratada como capacidade transversal da plataforma e não apenas como monitoramento de um serviço isolado. A estratégia do projeto cobre três dimensões complementares: estruturação de logs, rastreamento de linhagem de dados e instrumentação de métricas de execução.

A estratégia parte de logs estruturados em JSON como contrato padrão entre componentes. Esse formato torna os eventos "machine readble", facilitando parsing e consultas automatizadas, e aumenta a consistência das análises operacionais entre serviços e pipelines.

Para rastreabilidade ponta a ponta, o projeto utiliza `correlation_id` baseado em `logical_datetime` (timestamp do dado processado ao longo das pipelines) e `execution_id` (correlação da execução). Essa combinação permite auditoria entre etapas, diagnóstico mais rápido de falhas e monitoramento operacional consistente da saúde dos fluxos.

Fluxo de informação entre pipelines monitorado pela observabilidade:

`extractloadlivedata` → `transformlivedata` → `refinedfinishedtrips` 

Como referência de instrumentação de execução e métricas, o `extractloadlivedata` registra métricas por fase (`extract`, `save`, `notify`) com tentativas, sucessos, falhas e duração, além do evento final `execution_metrics_final`, estruturado para consultas em Prometheus/AlertManager e visibilidade operacional de cada execução.

## Contrato de Execution Report

Para alertas e monitoramento de resultado de execução, os eventos finais de relatório são:
- `execution_completed`
- `execution_failed_non_recoverable`

Semântica de correlação:
- `execution_id`: identificador primário de correlação no nível da execução (chave principal para alertas e monitoramento de resultado).
- `correlation_id`: identificador de escopo de requisição/dado (item individual processado); não é obrigatório em eventos finais de resultado da execução.

Campos obrigatórios no `metadata` de execution report:
- `execution_seconds`
- `items_total`
- `items_failed`
- `retries_seen`
- `correlation_ids` (lista de correlações trabalhadas na execução, ordenada e sem duplicidades)
- `correlation_ids_count` (quantidade total de correlações únicas trabalhadas)

Significado dos campos de correlação no execution report:
- `correlation_ids`: subconjunto ordenado (e eventualmente truncado) das correlações únicas processadas na execução, útil para diagnóstico rápido e amostragem operacional.
- `correlation_ids_count`: quantidade total de correlações únicas processadas na execução, incluindo itens que não aparecem em `correlation_ids` quando houver truncamento.

## Stack

| Componente | Papel |
|---|---|
| **Loki** | Backend de agregação de logs. Recebe streams de logs estruturados e os armazena indexados por labels. |
| **Promtail** | Agente de envio de logs. Coleta logs dos containers via Docker socket e os encaminha ao Loki. |
| **Grafana** | Camada de visualização. Consulta o Loki via LogQL e renderiza dashboards. |
| **Alertmanager** | Gerenciador de alertas. Recebe alertas de regras (ex.: Loki Ruler) e aplica roteamento, agrupamento e deduplicação. |

Os três serviços estão definidos no `docker-compose.yml` raiz e compartilham a rede `rede_fia`.

## Alertas por E-mail

O Loki Ruler avalia as regras e envia alertas para o Alertmanager, que aplica roteamento por severidade e envia notificações por e-mail.

Regras atualmente configuradas para `extractloadlivedata`:
- `ServiceFailed` (`severity=critical`): dispara quando há `execution_failed_non_recoverable`.
- `ServiceWarningThreshold` (`severity=warning`): dispara quando a execução finaliza com `retries_seen > 0`.

Regras atualmente configuradas para `transformlivedata`:
- `PipelinePhaseFailed` (`severity=critical`): dispara quando qualquer fase do pipeline emite um evento de falha.
- `AcceptanceRateBelowThreshold` (`severity=warning`): dispara quando a taxa de aceitação dos registros cai abaixo de 98%.
- `NoPipelineExecutionCompleted` (`severity=critical`): dispara quando nenhum `execution_finished` é detectado em 30 minutos.

Regras atualmente configuradas para `refinedfinishedtrips`:
- `ExecutionAborted` (`severity=critical`): dispara quando há `execution_aborted` — a execução foi interrompida (cobre falhas de freshness e gaps que atingem o limiar de falha).
- `NoPipelineExecutionCompleted` (`severity=critical`): dispara quando nenhum `execution_finished` é detectado em 1 hora.
- `PositionFreshnessHigh` (`severity=warning`): dispara quando `observed_lag_minutes` excede o limiar de aviso de 10 minutos — o pipeline continua, mas a qualidade dos dados pode estar degradada.
- `ExtractionGapHigh` (`severity=warning`): dispara quando `max_gap_minutes` excede o limiar de aviso de 5 minutos — sinal antecipado antes que o pipeline comece a ser interrompido.

Regras atualmente configuradas para `refinedtripfacts`:
- `ExecutionAborted` (`severity=critical`): dispara quando há `execution_aborted`.
- `NoPipelineExecutionCompleted` (`severity=critical`): dispara quando nenhum `execution_finished` é detectado em 1 hora.
- `CompletenessLossRateHigh` (`severity=warning`): dispara quando `loss_rate` excede 1% nos últimos 10 minutos.
- `DriftDetected` (`severity=warning`): dispara quando `drift_detected=true` no evento `quality_report_metrics`.

Configuração de e-mail usada pelo Alertmanager:
- `ALERTMANAGER_SMTP_HOST`
- `ALERTMANAGER_SMTP_PORT`
- `ALERTMANAGER_SMTP_USER`
- `ALERTMANAGER_SMTP_PASSWORD`
- `ALERTMANAGER_EMAIL_FROM`
- `ALERTMANAGER_EMAIL_TO`

## Arquitetura

```
extractloadlivedata (logs JSON em stdout)
  → Docker runtime
    → Promtail (coleta via Docker socket)
      → Loki (armazenamento indexado por labels)
        → Grafana (consultas LogQL + dashboards)
```

A aplicação emite logs estruturados em JSON para `stdout`. O Promtail os coleta externamente via Docker socket — a aplicação não tem conhecimento da camada de transporte. Isso mantém o contrato de logging estável e o backend substituível por ambiente.

## Contrato de Logs

Cada linha de log é um objeto JSON. Campos obrigatórios:

| Campo | Descrição |
|---|---|
| `timestamp` | Timestamp UTC (ISO 8601) |
| `level` | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` |
| `service` | Nome do serviço (ex.: `extractloadlivedata`) |
| `component` | Módulo ou classe que emite o log |
| `event` | Nome de evento estável em snake_case (ex.: `execution_metrics_final`) |
| `message` | Descrição legível por humanos |

Campos recomendados: `execution_id`, `correlation_id`, `status`, `metadata`.

## Labels do Loki

O Promtail indexa os seguintes labels para seleção de streams em LogQL:

| Label | Valor |
|---|---|
| `service` | `extractloadlivedata` |
| `container` | Nome do container |
| `source` | `docker` |

Todos os demais campos (ex.: `event`, `level`, `execution_id`) são extraídos no momento da consulta via `| json`.

## Dashboards

Os dashboards são provisionados automaticamente a partir de `grafana/provisioning/dashboards/`. Nenhuma importação manual é necessária.

| Dashboard | Arquivo | Descrição |
|---|---|---|
| extractloadlivedata | `extractloadlivedata.json` | Execuções, erros, warnings, tempo de execução por fase, stream de logs |
| transformlivedata | `transformlivedata.json` | Execuções, duração por fase, taxa de aceitação, volume de registros brutos, registros rejeitados por motivo, stream de logs |
| refinedfinishedtrips | `refinedfinishedtrips.json` | Execuções, duração por fase, viagens adicionadas, viagens detectadas, posições carregadas, qualidade da extração, freshness dos dados de posição, stream de logs |
| refinedtripfacts | `refinedtripfacts.json` | Execuções, duração por fase, volume e taxas de perda, violações de domínio, cobertura de dim_time, stream de logs |

Os screenshots dos dashboards estão nos READMEs de cada pipeline, junto com o inventário completo de painéis e queries Loki:

- [extractloadlivedata](../extractloadlivedata/README.md#dashboard-grafana) — erros e warnings por execução, tempo de execução por fase
- [transformlivedata](../dags-dev/transformlivedata/README.md#dashboard-grafana) — execuções, duração por fase, taxa de aceitação, volume de registros
- [refinedfinishedtrips](../dags-dev/refinedfinishedtrips/README.md#dashboard-grafana) — execuções, volume de viagens, qualidade da extração, freshness dos dados de posição
- [gtfs](../dags-dev/gtfs/README.md#dashboard-grafana) — execuções, duração por fase, linhas do trip_details, arquivos extraídos
- [refinedtripfacts](../dags-dev/refinedtripfacts/README.md#observabilidade-stack-loki--grafana) — execuções, volume e taxas de perda, violações de domínio, cobertura de dim_time

Após editar um JSON de dashboard, incremente o campo `version` e recarregue sem reiniciar o Grafana:

```bash
curl -X POST http://admin:<senha>@localhost:3000/api/admin/provisioning/dashboards/reload
```

## Estrutura de Diretórios

```
observability/
  loki/
    loki-config.yml          # Configuração do Loki (armazenamento em filesystem, nó único)
  alertmanager/
    alertmanager.yml.tmpl    # Template de configuração do Alertmanager (roteamento e receivers) — credenciais injetadas na inicialização via variáveis de ambiente
    Dockerfile               # Imagem customizada com entrypoint baseado em sed
    entrypoint.sh            # Renderiza o template na inicialização do container
  promtail/
    promtail-config.yml      # Configuração do Promtail (Docker socket, filtro extractloadlivedata)
  grafana/
    provisioning/
      datasources/
        loki.yml             # Datasource Loki provisionado automaticamente
      dashboards/
        dashboards.yml               # Configuração do provider de dashboards
        extractloadlivedata.json     # Dashboard do extractloadlivedata
        transformlivedata.json       # Dashboard do transformlivedata
        refinedfinishedtrips.json    # Dashboard do refinedfinishedtrips
        refinedtripfacts.json        # Dashboard do refinedtripfacts
  loki/
    rules/
      fake/
        refinedtripfacts-alerts.yaml  # Regras de alerta do refinedtripfacts
```

## URLs Locais

| Serviço | URL |
|---|---|
| Grafana | http://localhost:3000 |
| Alertmanager | http://localhost:9093 |

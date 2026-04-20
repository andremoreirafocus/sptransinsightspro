## Objetivo deste subprojeto
Receber resumos de qualidade de pipelines via webhook e enviar notificações por e-mail, incluindo alertas imediatos para falhas e alertas cumulativos para warnings.

## O que este subprojeto faz
- expõe um endpoint HTTP `POST /notify` para receber `summary` de qualidade
- registra todos os eventos em SQLite para análise de histórico
- envia e-mail imediato quando `status=FAIL`
- envia e-mail de warning apenas quando limiares cumulativos são atingidos
- registra logs em arquivo rotativo e stdout

## Pré-requisitos
- Serviço SMTP configurado e acessível
- Arquivo `.env` com as credenciais SMTP
- Arquivo de configuração por pipeline em `alertservice/config/pipelines.yaml`

## Configurações
### Endpoint
`POST /notify`

Payload esperado (exemplo):
```json
{
  "summary": {
    "pipeline": "transformlivedata",
    "execution_id": "...",
    "status": "WARN",
    "acceptance_rate": 0.99,
    "items_failed": 9,
    "failure_phase": null,
    "failure_message": null,
    "generated_at_utc": "2026-04-13T12:34:56Z"
  }
}
```

### Configuração por pipeline
Arquivo: `alertservice/config/pipelines.yaml`

Todos os campos abaixo são obrigatórios. O serviço falha no startup se algum estiver ausente ou inválido.

Exemplo:
```yaml
transformlivedata:
  warning_window:
    type: "time"
    value: 24
  warning_thresholds:
    max_failed_items: 500
    max_failed_ratio: 0.02
    max_consecutive_warn: 3
  notify_on_fail: true
  notify_on_warn: true
```

### Variáveis de ambiente (SMTP)
Todas as variáveis são obrigatórias. Um template está disponível em `.env.example`.

O assunto do e-mail é montado dinamicamente: `{EMAIL_SUBJECT_PREFIX} {pipeline}: {status}`

Exemplo: `[DQ] status update for pipeline transformlivedata: FAIL`

Observação: todos os campos são obrigatórios. Se qualquer um estiver ausente, o serviço falha no startup.

## Testes unitários
Ainda não implementados.

## Para instalar os requisitos
- cd alertservice
- python3 -m venv .venv
- source .venv/bin/activate
- pip install -r requirements.txt

## Para executar
### Execução manual (sem Docker)
Crie `alertservice/.env` com base em `.env.example` preenchendo todos os campos:

```
SMTP_HOST=
SMTP_PORT=
SMTP_USER=
SMTP_PASSWORD=
SMTP_USE_TLS=
EMAIL_FROM=
EMAIL_TO=
EMAIL_SUBJECT_PREFIX=
```

```bash
cd alertservice
uvicorn src.app:app --host 0.0.0.0 --port 8000
```

### Docker (via docker-compose)
As variáveis de configuração não-sensíveis (`SMTP_HOST`, `SMTP_PORT`, etc.) já estão declaradas em `docker-compose.yml`.

As credenciais SMTP são injetadas via variáveis de substituição. Adicione ao `.env` na raiz do projeto:

```
ALERTSERVICE_SMTP_USER=
ALERTSERVICE_SMTP_PASSWORD=
```

```bash
docker-compose up alertservice
```

## Observações
- Eventos são armazenados em SQLite (`alertservice/storage/alerts.db`).
- `FAIL` envia alerta imediato.
- `WARN` envia alerta apenas se regras cumulativas forem atingidas.
- Logs são gravados em `alertservice.log` (rotacionado) e também enviados ao stdout.

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
    "rows_failed": 9,
    "failure_phase": null,
    "failure_message": null,
    "generated_at_utc": "2026-04-13T12:34:56Z"
  }
}
```

### Configuração por pipeline
Arquivo: `alertservice/config/pipelines.yaml`

Exemplo:
```yaml
transformlivedata:
  warning_window:
    type: "time"
    value: 24
  warning_thresholds:
    max_failed_rows: 500
    max_failed_ratio: 0.02
    max_consecutive_warn: 3
  notify_on_fail: true
  notify_on_warn: true
```

### Variáveis de ambiente (SMTP)
```
SMTP_HOST=
SMTP_PORT=25
SMTP_USER=
SMTP_PASSWORD=
SMTP_USE_TLS=false
EMAIL_FROM=
EMAIL_TO=
EMAIL_SUBJECT_PREFIX=[DQ]
```

Observação: `SMTP_HOST`, `EMAIL_FROM` e `EMAIL_TO` são obrigatórios.  
Se estiverem ausentes, o serviço falha no startup.

## Testes unitários
Ainda não implementados.

## Para instalar os requisitos
- cd alertservice
- python3 -m venv .venv
- source .venv/bin/activate
- pip install -r requirements.txt

## Para executar
### Execução manual (sem Docker)
```bash
uvicorn src.app:app --host 0.0.0.0 --port 8000
```

### Docker
```bash
docker build -t alertservice ./alertservice
docker run -p 8000:8000 \
  -e SMTP_HOST=... \
  -e EMAIL_FROM=... \
  -e EMAIL_TO=... \
  alertservice
```

## Observações
- Eventos são armazenados em SQLite (`alertservice/storage/alerts.db`).
- `FAIL` envia alerta imediato.
- `WARN` envia alerta apenas se regras cumulativas forem atingidas.
- Logs são gravados em `alertservice.log` (rotacionado) e também enviados ao stdout.

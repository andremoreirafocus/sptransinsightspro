## Alert Service

Serviço mínimo para receber resumos de qualidade via webhook e enviar notificações por e-mail.

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
- `SMTP_HOST`
- `SMTP_PORT` (default: 25)
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_USE_TLS` (true/false)
- `EMAIL_FROM`
- `EMAIL_TO` (lista separada por vírgula)
- `EMAIL_SUBJECT_PREFIX` (opcional)

### Como executar
#### Docker
```bash
docker build -t alertservice ./alertservice
docker run -p 8000:8000 \
  -e SMTP_HOST=... \
  -e EMAIL_FROM=... \
  -e EMAIL_TO=... \
  alertservice
```

#### Execução manual (sem Docker)
```bash
cd alertservice
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

### Observações
- Eventos são armazenados em SQLite (`alertservice/storage/alerts.db`).
- `FAIL` envia alerta imediato.
- `WARN` envia alerta apenas se regras cumulativas forem atingidas.

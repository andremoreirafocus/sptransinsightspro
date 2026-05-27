#!/bin/sh
sed \
  -e "s|\${ALERTMANAGER_SMTP_HOST}|${ALERTMANAGER_SMTP_HOST}|g" \
  -e "s|\${ALERTMANAGER_SMTP_PORT}|${ALERTMANAGER_SMTP_PORT}|g" \
  -e "s|\${ALERTMANAGER_SMTP_USER}|${ALERTMANAGER_SMTP_USER}|g" \
  -e "s|\${ALERTMANAGER_SMTP_PASSWORD}|${ALERTMANAGER_SMTP_PASSWORD}|g" \
  -e "s|\${ALERTMANAGER_SMTP_USE_TLS}|${ALERTMANAGER_SMTP_USE_TLS}|g" \
  -e "s|\${ALERTMANAGER_EMAIL_FROM}|${ALERTMANAGER_EMAIL_FROM}|g" \
  -e "s|\${ALERTMANAGER_EMAIL_TO}|${ALERTMANAGER_EMAIL_TO}|g" \
  /etc/alertmanager/alertmanager.yml.tmpl > /tmp/alertmanager.yml
exec /bin/alertmanager --config.file=/tmp/alertmanager.yml "$@"

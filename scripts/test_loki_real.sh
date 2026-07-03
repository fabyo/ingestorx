PAYLOAD=$(cat << 'EOF'
{
  "streams": [
    {
      "stream": {
        "job": "ingestorx",
        "level": "INFO",
        "filename": "/var/log/ingestorx/xml_watcher.log.test"
      },
      "values": [
        [ "1782153665779294264", "{\"timestamp\": \"2026-06-22T19:03:15.876610Z\", \"level\": \"INFO\", \"fields\": {\"message\": \"telemetria do watcher ativa\", \"tenant_id\": \"tenant_default\"}}" ]
      ]
    }
  ]
}
EOF
)

curl -s -v -H "Content-Type: application/json" -XPOST -s "http://localhost:3100/loki/api/v1/push" --data-raw "$PAYLOAD"

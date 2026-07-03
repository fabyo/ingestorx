curl -s -G 'http://localhost:3100/loki/api/v1/query' --data-urlencode 'query=count_over_time({filename="/var/log/ingestorx/xml_watcher.log.2026-06-22"}[1h])'

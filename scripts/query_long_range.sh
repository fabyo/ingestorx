start_time=$(date -d "12 hours ago" +%s%N)
curl -s --data-urlencode 'query={job="ingestorx"}' -G "http://localhost:3100/loki/api/v1/query_range" --data-urlencode "start=${start_time}" | grep -o 'telemetria' | wc -l

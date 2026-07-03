# Capacidade e critérios de produção

O IngestorX não declara uma capacidade universal. A aprovação de uma versão
deve registrar tamanho médio/máximo, taxa sustentada, pico, hardware,
filesystem e latência do broker.

## Gate de capacidade

1. Gerar uma carga equivalente a pelo menos 2 vezes o pico contratado.
2. Manter a carga por 60 minutos e executar uma campanha de 24 horas.
3. Interromper RabbitMQ por 10 minutos e confirmar drenagem integral.
4. Reiniciar watcher e consumidor durante a carga.
5. Comparar hashes e contagens de entrada, processados, erro e DLQ.
6. Exigir perda igual a zero e documentar duplicatas reconciliadas.
7. Registrar p50, p95 e p99 da latência e uso máximo de CPU, RAM, disco e I/O.

Use `just load-test 1000000` apenas em volume dedicado com espaço e inodes
suficientes. Comece com 10 mil e aumente progressivamente. Um milhão de
arquivos em um único diretório não representa o layout recomendado para
produção; particione a entrada por data/tenant.

## Semântica

A entrega é `at-least-once`. `tenant_id + hash_sha256` é a chave idempotente
no PostgreSQL. Mensagens rejeitadas ficam em `<fila>.dlq`. XMLs brutos ficam
no object storage sob `tenant/ano/mes/hash.xml`; o banco guarda metadados e
auditoria. O MinIO standalone do Compose é para desenvolvimento. Produção
exige S3 gerenciado ou MinIO distribuído com erasure coding e replicação.

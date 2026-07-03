# Runbook operacional

## Saúde

- `/health`: processo HTTP vivo.
- `/ready`: RabbitMQ alcançável; retorna 503 caso contrário.
- `/metrics`: claims, processamentos ativos e contingência local.

## Recuperação

1. Não apague `dados/processando`, recibos ou contingência.
2. Confirme RabbitMQ, PostgreSQL e object storage; corrija a dependência
   indisponível e reinicie os serviços.
3. O watcher reconcilia órfãos e drena contingência; acompanhe métricas.
4. Inspecione `<XML_WATCHER_RABBITMQ_FILA>.dlq` antes de reprocessar.
5. Compare o hash armazenado antes de republicar qualquer mensagem.

## Backup e restauração

`just backup-postgres` cria um dump comprimido em `backups/`. Copie-o para
armazenamento externo criptografado e aplique política de retenção. Teste a
restauração regularmente em instância isolada:

```bash
gzip -dc backups/ARQUIVO.sql.gz | \
  docker compose exec -T postgres psql -U ingestorx -d ingestorx
```

O backup do banco não inclui XMLs no MinIO, RabbitMQ nem `dados/`. Eles exigem
backup consistente separado. Habilite versionamento/replicação do bucket e
defina RPO/RTO antes da entrada em produção.

## Segurança

Os secrets locais servem apenas para desenvolvimento. Produção exige secret
manager, rotação, TLS com CA corporativa, controle de acesso, logs imutáveis,
varredura de imagens/dependências e revisão de LGPD. Este projeto não implica
certificação bancária.

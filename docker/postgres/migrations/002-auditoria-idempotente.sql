CREATE UNIQUE INDEX IF NOT EXISTS auditoria_evento_idempotente_uq
    ON auditoria_arquivos (tenant_id, hash_sha256, evento);

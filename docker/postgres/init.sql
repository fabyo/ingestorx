CREATE TABLE IF NOT EXISTS arquivos (
    tenant_id text NOT NULL,
    hash_sha256 char(64) NOT NULL,
    correlation_id text NOT NULL,
    nome_original text NOT NULL,
    object_key text,
    tamanho_bytes bigint NOT NULL CHECK (tamanho_bytes >= 0),
    status text NOT NULL CHECK (status IN
        ('recebido', 'publicado', 'processando', 'processado', 'erro')),
    detectado_em timestamptz NOT NULL,
    processado_em timestamptz,
    erro text,
    tentativas integer NOT NULL DEFAULT 0,
    atualizado_em timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, hash_sha256)
);

CREATE UNIQUE INDEX IF NOT EXISTS arquivos_correlation_id_uq
    ON arquivos (correlation_id);
CREATE INDEX IF NOT EXISTS arquivos_status_atualizado_idx
    ON arquivos (status, atualizado_em);

CREATE TABLE IF NOT EXISTS auditoria_arquivos (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id text NOT NULL,
    hash_sha256 char(64) NOT NULL,
    evento text NOT NULL,
    detalhes jsonb NOT NULL DEFAULT '{}'::jsonb,
    criado_em timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS auditoria_arquivo_idx
    ON auditoria_arquivos (tenant_id, hash_sha256, criado_em);
CREATE UNIQUE INDEX IF NOT EXISTS auditoria_evento_idempotente_uq
    ON auditoria_arquivos (tenant_id, hash_sha256, evento);

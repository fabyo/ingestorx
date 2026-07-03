use crate::queue::EventoArquivo;
use tokio_postgres::{Client, NoTls};

pub struct StateStore {
    client: Client,
}

impl StateStore {
    pub async fn conectar(url: &str) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(erro = %error, "conexão PostgreSQL encerrada");
            }
        });
        client.simple_query("SELECT 1").await?;
        Ok(Self { client })
    }

    pub async fn registrar_processado(
        &self,
        tenant_id: &str,
        evento: &EventoArquivo,
        object_key: &str,
        nome_original: &str,
    ) -> anyhow::Result<()> {
        let tamanho = i64::try_from(evento.tamanho_bytes)
            .map_err(|_| anyhow::anyhow!("tamanho não cabe em bigint"))?;
        self.client
            .execute(
                "WITH gravado AS (
                   INSERT INTO arquivos
                     (tenant_id, hash_sha256, correlation_id, nome_original,
                      object_key, tamanho_bytes, status, detectado_em, processado_em)
                   VALUES ($1, $2, $3, $4, $5, $6, 'processado', $7::text::timestamptz, now())
                   ON CONFLICT (tenant_id, hash_sha256) DO UPDATE SET
                     object_key = EXCLUDED.object_key, status = 'processado',
                     processado_em = now(), atualizado_em = now(), erro = NULL
                   RETURNING tenant_id, hash_sha256
                 )
                 INSERT INTO auditoria_arquivos (tenant_id, hash_sha256, evento, detalhes)
                 SELECT tenant_id, hash_sha256, 'processado',
                        jsonb_build_object('object_key', $5::text)
                 FROM gravado
                 ON CONFLICT (tenant_id, hash_sha256, evento) DO NOTHING",
                &[
                    &tenant_id,
                    &evento.hash_sha256,
                    &evento.correlation_id,
                    &nome_original,
                    &object_key,
                    &tamanho,
                    &evento.detectado_em.0,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn object_ja_processado(
        &self,
        tenant_id: &str,
        hash_sha256: &str,
    ) -> anyhow::Result<Option<String>> {
        let row = self
            .client
            .query_opt(
                "SELECT object_key FROM arquivos
                 WHERE tenant_id = $1 AND hash_sha256 = $2 AND status = 'processado'",
                &[&tenant_id, &hash_sha256],
            )
            .await?;
        Ok(row.and_then(|r| r.get(0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;
    use std::path::PathBuf;

    #[tokio::test]
    #[ignore = "requer PostgreSQL local configurado"]
    async fn registro_e_auditoria_sao_idempotentes() {
        let config = crate::config::WatcherConfig::from_env_or_default().unwrap();
        let state = StateStore::conectar(&config.postgres_url).await.unwrap();
        let hash = hex::encode(sha2::Sha256::digest(uuid::Uuid::new_v4().as_bytes()));
        let evento = EventoArquivo::novo(hash.clone(), PathBuf::from("x.xml"), 10, "teste");
        for _ in 0..2 {
            state
                .registrar_processado("teste", &evento, "teste/key.xml", "x.xml")
                .await
                .unwrap();
        }
        let row = state
            .client
            .query_one(
                "SELECT count(*) FROM auditoria_arquivos
                 WHERE tenant_id = 'teste' AND hash_sha256 = $1 AND evento = 'processado'",
                &[&hash],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 1);
        state
            .client
            .execute(
                "DELETE FROM auditoria_arquivos WHERE tenant_id = 'teste' AND hash_sha256 = $1",
                &[&hash],
            )
            .await
            .unwrap();
        state
            .client
            .execute(
                "DELETE FROM arquivos WHERE tenant_id = 'teste' AND hash_sha256 = $1",
                &[&hash],
            )
            .await
            .unwrap();
    }
}

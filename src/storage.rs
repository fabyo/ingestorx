use crate::queue::EventoArquivo;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct ObjectStorage {
    host: String,
    port: u16,
    authority: String,
    region: String,
    bucket: String,
    access_key: String,
    secret_key: String,
}

impl ObjectStorage {
    pub async fn conectar(
        endpoint: &str,
        region: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
    ) -> anyhow::Result<Self> {
        let authority = endpoint.strip_prefix("http://").ok_or_else(|| {
            anyhow::anyhow!("object storage local requer endpoint http://host:porta")
        })?;
        anyhow::ensure!(!authority.contains('/'), "endpoint S3 não pode conter path");
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("endpoint S3 deve informar a porta"))?;
        let storage = Self {
            host: host.to_string(),
            port: port.parse()?,
            authority: authority.to_string(),
            region: region.to_string(),
            bucket: bucket.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
        };
        storage.garantir_bucket().await?;
        Ok(storage)
    }

    async fn garantir_bucket(&self) -> anyhow::Result<()> {
        let vazio = hex::encode(Sha256::digest([]));
        let status = self
            .request("PUT", &format!("/{}", self.bucket), &vazio, None, 0)
            .await?;
        anyhow::ensure!(
            status == 200 || status == 409,
            "falha ao criar/verificar bucket {}: HTTP {status}",
            self.bucket
        );
        Ok(())
    }

    pub async fn enviar(
        &self,
        tenant_id: &str,
        evento: &EventoArquivo,
        arquivo: &Path,
    ) -> anyhow::Result<String> {
        let key = chave_objeto(tenant_id, evento);
        let tamanho = tokio::fs::metadata(arquivo).await?.len();
        anyhow::ensure!(
            tamanho == evento.tamanho_bytes,
            "arquivo mudou antes do upload"
        );
        let status = self
            .request(
                "PUT",
                &format!("/{}/{}", self.bucket, key),
                &evento.hash_sha256,
                Some(arquivo),
                tamanho,
            )
            .await?;
        anyhow::ensure!(status == 200, "upload S3 falhou: HTTP {status}");
        Ok(key)
    }

    async fn request(
        &self,
        method: &str,
        uri: &str,
        payload_hash: &str,
        arquivo: Option<&Path>,
        tamanho: u64,
    ) -> anyhow::Result<u16> {
        let agora = chrono::Utc::now();
        let amz_date = agora.format("%Y%m%dT%H%M%SZ").to_string();
        let data = agora.format("%Y%m%d").to_string();
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_headers = format!(
            "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
            self.authority, payload_hash, amz_date
        );
        let canonical_request =
            format!("{method}\n{uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
        let scope = format!("{data}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key = chave_assinatura(&self.secret_key, &data, &self.region)?;
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes())?);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );

        let mut stream = tokio::net::TcpStream::connect((&*self.host, self.port)).await?;
        let headers = format!(
            "{method} {uri} HTTP/1.1\r\nHost: {}\r\nContent-Length: {tamanho}\r\nContent-Type: application/xml\r\nX-Amz-Content-Sha256: {payload_hash}\r\nX-Amz-Date: {amz_date}\r\nAuthorization: {authorization}\r\nConnection: close\r\n\r\n",
            self.authority
        );
        stream.write_all(headers.as_bytes()).await?;
        if let Some(path) = arquivo {
            let mut file = tokio::fs::File::open(path).await?;
            tokio::io::copy(&mut file, &mut stream).await?;
        }
        stream.flush().await?;
        let mut resposta = Vec::new();
        stream.read_to_end(&mut resposta).await?;
        let primeira = String::from_utf8_lossy(&resposta)
            .lines()
            .next()
            .unwrap_or_default()
            .to_string();
        primeira
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| anyhow::anyhow!("resposta HTTP inválida do object storage"))?
            .parse()
            .map_err(Into::into)
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn chave_assinatura(secret: &str, data: &str, region: &str) -> anyhow::Result<Vec<u8>> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), data.as_bytes())?;
    let k_region = hmac_sha256(&k_date, region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, b"s3")?;
    hmac_sha256(&k_service, b"aws4_request")
}

pub fn chave_objeto(tenant_id: &str, evento: &EventoArquivo) -> String {
    let tenant: String = tenant_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let data = evento.detectado_em.0.get(..10).unwrap_or("sem-data");
    let mut partes = data.split('-');
    let ano = partes.next().unwrap_or("sem-ano");
    let mes = partes.next().unwrap_or("sem-mes");
    format!("{tenant}/{ano}/{mes}/{}.xml", evento.hash_sha256)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn chave_e_deterministica_e_particionada() {
        let evento = EventoArquivo::novo("a".repeat(64), PathBuf::from("x.xml"), 1, "t");
        let chave = chave_objeto("cliente/um", &evento);
        assert!(chave.starts_with("cliente_um/"));
        assert!(chave.ends_with(&format!("/{}.xml", "a".repeat(64))));
        assert!(!chave.contains("cliente/um"));
    }

    #[tokio::test]
    #[ignore = "requer MinIO local configurado"]
    async fn minio_aceita_upload_assinado() {
        let config = crate::config::WatcherConfig::from_env_or_default().unwrap();
        let storage = ObjectStorage::conectar(
            &config.object_storage_endpoint,
            &config.object_storage_region,
            &config.object_storage_bucket,
            &config.object_storage_access_key,
            &config.object_storage_secret_key,
        )
        .await
        .unwrap();
        let path = std::env::temp_dir().join(format!("minio-{}.xml", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, b"<xml/>").await.unwrap();
        let hash = crate::file_ops::calcular_hash_sha256(&path).await.unwrap();
        let evento = EventoArquivo::novo(hash, path.clone(), 6, "teste");
        let key = storage.enviar("teste", &evento, &path).await.unwrap();
        assert!(key.starts_with("teste/"));
        tokio::fs::remove_file(path).await.unwrap();
    }
}

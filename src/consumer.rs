use crate::config::WatcherConfig;
use crate::file_ops;
use crate::queue::{EventoArquivo, QueueError, RabbitMqPublisher};
use futures_util::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error(transparent)]
    Fila(#[from] QueueError),
    #[error("erro RabbitMQ: {0}")]
    Rabbit(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Arquivo(#[from] file_ops::FileOpsError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("hash divergente: esperado {esperado}, encontrado {encontrado}")]
    HashDivergente {
        esperado: String,
        encontrado: String,
    },
}

pub async fn executar(config: WatcherConfig, cancel: CancellationToken) -> anyhow::Result<()> {
    let state = Arc::new(crate::state::StateStore::conectar(&config.postgres_url).await?);
    let storage = Arc::new(
        crate::storage::ObjectStorage::conectar(
            &config.object_storage_endpoint,
            &config.object_storage_region,
            &config.object_storage_bucket,
            &config.object_storage_access_key,
            &config.object_storage_secret_key,
        )
        .await?,
    );
    let publisher = RabbitMqPublisher::new(
        &config.rabbitmq_url,
        &config.rabbitmq_fila,
        config.rabbitmq_timeout,
        config.rabbitmq_ca_cert.as_deref(),
    )
    .map_err(anyhow::Error::msg)?;
    let conexao = publisher.conexao().await?;
    let channel = conexao
        .create_channel()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    channel
        .queue_declare(
            &format!("{}.dlq", config.rabbitmq_fila),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_declare(
            &config.rabbitmq_fila,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await?;
    channel.basic_qos(16, BasicQosOptions::default()).await?;
    let mut consumer = channel
        .basic_consume(
            &config.rabbitmq_fila,
            &format!("ingestorx-{}", config.worker_id),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!(fila = %config.rabbitmq_fila, "consumidor downstream ativo");

    let config = Arc::new(config);
    let limite = Arc::new(tokio::sync::Semaphore::new(
        config.max_consumidores_concorrentes,
    ));
    let mut tarefas = tokio::task::JoinSet::new();
    loop {
        let entrega = tokio::select! {
            entrega = consumer.next() => entrega,
            _ = cancel.cancelled() => {
                tarefas.join_all().await;
                return Ok(());
            },
        };
        let Some(entrega) = entrega else {
            anyhow::bail!("stream do consumidor RabbitMQ encerrado");
        };
        let entrega = entrega?;
        let permit = limite.clone().acquire_owned().await?;
        let config = Arc::clone(&config);
        let state = Arc::clone(&state);
        let storage = Arc::clone(&storage);
        let channel = channel.clone();
        tarefas.spawn(async move {
            let _permit = permit;
            processar_entrega(config, state, storage, channel, entrega).await
        });
        while let Some(resultado) = tarefas.try_join_next() {
            resultado??;
        }
    }
}

async fn processar_entrega(
    config: Arc<WatcherConfig>,
    state: Arc<crate::state::StateStore>,
    storage: Arc<crate::storage::ObjectStorage>,
    channel: lapin::Channel,
    entrega: lapin::message::Delivery,
) -> anyhow::Result<()> {
    let evento = match serde_json::from_slice::<EventoArquivo>(&entrega.data) {
        Ok(evento) => evento,
        Err(e) => {
            error!(erro = %e, "mensagem inválida descartada");
            publicar_dlq(&channel, &config.rabbitmq_fila, &entrega.data).await?;
            entrega.ack(BasicAckOptions::default()).await?;
            return Ok(());
        }
    };

    let ja_processado = match state
        .object_ja_processado(&config.tenant_id, &evento.hash_sha256)
        .await
    {
        Ok(valor) => valor,
        Err(e) => {
            warn!(erro = %e, correlation_id = %evento.correlation_id, "PostgreSQL indisponível; mensagem será reenfileirada");
            entrega
                .nack(BasicNackOptions {
                    requeue: true,
                    ..Default::default()
                })
                .await?;
            return Ok(());
        }
    };
    if let Some(object_key) = ja_processado {
        entrega.ack(BasicAckOptions::default()).await?;
        remover_recibo(&config, &evento.path_absoluto).await;
        remover_copia_local_processada(&config, &evento.path_absoluto).await;
        info!(correlation_id = %evento.correlation_id, object_key, "duplicata já persistida; mensagem confirmada");
        return Ok(());
    }

    match processar_evento(&config, &evento).await {
        Ok(destino) => {
            let nome_original = destino
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.splitn(3, "__").nth(2))
                .unwrap_or("desconhecido");
            let persistencia = async {
                let object_key = storage.enviar(&config.tenant_id, &evento, &destino).await?;
                state
                    .registrar_processado(&config.tenant_id, &evento, &object_key, nome_original)
                    .await?;
                anyhow::Ok(object_key)
            }
            .await;
            let object_key = match persistencia {
                Ok(key) => key,
                Err(e) => {
                    warn!(erro = %e, correlation_id = %evento.correlation_id, "falha no banco/object storage; mensagem será reenfileirada");
                    entrega
                        .nack(BasicNackOptions {
                            requeue: true,
                            ..Default::default()
                        })
                        .await?;
                    return Ok(());
                }
            };
            entrega.ack(BasicAckOptions::default()).await?;
            remover_recibo(&config, &evento.path_absoluto).await;
            if let Err(e) = tokio::fs::remove_file(&destino).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!(path = %destino.display(), erro = %e, "object storage confirmado, mas cópia local não pôde ser removida");
                }
            }
            info!(
                correlation_id = %evento.correlation_id,
                destino = %destino.display(),
                object_key = %object_key,
                "arquivo processado e mensagem confirmada"
            );
        }
        Err(ConsumerError::HashDivergente { .. }) => {
            mover_corrompido_para_erro(&config, &evento.path_absoluto).await;
            publicar_dlq(&channel, &config.rabbitmq_fila, &entrega.data).await?;
            entrega.ack(BasicAckOptions::default()).await?;
            error!(correlation_id = %evento.correlation_id, "hash divergente; mensagem rejeitada");
        }
        Err(e) => {
            warn!(erro = %e, correlation_id = %evento.correlation_id, "falha transitória; mensagem será reenfileirada");
            entrega
                .nack(BasicNackOptions {
                    requeue: true,
                    ..Default::default()
                })
                .await?;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
    Ok(())
}

async fn publicar_dlq(channel: &lapin::Channel, fila: &str, payload: &[u8]) -> anyhow::Result<()> {
    let confirmacao = channel
        .basic_publish(
            "",
            &format!("{fila}.dlq"),
            lapin::options::BasicPublishOptions::default(),
            payload,
            lapin::BasicProperties::default().with_delivery_mode(2),
        )
        .await?
        .await?;
    anyhow::ensure!(confirmacao.is_ack(), "RabbitMQ rejeitou publicação na DLQ");
    Ok(())
}

async fn remover_copia_local_processada(config: &WatcherConfig, origem: &Path) {
    let Some(nome) = origem.file_name() else {
        return;
    };
    for path in [origem.to_path_buf(), config.pasta_processado.join(nome)] {
        if let Err(e) = tokio::fs::remove_file(&path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(path = %path.display(), erro = %e, "falha ao remover cópia local idempotente");
            }
        }
    }
}

pub async fn processar_evento(
    config: &WatcherConfig,
    evento: &EventoArquivo,
) -> Result<PathBuf, ConsumerError> {
    let nome = evento
        .path_absoluto
        .file_name()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "arquivo sem nome"))?;
    let destino = config.pasta_processado.join(nome);

    if !tokio::fs::try_exists(&evento.path_absoluto).await? {
        if tokio::fs::try_exists(&destino).await?
            && file_ops::calcular_hash_sha256(&destino).await? == evento.hash_sha256
        {
            return Ok(destino);
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("arquivo não encontrado: {}", evento.path_absoluto.display()),
        )
        .into());
    }

    let hash = file_ops::calcular_hash_sha256(&evento.path_absoluto).await?;
    if hash != evento.hash_sha256 {
        return Err(ConsumerError::HashDivergente {
            esperado: evento.hash_sha256.clone(),
            encontrado: hash,
        });
    }
    tokio::fs::rename(&evento.path_absoluto, &destino).await?;
    Ok(destino)
}

async fn remover_recibo(config: &WatcherConfig, origem: &Path) {
    let Some(nome) = origem.file_name() else {
        return;
    };
    let recibo = config
        .pasta_recibos_publicacao
        .join(format!("{}.json", nome.to_string_lossy()));
    if let Err(e) = tokio::fs::remove_file(&recibo).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!(path = %recibo.display(), erro = %e, "falha ao remover recibo processado");
        }
    }
}

async fn mover_corrompido_para_erro(config: &WatcherConfig, origem: &Path) {
    let Some(nome) = origem.file_name() else {
        return;
    };
    let destino = config
        .pasta_erro
        .join(format!("hash-invalido-{}", nome.to_string_lossy()));
    if let Err(e) = tokio::fs::rename(origem, &destino).await {
        error!(erro = %e, path = %origem.display(), "falha ao mover arquivo com hash divergente");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    fn config_teste(base: &Path) -> WatcherConfig {
        WatcherConfig {
            pasta_entrada: base.join("entrada"),
            pasta_processando: base.join("processando/worker"),
            pasta_processado: base.join("processado"),
            pasta_erro: base.join("erro"),
            pasta_ignorados: base.join("ignorados"),
            pasta_logs: base.join("logs"),
            pasta_buffer_contingencia: base.join("buffer"),
            pasta_recibos_publicacao: base.join("recibos/worker"),
            tamanho_maximo_bytes: 1024,
            intervalo_checagem_estabilidade: Duration::from_millis(1),
            leituras_estaveis_necessarias: 2,
            intervalo_scanner_reconciliacao: Duration::from_secs(1),
            intervalo_heartbeat: Duration::from_secs(1),
            max_tentativas_retry: 1,
            tenant_id: "tenant".into(),
            worker_id: "worker".into(),
            extensoes_permitidas: vec!["xml".into()],
            rabbitmq_url: "amqps://teste".into(),
            rabbitmq_fila: "teste".into(),
            rabbitmq_timeout: Duration::from_secs(1),
            rabbitmq_ca_cert: None,
            observabilidade_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            pasta_lock_scanner: base.join(".lock"),
            max_processamentos_concorrentes: 4,
            max_consumidores_concorrentes: 2,
            postgres_url: "postgresql://localhost/teste".into(),
            object_storage_endpoint: "http://localhost:9000".into(),
            object_storage_region: "us-east-1".into(),
            object_storage_bucket: "teste".into(),
            object_storage_access_key: "teste".into(),
            object_storage_secret_key: "teste".into(),
        }
    }

    #[tokio::test]
    async fn move_para_processado_e_e_idempotente() {
        let base =
            std::env::temp_dir().join(format!("ingestorx_consumer_{}", uuid::Uuid::new_v4()));
        let config = config_teste(&base);
        config.garantir_estrutura_pastas().unwrap();
        let origem = config.pasta_processando.join("arquivo.xml");
        tokio::fs::write(&origem, b"conteudo").await.unwrap();
        let hash = file_ops::calcular_hash_sha256(&origem).await.unwrap();
        let evento = EventoArquivo::novo(hash, origem.clone(), 8, "tenant");

        let destino = processar_evento(&config, &evento).await.unwrap();
        assert_eq!(tokio::fs::read(&destino).await.unwrap(), b"conteudo");
        assert_eq!(processar_evento(&config, &evento).await.unwrap(), destino);
        tokio::fs::remove_dir_all(base).await.unwrap();
    }

    #[tokio::test]
    async fn rejeita_hash_divergente() {
        let base = std::env::temp_dir().join(format!("ingestorx_hash_{}", uuid::Uuid::new_v4()));
        let config = config_teste(&base);
        config.garantir_estrutura_pastas().unwrap();
        let origem = config.pasta_processando.join("arquivo.xml");
        tokio::fs::write(&origem, b"alterado").await.unwrap();
        let evento = EventoArquivo::novo("0".repeat(64), origem, 8, "tenant");

        assert!(matches!(
            processar_evento(&config, &evento).await,
            Err(ConsumerError::HashDivergente { .. })
        ));
        tokio::fs::remove_dir_all(base).await.unwrap();
    }
}

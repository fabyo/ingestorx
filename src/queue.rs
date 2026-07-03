//! Publicação na fila — o "aviso" que o Rust dá ao resto do pipeline depois
//! que o arquivo já está seguro na pasta de processamento.
//!
//! Princípio central: o ARQUIVO JÁ FOI MOVIDO de forma atômica antes de
//! chegarmos aqui. Publicar na fila é "avisar que existe trabalho a fazer",
//! não é o que garante a posse do arquivo — isso já foi resolvido pelo
//! `rename()` em file_ops.rs. Essa ordem importa: nunca publique antes de
//! ter certeza de que o move foi bem-sucedido, senão um consumidor pode
//! tentar processar um arquivo que ainda está em trânsito.
//!
//! O publisher RabbitMQ usa confirmações do broker. O resto do sistema
//! depende apenas da trait `QueuePublisher`, permitindo trocar o broker.

use async_trait::async_trait;
use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    tcp::OwnedTLSConfig,
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{error, info, instrument, warn};

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("broker inacessível: {0}")]
    BrokerInacessivel(String),
    #[error("falha de IO no buffer de contingência: {0}")]
    Io(#[from] std::io::Error),
    #[error("falha de serialização: {0}")]
    Serializacao(#[from] serde_json::Error),
}

/// Mensagem publicada na fila. Carrega o `correlation_id` (= hash do
/// conteúdo) que vai acompanhar o arquivo em todo o pipeline downstream —
/// essencial para tracing distribuído e para os consumidores aplicarem
/// idempotência (descartar duplicata caso o mesmo evento seja publicado
/// mais de uma vez por alguma falha de rede entre o ack e a confirmação).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventoArquivo {
    pub correlation_id: String,
    pub hash_sha256: String,
    pub path_absoluto: PathBuf,
    pub tamanho_bytes: u64,
    pub detectado_em: chrono_like_timestamp::AgoraIso8601,
}

// Helper para gerar um timestamp ISO-8601 real via chrono.
pub mod chrono_like_timestamp {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct AgoraIso8601(pub String);

    impl AgoraIso8601 {
        pub fn agora() -> Self {
            let agora = chrono::Utc::now();
            Self(agora.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        }
    }
}

#[async_trait]
pub trait QueuePublisher: Send + Sync {
    /// Publica o evento e só retorna `Ok` depois de receber confirmação
    /// real do broker (publisher confirms no RabbitMQ, ou ack de partição
    /// no Kafka). NUNCA considere sucesso só por não ter dado erro de rede
    /// na chamada — isso é "fire and forget" disfarçado de garantia.
    async fn publicar(&self, evento: &EventoArquivo) -> Result<(), QueueError>;

    /// Tenta drenar os eventos salvos localmente em buffer de contingência.
    async fn drenar(&self) -> Result<(), QueueError>;

    /// Verificação barata usada pelo endpoint de readiness.
    async fn pronto(&self) -> Result<(), QueueError> {
        Ok(())
    }
}

/// Implementação real do RabbitMQ usando pool de conexões auto-curável.
pub struct RabbitMqPublisher {
    url: String,
    fila: String,
    timeout: Duration,
    tls: OwnedTLSConfig,
    conexao: Mutex<Option<std::sync::Arc<Connection>>>,
    canal: Mutex<Option<Channel>>,
}

impl RabbitMqPublisher {
    pub fn new(
        url: &str,
        fila: &str,
        timeout: Duration,
        ca_cert: Option<&Path>,
    ) -> Result<Self, String> {
        let cert_chain = ca_cert
            .map(std::fs::read_to_string)
            .transpose()
            .map_err(|e| format!("falha ao ler CA do RabbitMQ: {e}"))?;
        Ok(Self {
            url: url.to_string(),
            fila: fila.to_string(),
            timeout,
            tls: OwnedTLSConfig {
                identity: None,
                cert_chain,
            },
            conexao: Mutex::new(None),
            canal: Mutex::new(None),
        })
    }

    async fn canal(&self) -> Result<Channel, QueueError> {
        let mut guard = self.canal.lock().await;
        if let Some(canal) = guard.as_ref().filter(|c| c.status().connected()) {
            return Ok(canal.clone());
        }

        let conn = self.conexao().await?;
        let channel = self.aguardar("criar canal", conn.create_channel()).await?;
        self.aguardar(
            "declarar DLQ",
            channel.queue_declare(
                &format!("{}.dlq", self.fila),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            ),
        )
        .await?;
        self.aguardar(
            "declarar fila",
            channel.queue_declare(
                &self.fila,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            ),
        )
        .await?;
        self.aguardar(
            "habilitar confirms",
            channel.confirm_select(lapin::options::ConfirmSelectOptions::default()),
        )
        .await?;
        *guard = Some(channel.clone());
        Ok(channel)
    }

    pub async fn conexao(&self) -> Result<std::sync::Arc<Connection>, QueueError> {
        let mut guard = self.conexao.lock().await;
        if let Some(conexao) = guard.as_ref().filter(|c| c.status().connected()) {
            return Ok(std::sync::Arc::clone(conexao));
        }

        let conexao = self
            .aguardar(
                "conectar com RabbitMQ",
                Connection::connect_with_config(
                    &self.url,
                    ConnectionProperties::default(),
                    OwnedTLSConfig {
                        identity: None,
                        cert_chain: self.tls.cert_chain.clone(),
                    },
                ),
            )
            .await?;
        let conexao = std::sync::Arc::new(conexao);
        *guard = Some(std::sync::Arc::clone(&conexao));
        Ok(conexao)
    }

    async fn aguardar<T, E, F>(&self, operacao: &'static str, future: F) -> Result<T, QueueError>
    where
        E: std::fmt::Display,
        F: Future<Output = Result<T, E>>,
    {
        tokio::time::timeout(self.timeout, future)
            .await
            .map_err(|_| {
                QueueError::BrokerInacessivel(format!(
                    "timeout de {:?} durante {operacao}",
                    self.timeout
                ))
            })?
            .map_err(|e| QueueError::BrokerInacessivel(e.to_string()))
    }
}

#[async_trait]
impl QueuePublisher for RabbitMqPublisher {
    async fn publicar(&self, evento: &EventoArquivo) -> Result<(), QueueError> {
        // Canal confirmado reutilizável; recriado automaticamente se cair.
        let channel = self.canal().await?;

        let payload = serde_json::to_vec(evento)?;

        // Default exchange roteia diretamente pelo nome da fila.
        let confirm_pendente = self
            .aguardar(
                "publicar",
                channel.basic_publish(
                    "",
                    &self.fila,
                    BasicPublishOptions::default(),
                    &payload, // Passando o slice de bytes
                    BasicProperties::default().with_delivery_mode(2), // Persistent mode
                ),
            )
            .await?;
        let confirm = self
            .aguardar("aguardar publisher confirm", confirm_pendente)
            .await?;

        if confirm.is_ack() {
            info!(
                path = %evento.path_absoluto.display(),
                hash = %evento.hash_sha256,
                "evento publicado com sucesso no RabbitMQ (Ack recebido)"
            );
            Ok(())
        } else {
            Err(QueueError::BrokerInacessivel("Broker retornou Nack".into()))
        }
    }

    async fn drenar(&self) -> Result<(), QueueError> {
        Ok(())
    }

    async fn pronto(&self) -> Result<(), QueueError> {
        self.conexao().await.map(|_| ())
    }
}

/// Decorator que adiciona BUFFER DE CONTINGÊNCIA DURÁVEL em torno de
/// qualquer `QueuePublisher` real.
///
/// Por quê: se o broker cair, o watcher NÃO PODE simplesmente perder o
/// evento nem ficar bloqueado esperando indefinidamente enquanto novos
/// XMLs continuam chegando. A estratégia:
///
/// 1. Tenta publicar normalmente.
/// 2. Se falhar, grava o evento em um arquivo local append-only (uma
///    linha JSON por evento, com fsync) — o arquivo já está fisicamente
///    seguro na pasta de processamento, então perder a mensagem da fila
///    não significa perder o XML, só atrasa o processamento.
/// 3. Uma task separada (`drenar_buffer_contingencia`) tenta reenviar
///    periodicamente o que está no buffer assim que o broker volta.
pub struct PublisherComContingencia<P: QueuePublisher> {
    publisher_real: P,
    arquivo_buffer: PathBuf,
    /// Protege append e rotação/drenagem do JSONL no processo. Sem isso,
    /// duas falhas simultâneas poderiam intercalar bytes de eventos.
    lock_buffer: Mutex<()>,
}

impl<P: QueuePublisher> PublisherComContingencia<P> {
    pub fn new(publisher_real: P, pasta_buffer: &Path) -> Self {
        Self {
            publisher_real,
            arquivo_buffer: pasta_buffer.join("eventos_pendentes.jsonl"),
            lock_buffer: Mutex::new(()),
        }
    }

    async fn gravar_no_buffer(&self, evento: &EventoArquivo) -> Result<(), QueueError> {
        use tokio::io::AsyncWriteExt;

        let _guard = self.lock_buffer.lock().await;
        let mut linha = serde_json::to_vec(evento)?;
        linha.push(b'\n');
        let mut arquivo = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.arquivo_buffer)
            .await?;

        arquivo.write_all(&linha).await?;
        // fsync explícito: garante que a linha está em disco antes de
        // seguirmos, não só no page cache do SO. Crítico para um buffer
        // de contingência — se ele mesmo não for durável, não serve pra nada.
        arquivo.sync_all().await?;

        Ok(())
    }

    async fn drenar_internamente(&self) -> Result<(), QueueError> {
        use tokio::io::AsyncBufReadExt;

        let _guard = self.lock_buffer.lock().await;
        let path = &self.arquivo_buffer;
        if !path.exists() {
            return Ok(());
        }

        let working_path = path.with_extension("jsonl.working");

        if !working_path.exists() {
            if let Err(e) = tokio::fs::rename(path, &working_path).await {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Ok(());
                }
                return Err(QueueError::Io(e));
            }
        }

        info!(
            arquivo = %working_path.display(),
            "iniciando processamento do buffer de contingência local"
        );

        let arquivo = match tokio::fs::File::open(&working_path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(QueueError::Io(e)),
        };

        let leitor = tokio::io::BufReader::new(arquivo);
        let mut linhas = leitor.lines();
        let mut eventos = Vec::new();
        let mut linhas_corrompidas = Vec::new();

        while let Some(linha) = linhas.next_line().await? {
            let linha_trim = linha.trim();
            if linha_trim.is_empty() {
                continue;
            }
            match serde_json::from_str::<EventoArquivo>(linha_trim) {
                Ok(e) => eventos.push(e),
                Err(err) => {
                    error!(
                        erro = %err,
                        linha = %linha_trim,
                        "linha corrompida no buffer de contingência local, enviando para quarentena"
                    );
                    linhas_corrompidas.push(linha);
                }
            }
        }

        if !linhas_corrompidas.is_empty() {
            self.gravar_linhas_corrompidas(&linhas_corrompidas).await?;
        }

        if eventos.is_empty() {
            let _ = tokio::fs::remove_file(&working_path).await;
            return Ok(());
        }

        let mut index_falha = None;

        for (i, evento) in eventos.iter().enumerate() {
            match self.publisher_real.publicar(evento).await {
                Ok(()) => {
                    info!(
                        correlation_id = %evento.correlation_id,
                        "evento do buffer de contingência publicado com sucesso"
                    );
                }
                Err(e) => {
                    warn!(
                        erro = %e,
                        correlation_id = %evento.correlation_id,
                        "falha ao reenviar evento do buffer de contingência, abortando drenagem"
                    );
                    index_falha = Some(i);
                    break;
                }
            }
        }

        if let Some(idx) = index_falha {
            let mut novas_linhas = Vec::new();
            for evento in &eventos[idx..] {
                let linha = serde_json::to_string(evento)?;
                novas_linhas.push(linha);
            }

            let mut arquivo_original = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await?;

            for linha in novas_linhas {
                use tokio::io::AsyncWriteExt;
                arquivo_original.write_all(linha.as_bytes()).await?;
                arquivo_original.write_all(b"\n").await?;
            }
            arquivo_original.sync_all().await?;

            let _ = tokio::fs::remove_file(&working_path).await;

            Err(QueueError::BrokerInacessivel(
                "broker falhou durante drenagem".into(),
            ))
        } else {
            let _ = tokio::fs::remove_file(&working_path).await;
            info!("todos os eventos do buffer de contingência foram drenados com sucesso");
            Ok(())
        }
    }

    async fn gravar_linhas_corrompidas(&self, linhas: &[String]) -> Result<(), QueueError> {
        use tokio::io::AsyncWriteExt;

        let path = self
            .arquivo_buffer
            .with_file_name("eventos_corrompidos.jsonl");
        let mut arquivo = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        for linha in linhas {
            arquivo.write_all(linha.as_bytes()).await?;
            arquivo.write_all(b"\n").await?;
        }
        arquivo.sync_all().await?;
        Ok(())
    }
}

#[async_trait]
impl<P: QueuePublisher> QueuePublisher for PublisherComContingencia<P> {
    #[instrument(skip(self), fields(correlation_id = %evento.correlation_id))]
    async fn publicar(&self, evento: &EventoArquivo) -> Result<(), QueueError> {
        match self.publisher_real.publicar(evento).await {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!(
                    erro = %e,
                    "broker inacessível, gravando evento no buffer de contingência local"
                );
                self.gravar_no_buffer(evento).await?;
                error!(
                    correlation_id = %evento.correlation_id,
                    "evento NÃO publicado no broker — em buffer de contingência aguardando drenagem"
                );
                Ok(())
            }
        }
    }

    async fn drenar(&self) -> Result<(), QueueError> {
        self.drenar_internamente().await
    }

    async fn pronto(&self) -> Result<(), QueueError> {
        self.publisher_real.pronto().await
    }
}

impl EventoArquivo {
    pub fn novo(
        hash_sha256: String,
        path_absoluto: PathBuf,
        tamanho_bytes: u64,
        tenant_id: &str,
    ) -> Self {
        let correlation_id = format!("{}_{}", hash_sha256, tenant_id);
        Self {
            correlation_id,
            hash_sha256,
            path_absoluto,
            tamanho_bytes,
            detectado_em: chrono_like_timestamp::AgoraIso8601::agora(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct PublisherFalho;

    #[async_trait]
    impl QueuePublisher for PublisherFalho {
        async fn publicar(&self, _: &EventoArquivo) -> Result<(), QueueError> {
            Err(QueueError::BrokerInacessivel("teste".into()))
        }

        async fn drenar(&self) -> Result<(), QueueError> {
            Ok(())
        }
    }

    struct PublisherContador(AtomicUsize);

    #[async_trait]
    impl QueuePublisher for PublisherContador {
        async fn publicar(&self, _: &EventoArquivo) -> Result<(), QueueError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn drenar(&self) -> Result<(), QueueError> {
            Ok(())
        }
    }

    fn pasta_teste(nome: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ingestorx_{nome}_{}", uuid::Uuid::new_v4()))
    }

    fn evento(id: usize) -> EventoArquivo {
        EventoArquivo::novo(
            format!("{id:064x}"),
            PathBuf::from(format!("arquivo_{id}.xml")),
            id as u64,
            "teste",
        )
    }

    #[tokio::test]
    async fn gravacao_concorrente_mantem_um_json_por_linha() {
        let pasta = pasta_teste("concorrencia");
        tokio::fs::create_dir_all(&pasta).await.unwrap();
        let publisher = Arc::new(PublisherComContingencia::new(PublisherFalho, &pasta));
        let mut tarefas = Vec::new();

        for id in 0..32 {
            let publisher = Arc::clone(&publisher);
            tarefas.push(tokio::spawn(async move {
                publisher.publicar(&evento(id)).await
            }));
        }
        for tarefa in tarefas {
            tarefa.await.unwrap().unwrap();
        }

        let conteudo = tokio::fs::read_to_string(pasta.join("eventos_pendentes.jsonl"))
            .await
            .unwrap();
        let linhas: Vec<_> = conteudo.lines().collect();
        assert_eq!(linhas.len(), 32);
        assert!(linhas
            .iter()
            .all(|linha| serde_json::from_str::<EventoArquivo>(linha).is_ok()));
        tokio::fs::remove_dir_all(pasta).await.unwrap();
    }

    #[tokio::test]
    async fn drenagem_publica_validos_e_preserva_corrompidos() {
        let pasta = pasta_teste("corrompido");
        tokio::fs::create_dir_all(&pasta).await.unwrap();
        let valido = serde_json::to_string(&evento(1)).unwrap();
        tokio::fs::write(
            pasta.join("eventos_pendentes.jsonl"),
            format!("{{json-invalido}}\n{valido}\n"),
        )
        .await
        .unwrap();
        let publisher =
            PublisherComContingencia::new(PublisherContador(AtomicUsize::new(0)), &pasta);

        publisher.drenar().await.unwrap();

        assert_eq!(publisher.publisher_real.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            tokio::fs::read_to_string(pasta.join("eventos_corrompidos.jsonl"))
                .await
                .unwrap(),
            "{json-invalido}\n"
        );
        tokio::fs::remove_dir_all(pasta).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requer RABBITMQ_INTEGRATION_URL e um RabbitMQ ativo"]
    async fn rabbitmq_declara_fila_e_confirma_publicacao() {
        let url = std::env::var("RABBITMQ_INTEGRATION_URL").unwrap();
        let fila = std::env::var("RABBITMQ_INTEGRATION_QUEUE")
            .unwrap_or_else(|_| "ingestorx_integracao".to_string());
        let ca = std::env::var("RABBITMQ_INTEGRATION_CA").ok();
        let publisher = RabbitMqPublisher::new(
            &url,
            &fila,
            Duration::from_secs(5),
            ca.as_deref().map(Path::new),
        )
        .unwrap();
        publisher.publicar(&evento(99)).await.unwrap();
    }
}

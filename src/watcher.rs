//! Núcleo do watcher: listener inotify + scanner de reconciliação,
//! convergindo no MESMO caminho de tratamento (`tratar_evento`).
//!
//! Isso resolve um problema de design comum (e que você mesmo desenhou
//! no esboço inicial): ter o EventListener e o Scanner publicando de
//! forma independente cria corrida e duplicidade. Aqui, os dois só
//! GERAM CANDIDATOS a arquivo pronto; quem decide se age ou não é sempre
//! a mesma função, protegida por um conjunto de "claims" em memória + o
//! `rename()` atômico como garantia final (mesmo entre instâncias, já que
//! um rename para um destino já ocupado falha de forma segura).

use notify::event::{AccessKind, AccessMode, CreateKind, ModifyKind, RenameMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::WatcherConfig;
use crate::file_ops::{self, FileOpsError};
use crate::queue::{chrono_like_timestamp, EventoArquivo, QueuePublisher};

#[derive(Debug, thiserror::Error)]
enum ProcessamentoError {
    #[error(transparent)]
    Arquivo(#[from] FileOpsError),
    #[error("falha ao publicar evento e persistir contingência: {source}")]
    Publicacao {
        destino: PathBuf,
        #[source]
        source: crate::queue::QueueError,
    },
    #[error("evento aceito, mas falhou ao persistir recibo: {source}")]
    Recibo {
        destino: PathBuf,
        #[source]
        source: crate::queue::QueueError,
    },
}

pub struct EstadoCompartilhado {
    pub config: WatcherConfig,
    pub publisher: Arc<dyn QueuePublisher>,
    /// Arquivos atualmente "reivindicados" por uma task em andamento.
    /// Evita que o mesmo path seja processado duas vezes em paralelo
    /// quando listener e scanner detectam o mesmo arquivo quase ao
    /// mesmo tempo. Não substitui o `rename()` atômico — é uma otimização
    /// para não desperdiçar trabalho, a garantia real continua sendo o SO.
    pub claims: Mutex<HashSet<PathBuf>>,
    pub cancel_token: CancellationToken,
    pub limite_processamento: Arc<Semaphore>,
}

/// Estabelece o watch inotify (via crate `notify`, que usa inotify no
/// Linux) e encaminha eventos relevantes para um canal tokio.
///
/// O callback do `notify` roda em thread própria, fora do runtime tokio —
/// por isso usamos `blocking_send`, que aplica backpressure real: se o
/// canal estiver cheio, a thread do inotify bloqueia ali, atrasando a
/// entrega de novos eventos em vez de descartá-los. Um canal com
/// capacidade alta (4096) absorve rajadas sem chegar a esse ponto na
/// prática.
fn iniciar_watch_inotify(
    pasta: &Path,
) -> anyhow::Result<(RecommendedWatcher, mpsc::Receiver<PathBuf>)> {
    let (tx, rx) = mpsc::channel::<PathBuf>(4096);

    let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| match res {
        Ok(event) => {
            if let Some(path) = caminho_relevante(&event) {
                if let Err(e) = tx.blocking_send(path) {
                    error!(erro = %e, "canal de eventos fechado, evento de inotify perdido");
                }
            }
        }
        Err(e) => {
            error!(erro = %e, "erro reportado pelo backend inotify");
        }
    })?;

    watcher.watch(pasta, RecursiveMode::Recursive)?;
    info!(pasta = %pasta.display(), "watch inotify estabelecido");

    Ok((watcher, rx))
}

/// Filtra eventos brutos do inotify para os que realmente indicam
/// "pode haver um arquivo pronto":
///
/// - `CLOSE_WRITE` (Access::Close(Write)): o processo que escrevia fechou
///   o handle — sinal mais forte de "terminei de escrever" no Linux.
/// - Rename para dentro do diretório (`Modify::Name(RenameMode::To)`):
///   cobre o padrão do Windows Explorer / robocopy de escrever em nome
///   temporário e renomear ao final — exatamente o cenário que causava
///   o "arquivo sumiu" que você relatou.
/// - `Create(File)`: sinal fraco, mantido só para diagnóstico; a ação
///   real só acontece depois da checagem de estabilidade em `file_ops`.
///
/// Deliberadamente IGNORAMOS `Access::Open` e `Access::Read` — são ruído
/// puro para o nosso propósito e inflariam o volume de eventos à toa.
fn caminho_relevante(event: &Event) -> Option<PathBuf> {
    match &event.kind {
        EventKind::Access(AccessKind::Close(AccessMode::Write)) => event.paths.first().cloned(),
        EventKind::Modify(ModifyKind::Name(RenameMode::To)) => event.paths.first().cloned(),
        EventKind::Create(CreateKind::File) => event.paths.first().cloned(),
        _ => None,
    }
}

/// Task supervisionada do EventListener. Retorna `Err` (disparando
/// restart pelo supervisor) se o watch inotify cair ou o canal fechar
/// inesperadamente.
pub async fn rodar_listener(estado: Arc<EstadoCompartilhado>) -> anyhow::Result<()> {
    let (_watcher, mut rx) = iniciar_watch_inotify(&estado.config.pasta_entrada)?;

    loop {
        tokio::select! {
            _ = estado.cancel_token.cancelled() => {
                info!("listener de eventos cancelado via token de shutdown");
                return Ok(());
            }
            caminho_opt = rx.recv() => {
                match caminho_opt {
                    Some(path) => {
                        let estado = Arc::clone(&estado);
                        tokio::spawn(async move {
                            let Ok(_permit) = estado.limite_processamento.clone().acquire_owned().await else {
                                return;
                            };
                            tratar_evento(estado, path).await;
                        });
                    }
                    None => {
                        anyhow::bail!("canal de eventos do inotify fechou inesperadamente");
                    }
                }
            }
        }
    }
}

/// Scanner de reconciliação: rede de segurança, não caminho primário de
/// detecção. Cobre cenários em que o listener perdeu eventos (ex: o
/// processo reiniciou no meio de uma rajada de XMLs chegando, ou — em
/// deploys que monitoram filesystem de rede — o inotify simplesmente não
/// disparou por limitação do backend).
pub async fn rodar_scanner(estado: Arc<EstadoCompartilhado>) -> anyhow::Result<()> {
    loop {
        // `create_dir` é atômico também em filesystems compartilhados usuais.
        // O lease obsoleto permite recuperação depois de crash do líder.
        let Some(_lock_scanner) = tentar_adquirir_lock_scanner(&estado.config).await? else {
            debug!("scanner não executado: outra instância mantém o lock");
            tokio::select! {
                _ = tokio::time::sleep(estado.config.intervalo_scanner_reconciliacao) => {}
                _ = estado.cancel_token.cancelled() => return Ok(()),
            }
            continue;
        };

        let pasta_entrada = estado.config.pasta_entrada.clone();

        let entradas_resultado = tokio::task::spawn_blocking(move || {
            let mut relevantes = Vec::new();
            // max_depth(3) atende à exigência de subpastas sem ir fundo demais
            for entry in walkdir::WalkDir::new(&pasta_entrada)
                .max_depth(3)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    relevantes.push(entry.path().to_path_buf());
                }
            }
            relevantes
        })
        .await;

        let mut candidatos = 0u32;
        if let Ok(entradas) = entradas_resultado {
            // OTIMIZAÇÃO: Copia o HashSet de claims apenas uma vez por ciclo para evitar lock contention.
            let claims_copia = {
                let claims = estado.claims.lock().await;
                claims.clone()
            };

            for path in entradas {
                if claims_copia.contains(&path) {
                    continue;
                }

                candidatos += 1;
                let estado_clone = Arc::clone(&estado);
                tokio::spawn(async move {
                    let Ok(_permit) = estado_clone
                        .limite_processamento
                        .clone()
                        .acquire_owned()
                        .await
                    else {
                        return;
                    };
                    tratar_evento(estado_clone, path).await;
                });
            }
        } else {
            warn!("scanner: falha ao listar pasta de entrada com walkdir");
        }

        debug!(candidatos, "ciclo de reconciliação do scanner concluído");

        tokio::select! {
            _ = tokio::time::sleep(estado.config.intervalo_scanner_reconciliacao) => {}
            _ = estado.cancel_token.cancelled() => {
                info!("scanner de reconciliação cancelado via token de shutdown");
                return Ok(());
            }
        }
    }
}

struct LockScanner {
    path: PathBuf,
}

impl Drop for LockScanner {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.path) {
            warn!(path = %self.path.display(), erro = %e, "falha ao liberar lock do scanner");
        }
    }
}

async fn tentar_adquirir_lock_scanner(
    config: &WatcherConfig,
) -> anyhow::Result<Option<LockScanner>> {
    let path = &config.pasta_lock_scanner;
    match tokio::fs::create_dir(path).await {
        Ok(()) => {
            tokio::fs::write(path.join("owner"), &config.worker_id).await?;
            return Ok(Some(LockScanner { path: path.clone() }));
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }

    let max_idade = config.intervalo_scanner_reconciliacao.saturating_mul(3);
    let obsoleto = tokio::fs::metadata(path)
        .await
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|mtime| mtime.elapsed().ok())
        .is_some_and(|idade| idade > max_idade);
    if !obsoleto {
        return Ok(None);
    }

    warn!(path = %path.display(), "removendo lock obsoleto do scanner");
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => tentar_criar_lock_scanner(config).await,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tentar_criar_lock_scanner(config).await
        }
        Err(e) => Err(e.into()),
    }
}

async fn tentar_criar_lock_scanner(config: &WatcherConfig) -> anyhow::Result<Option<LockScanner>> {
    match tokio::fs::create_dir(&config.pasta_lock_scanner).await {
        Ok(()) => {
            tokio::fs::write(config.pasta_lock_scanner.join("owner"), &config.worker_id).await?;
            Ok(Some(LockScanner {
                path: config.pasta_lock_scanner.clone(),
            }))
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Heartbeat: em produção isso publicaria num endpoint/serviço central de
/// monitoramento (ou exporia métrica Prometheus consumida por um
/// Alertmanager). Aqui registramos via log estruturado para manter o
/// exemplo autocontido — a estrutura do dado é o que importa replicar.
async fn contar_eventos_contingencia(pasta_contingencia: &Path) -> usize {
    let mut total = 0;
    for nome in &["eventos_pendentes.jsonl", "eventos_pendentes.jsonl.working"] {
        let path = pasta_contingencia.join(nome);
        if path.exists() {
            if let Ok(file) = tokio::fs::File::open(&path).await {
                use tokio::io::AsyncBufReadExt;
                let reader = tokio::io::BufReader::new(file);
                let mut lines = reader.lines();
                while let Ok(Some(_)) = lines.next_line().await {
                    total += 1;
                }
            }
        }
    }
    total
}

pub async fn rodar_heartbeat(estado: Arc<EstadoCompartilhado>) -> anyhow::Result<()> {
    let iniciado_em = chrono_like_timestamp::AgoraIso8601::agora();
    loop {
        let em_andamento = estado.claims.lock().await.len();
        let backlog_contingencia =
            contar_eventos_contingencia(&estado.config.pasta_buffer_contingencia).await;

        let backlog_alert = em_andamento > 100; // Flag indicando gargalo crítico

        info!(
            heartbeat = true,
            tenant_id = %estado.config.tenant_id,
            worker_id = %estado.config.worker_id,
            arquivos_em_processamento = em_andamento,
            backlog_critico = backlog_alert,
            backlog_contingencia = backlog_contingencia,
            iniciado_em = iniciado_em.0,
            "telemetria do watcher ativa"
        );

        tokio::select! {
            _ = tokio::time::sleep(estado.config.intervalo_heartbeat) => {}
            _ = estado.cancel_token.cancelled() => {
                info!("heartbeat cancelado via token de shutdown");
                return Ok(());
            }
        }
    }
}

pub async fn rodar_http_observabilidade(estado: Arc<EstadoCompartilhado>) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind(estado.config.observabilidade_addr).await?;
    info!(addr = %estado.config.observabilidade_addr, "endpoint de observabilidade ativo");

    loop {
        let (mut stream, _) = tokio::select! {
            resultado = listener.accept() => resultado?,
            _ = estado.cancel_token.cancelled() => return Ok(()),
        };
        let estado = Arc::clone(&estado);
        tokio::spawn(async move {
            let mut buffer = [0u8; 2048];
            let Ok(lidos) = stream.read(&mut buffer).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buffer[..lidos]);
            let path = request.split_whitespace().nth(1).unwrap_or("/");
            let (status, content_type, body) = resposta_observabilidade(&estado, path).await;
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

async fn resposta_observabilidade(
    estado: &EstadoCompartilhado,
    path: &str,
) -> (&'static str, &'static str, String) {
    match path {
        "/health" => (
            "200 OK",
            "application/json",
            "{\"status\":\"ok\"}\n".to_string(),
        ),
        "/ready" => {
            match tokio::time::timeout(estado.config.rabbitmq_timeout, estado.publisher.pronto())
                .await
            {
                Ok(Ok(())) => (
                    "200 OK",
                    "application/json",
                    "{\"status\":\"ready\"}\n".to_string(),
                ),
                _ => (
                    "503 Service Unavailable",
                    "application/json",
                    "{\"status\":\"not_ready\",\"dependency\":\"rabbitmq\"}\n".to_string(),
                ),
            }
        }
        "/metrics" => {
            let claims = estado.claims.lock().await.len();
            let limite = estado.config.max_processamentos_concorrentes;
            let ativos = limite.saturating_sub(estado.limite_processamento.available_permits());
            let contingencia =
                contar_eventos_contingencia(&estado.config.pasta_buffer_contingencia).await;
            let body = format!(
                "# TYPE ingestorx_claims gauge\ningestorx_claims {claims}\n\
# TYPE ingestorx_contingencia_eventos gauge\ningestorx_contingencia_eventos {contingencia}\n\
# TYPE ingestorx_processamentos_ativos gauge\ningestorx_processamentos_ativos {ativos}\n\
# TYPE ingestorx_processamentos_limite gauge\ningestorx_processamentos_limite {limite}\n"
            );
            ("200 OK", "text/plain; version=0.0.4", body)
        }
        _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
    }
}

#[tracing::instrument(skip(estado), fields(correlation_id = %uuid::Uuid::new_v4()))]
async fn tratar_evento(estado: Arc<EstadoCompartilhado>, path: PathBuf) {
    if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
        if filename.contains("Zone.Identifier") {
            debug!(path = %path.display(), "detectado arquivo Zone.Identifier, excluindo imediatamente");
            if let Err(e) = tokio::fs::remove_file(&path).await {
                warn!(path = %path.display(), erro = %e, "falha ao excluir arquivo Zone.Identifier");
            }
            return;
        }
    }

    {
        let mut claims = estado.claims.lock().await;
        if claims.contains(&path) {
            debug!(path = %path.display(), "evento duplicado para arquivo já em processamento, ignorando");
            return;
        }
        claims.insert(path.clone());
    }

    info!(path = %path.display(), "novo arquivo localizado para processamento");

    let resultado = processar_arquivo(&estado, &path).await;

    estado.claims.lock().await.remove(&path);

    if let Err(e) = resultado {
        tratar_erro_processamento(&estado, &path, e).await;
    }
}

async fn processar_arquivo(
    estado: &EstadoCompartilhado,
    path: &Path,
) -> Result<(), ProcessamentoError> {
    let cfg = &estado.config;

    let arquivo_estavel = file_ops::aguardar_estabilidade(
        path,
        cfg.leituras_estaveis_necessarias,
        cfg.intervalo_checagem_estabilidade,
        cfg.tamanho_maximo_bytes,
    )
    .await?;

    let ext = arquivo_estavel
        .path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    if !estado.config.extensoes_permitidas.contains(&ext) {
        return Err(FileOpsError::ExtensaoInvalida.into());
    }

    info!(
        path = %arquivo_estavel.path.display(),
        hash = %arquivo_estavel.hash_sha256,
        tamanho_bytes = arquivo_estavel.tamanho_bytes,
        "arquivo confirmado estável e com extensão válida"
    );

    let destino = file_ops::mover_atomico_com_retry(
        &arquivo_estavel,
        &cfg.pasta_processando,
        cfg.max_tentativas_retry,
    )
    .await?;

    info!(
        destino = %destino.display(),
        hash = %arquivo_estavel.hash_sha256,
        "arquivo movido atomicamente para pasta de processamento"
    );

    let evento = EventoArquivo::novo(
        arquivo_estavel.hash_sha256.clone(),
        destino.clone(),
        arquivo_estavel.tamanho_bytes,
        &cfg.tenant_id,
    );

    // A publicação nunca deve "perder" o evento — a implementação real
    // (PublisherComContingencia, em queue.rs) garante isso gravando em
    // buffer local durável caso o broker esteja inacessível.
    estado
        .publisher
        .publicar(&evento)
        .await
        .map_err(|source| ProcessamentoError::Publicacao {
            destino: destino.clone(),
            source,
        })?;
    registrar_recibo_publicacao(&estado.config, &evento)
        .await
        .map_err(|source| ProcessamentoError::Recibo { destino, source })?;

    Ok(())
}

fn caminho_recibo(config: &WatcherConfig, arquivo: &Path) -> Option<PathBuf> {
    arquivo.file_name().map(|nome| {
        config
            .pasta_recibos_publicacao
            .join(format!("{}.json", nome.to_string_lossy()))
    })
}

async fn registrar_recibo_publicacao(
    config: &WatcherConfig,
    evento: &EventoArquivo,
) -> Result<(), crate::queue::QueueError> {
    use tokio::io::AsyncWriteExt;

    let destino = caminho_recibo(config, &evento.path_absoluto).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "arquivo sem nome para recibo",
        )
    })?;
    let temporario = destino.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    let bytes = serde_json::to_vec(evento)?;
    let mut arquivo = tokio::fs::File::create(&temporario).await?;
    arquivo.write_all(&bytes).await?;
    arquivo.sync_all().await?;
    tokio::fs::rename(&temporario, &destino).await?;

    let pasta = config.pasta_recibos_publicacao.clone();
    tokio::task::spawn_blocking(move || std::fs::File::open(pasta)?.sync_all())
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))??;
    Ok(())
}

/// Recupera arquivos que foram movidos antes de um crash, mas não possuem
/// recibo de publicação/contingência. Deve rodar antes do listener/scanner.
pub async fn recuperar_arquivos_processando(estado: &EstadoCompartilhado) -> anyhow::Result<usize> {
    let mut recuperados = 0usize;
    let mut entradas = tokio::fs::read_dir(&estado.config.pasta_processando).await?;

    while let Some(entrada) = entradas.next_entry().await? {
        let path = entrada.path();
        if !entrada.file_type().await?.is_file() {
            continue;
        }
        let Some(recibo) = caminho_recibo(&estado.config, &path) else {
            continue;
        };
        if tokio::fs::try_exists(&recibo).await? {
            continue;
        }

        let arquivo = file_ops::aguardar_estabilidade(
            &path,
            estado.config.leituras_estaveis_necessarias,
            estado.config.intervalo_checagem_estabilidade,
            estado.config.tamanho_maximo_bytes,
        )
        .await?;
        let evento = EventoArquivo::novo(
            arquivo.hash_sha256,
            path.clone(),
            arquivo.tamanho_bytes,
            &estado.config.tenant_id,
        );
        estado.publisher.publicar(&evento).await?;
        registrar_recibo_publicacao(&estado.config, &evento).await?;
        recuperados += 1;
        warn!(path = %path.display(), "arquivo órfão recuperado e republicado");
    }

    limpar_recibos_obsoletos(&estado.config).await?;
    Ok(recuperados)
}

async fn limpar_recibos_obsoletos(config: &WatcherConfig) -> anyhow::Result<()> {
    let mut recibos = tokio::fs::read_dir(&config.pasta_recibos_publicacao).await?;
    while let Some(entrada) = recibos.next_entry().await? {
        let recibo = entrada.path();
        if !entrada.file_type().await?.is_file() {
            continue;
        }
        let Some(nome_recibo) = recibo.file_name().and_then(|nome| nome.to_str()) else {
            continue;
        };
        let Some(nome_arquivo) = nome_recibo.strip_suffix(".json") else {
            continue;
        };
        if !tokio::fs::try_exists(config.pasta_processando.join(nome_arquivo)).await? {
            tokio::fs::remove_file(recibo).await?;
        }
    }
    Ok(())
}

async fn tratar_erro_processamento(
    estado: &EstadoCompartilhado,
    path: &Path,
    erro: ProcessamentoError,
) {
    match erro {
        ProcessamentoError::Recibo { destino, source } => {
            error!(
                path = %destino.display(),
                erro = %source,
                "evento aceito, mas recibo falhou; arquivo será reconciliado no próximo startup"
            );
        }
        ProcessamentoError::Publicacao { destino, source } => {
            error!(
                path = %destino.display(),
                erro = %source,
                "publicação e contingência falharam; movendo arquivo para erro"
            );
            mover_para_erro_best_effort(estado, &destino).await;
        }
        ProcessamentoError::Arquivo(FileOpsError::ArquivoDesapareceu(_)) => {
            // RUÍDO ESPERADO, não falha real: muito provavelmente era um
            // arquivo temporário do Windows que foi renomeado. O evento
            // do arquivo final chega separadamente (MOVED_TO / CLOSE_WRITE)
            // e será tratado normalmente quando chegar.
            debug!(
                path = %path.display(),
                "arquivo desapareceu antes da checagem de estabilidade (provável temporário renomeado), ignorando"
            );
        }
        ProcessamentoError::Arquivo(FileOpsError::TamanhoExcedido { tamanho, limite }) => {
            warn!(
                path = %path.display(),
                tamanho,
                limite,
                "arquivo excede tamanho máximo, movendo para pasta de erro"
            );
            mover_para_erro_best_effort(estado, path).await;
        }
        ProcessamentoError::Arquivo(FileOpsError::RetriesEsgotados { .. }) => {
            error!(
                path = %path.display(),
                erro = %erro,
                "esgotadas tentativas de mover arquivo, movendo para pasta de erro para investigação manual"
            );
            mover_para_erro_best_effort(estado, path).await;
        }
        ProcessamentoError::Arquivo(FileOpsError::Io(_)) => {
            // Falha transitória de IO durante a checagem de estabilidade.
            // Não movemos para erro aqui de propósito: o scanner de
            // reconciliação vai encontrar esse arquivo de novo no próximo
            // ciclo e tentar novamente, já que ele continua na pasta de
            // entrada intocado.
            warn!(
                path = %path.display(),
                erro = %erro,
                "falha de IO transitória, arquivo permanece na entrada para nova tentativa pelo scanner"
            );
        }
        ProcessamentoError::Arquivo(FileOpsError::TamanhoZero) => {
            warn!(
                path = %path.display(),
                "arquivo rejeitado pois possui 0 bytes, movendo para pasta de ignorados"
            );
            mover_para_ignorados_best_effort(estado, path).await;
        }
        ProcessamentoError::Arquivo(FileOpsError::ExtensaoInvalida) => {
            warn!(
                path = %path.display(),
                "arquivo rejeitado por ter extensão não permitida, movendo para ignorados"
            );
            mover_para_ignorados_best_effort(estado, path).await;
        }
    }
}

async fn mover_para_erro_best_effort(estado: &EstadoCompartilhado, path: &Path) {
    if let Some(nome) = path.file_name() {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let nome_destino = format!("{}_{}", ts, nome.to_string_lossy());
        let destino = estado.config.pasta_erro.join(nome_destino);
        if let Err(e) = tokio::fs::rename(path, &destino).await {
            error!(
                path = %path.display(),
                destino = %destino.display(),
                erro = %e,
                "falha ao mover arquivo problemático para pasta de erro — intervenção manual necessária"
            );
        }
    }
}

async fn mover_para_ignorados_best_effort(estado: &EstadoCompartilhado, path: &Path) {
    if let Some(nome) = path.file_name() {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let nome_destino = format!("{}_{}", ts, nome.to_string_lossy());
        let destino = estado.config.pasta_ignorados.join(nome_destino);
        if let Err(e) = tokio::fs::rename(path, &destino).await {
            warn!(
                path = %path.display(),
                destino = %destino.display(),
                erro = %e,
                "falha ao mover arquivo para pasta de ignorados"
            );
        }
    }
}

pub async fn rodar_drenador_contingencia(estado: Arc<EstadoCompartilhado>) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
            _ = estado.cancel_token.cancelled() => {
                info!("drenador de contingência cancelado via token de shutdown");
                return Ok(());
            }
        }

        if let Err(e) = estado.publisher.drenar().await {
            warn!(
                erro = %e,
                "falha ao drenar buffer de contingência local — tentará novamente no próximo ciclo"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct PublisherContador(AtomicUsize);

    #[async_trait]
    impl QueuePublisher for PublisherContador {
        async fn publicar(&self, _: &EventoArquivo) -> Result<(), crate::queue::QueueError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn drenar(&self) -> Result<(), crate::queue::QueueError> {
            Ok(())
        }
    }

    fn config_teste(base: &Path) -> WatcherConfig {
        WatcherConfig {
            pasta_entrada: base.join("entrada"),
            pasta_processando: base.join("processando/worker_teste"),
            pasta_processado: base.join("processado"),
            pasta_erro: base.join("erro"),
            pasta_ignorados: base.join("ignorados"),
            pasta_logs: base.join("logs"),
            pasta_buffer_contingencia: base.join("buffer"),
            pasta_recibos_publicacao: base.join("recibos/worker_teste"),
            tamanho_maximo_bytes: 1024,
            intervalo_checagem_estabilidade: std::time::Duration::from_millis(1),
            leituras_estaveis_necessarias: 2,
            intervalo_scanner_reconciliacao: std::time::Duration::from_millis(10),
            intervalo_heartbeat: std::time::Duration::from_secs(1),
            max_tentativas_retry: 1,
            tenant_id: "tenant_teste".into(),
            worker_id: "worker_teste".into(),
            extensoes_permitidas: vec!["xml".into()],
            rabbitmq_url: "amqp://localhost".into(),
            rabbitmq_fila: "teste".into(),
            rabbitmq_timeout: std::time::Duration::from_secs(1),
            rabbitmq_ca_cert: None,
            observabilidade_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            pasta_lock_scanner: base.join(".scanner.lock"),
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
    async fn recupera_orfao_uma_unica_vez_gracas_ao_recibo() {
        let base =
            std::env::temp_dir().join(format!("ingestorx_recovery_{}", uuid::Uuid::new_v4()));
        let config = config_teste(&base);
        config.garantir_estrutura_pastas().unwrap();
        let orfao = config.pasta_processando.join("arquivo.xml");
        tokio::fs::write(&orfao, b"conteudo").await.unwrap();
        let publisher = Arc::new(PublisherContador(AtomicUsize::new(0)));
        let estado = EstadoCompartilhado {
            config,
            publisher: publisher.clone(),
            claims: Mutex::new(HashSet::new()),
            cancel_token: CancellationToken::new(),
            limite_processamento: Arc::new(Semaphore::new(4)),
        };

        assert_eq!(recuperar_arquivos_processando(&estado).await.unwrap(), 1);
        assert_eq!(recuperar_arquivos_processando(&estado).await.unwrap(), 0);
        assert_eq!(publisher.0.load(Ordering::SeqCst), 1);
        tokio::fs::remove_dir_all(base).await.unwrap();
    }

    #[tokio::test]
    async fn lock_do_scanner_e_exclusivo() {
        let base = std::env::temp_dir().join(format!("ingestorx_lock_{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&base).await.unwrap();
        let config = config_teste(&base);
        let primeiro = tentar_adquirir_lock_scanner(&config).await.unwrap();
        assert!(primeiro.is_some());
        assert!(tentar_adquirir_lock_scanner(&config)
            .await
            .unwrap()
            .is_none());
        drop(primeiro);
        assert!(tentar_adquirir_lock_scanner(&config)
            .await
            .unwrap()
            .is_some());
        tokio::fs::remove_dir_all(base).await.unwrap();
    }

    #[tokio::test]
    async fn health_e_metricas_expoem_estado() {
        let base = std::env::temp_dir().join(format!("ingestorx_http_{}", uuid::Uuid::new_v4()));
        let config = config_teste(&base);
        config.garantir_estrutura_pastas().unwrap();
        let estado = EstadoCompartilhado {
            config,
            publisher: Arc::new(PublisherContador(AtomicUsize::new(0))),
            claims: Mutex::new(HashSet::from([PathBuf::from("arquivo.xml")])),
            cancel_token: CancellationToken::new(),
            limite_processamento: Arc::new(Semaphore::new(4)),
        };

        let (status, _, health) = resposta_observabilidade(&estado, "/health").await;
        assert_eq!(status, "200 OK");
        assert!(health.contains("\"status\":\"ok\""));
        let (_, _, metrics) = resposta_observabilidade(&estado, "/metrics").await;
        assert!(metrics.contains("ingestorx_claims 1"));
        tokio::fs::remove_dir_all(base).await.unwrap();
    }
}

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};

use xml_watcher::config::WatcherConfig;
use xml_watcher::queue::{PublisherComContingencia, QueuePublisher, RabbitMqPublisher};
use xml_watcher::watcher::EstadoCompartilhado;
use xml_watcher::{logging, supervisor, watcher};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let config = WatcherConfig::from_env_or_default()?;
    config.validar()?;
    config.garantir_estrutura_pastas()?;
    config.validar_diretorios_gravaveis()?;

    // A guarda do logging precisa ficar viva até o fim do `main`. Quando
    // ela é dropada (ao sair do escopo, inclusive em caso de erro), o
    // writer não-bloqueante faz o flush final dos logs em buffer.
    let _logging_guard = logging::iniciar_logging(&config.pasta_logs)?;

    info!(
        pasta_entrada = %config.pasta_entrada.display(),
        "iniciando xml_watcher"
    );

    let rabbitmq_publisher = RabbitMqPublisher::new(
        &config.rabbitmq_url,
        &config.rabbitmq_fila,
        config.rabbitmq_timeout,
        config.rabbitmq_ca_cert.as_deref(),
    )
    .expect("Falha ao inicializar cliente RabbitMQ — verifique a URL no .env");

    let publisher: Arc<dyn QueuePublisher> = Arc::new(PublisherComContingencia::new(
        rabbitmq_publisher,
        &config.pasta_buffer_contingencia,
    ));

    let cancel_token = tokio_util::sync::CancellationToken::new();

    let estado = Arc::new(EstadoCompartilhado {
        config: config.clone(),
        publisher,
        claims: Mutex::new(std::collections::HashSet::new()),
        cancel_token: cancel_token.clone(),
        limite_processamento: Arc::new(tokio::sync::Semaphore::new(
            config.max_processamentos_concorrentes,
        )),
    });

    let recuperados = watcher::recuperar_arquivos_processando(&estado).await?;
    info!(
        recuperados,
        "reconciliação inicial de arquivos em processamento concluída"
    );

    // Cada task crítica roda sob supervisão: se cair (erro ou panic), é
    // reiniciada automaticamente com backoff, e o reinício é logado como
    // evento de alta severidade para acionar alerta no monitoramento real.
    let cancel_listener = cancel_token.clone();
    let estado_listener = Arc::clone(&estado);
    tokio::spawn(supervisor::supervisionar(
        "event_listener",
        cancel_listener,
        move || {
            let estado = Arc::clone(&estado_listener);
            async move { watcher::rodar_listener(estado).await }
        },
    ));

    let cancel_scanner = cancel_token.clone();
    let estado_scanner = Arc::clone(&estado);
    tokio::spawn(supervisor::supervisionar(
        "scanner_reconciliacao",
        cancel_scanner,
        move || {
            let estado = Arc::clone(&estado_scanner);
            async move { watcher::rodar_scanner(estado).await }
        },
    ));

    let cancel_heartbeat = cancel_token.clone();
    let estado_heartbeat = Arc::clone(&estado);
    tokio::spawn(supervisor::supervisionar(
        "heartbeat",
        cancel_heartbeat,
        move || {
            let estado = Arc::clone(&estado_heartbeat);
            async move { watcher::rodar_heartbeat(estado).await }
        },
    ));

    let cancel_drainer = cancel_token.clone();
    let estado_drainer = Arc::clone(&estado);
    tokio::spawn(supervisor::supervisionar(
        "contingency_drainer",
        cancel_drainer,
        move || {
            let estado = Arc::clone(&estado_drainer);
            async move { watcher::rodar_drenador_contingencia(estado).await }
        },
    ));

    let cancel_http = cancel_token.clone();
    let estado_http = Arc::clone(&estado);
    tokio::spawn(supervisor::supervisionar(
        "http_observabilidade",
        cancel_http,
        move || {
            let estado = Arc::clone(&estado_http);
            async move { watcher::rodar_http_observabilidade(estado).await }
        },
    ));

    info!("xml_watcher operacional — aguardando sinal de encerramento (Ctrl+C / SIGTERM)");

    aguardar_sinal_encerramento().await;

    info!("sinal de encerramento recebido, iniciando shutdown gracioso");
    cancel_token.cancel();

    drenar_processamento_em_andamento(&estado, Duration::from_secs(30)).await;

    info!("shutdown concluído");
    Ok(())
}

/// Aguarda SIGINT (Ctrl+C) ou SIGTERM (enviado por systemd/Kubernetes em
/// um `stop`/rolling deploy). Sem tratar isso explicitamente, o processo
/// morre abruptamente em qualquer redeploy, arriscando perder eventos que
/// estavam em trânsito entre a detecção e a publicação na fila.
async fn aguardar_sinal_encerramento() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("falha ao registrar SIGTERM");
        let mut sigint = signal(SignalKind::interrupt()).expect("falha ao registrar SIGINT");

        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Não derruba o processo enquanto houver arquivos com claim ativo (ou
/// seja, em algum ponto entre detecção e publicação confirmada). Espera
/// até um timeout máximo — passado esse limite, registra o que ficou
/// pendente (para investigação) e segue com o shutdown, já que travar o
/// redeploy indefinidamente também não é aceitável em produção.
async fn drenar_processamento_em_andamento(estado: &EstadoCompartilhado, timeout: Duration) {
    let inicio = tokio::time::Instant::now();

    loop {
        let pendentes = estado.claims.lock().await.len();
        if pendentes == 0 {
            info!("nenhum arquivo em processamento, shutdown pode prosseguir imediatamente");
            return;
        }

        if inicio.elapsed() >= timeout {
            warn!(
                pendentes,
                "timeout de drenagem atingido com arquivos ainda em processamento — esses arquivos serão retomados pelo scanner de reconciliação após o restart"
            );
            return;
        }

        info!(
            pendentes,
            "aguardando drenagem de arquivos em processamento antes de encerrar"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

//! Logging estruturado e DURÁVEL.
//!
//! "Log que vai até o fim" significa, na prática, três garantias:
//!
//! 1. Nunca usar `println!`/`eprintln!` — eles não têm nível, não têm
//!    contexto estruturado, e não sobrevivem a um simples redirecionamento
//!    de stdout perdido em produção. Usamos `tracing`, que é o padrão de
//!    fato em Rust para logging estruturado + tracing distribuído.
//!
//! 2. O log é gravado em DISCO (arquivo rotacionado por dia) além de
//!    stdout. Se o coletor central (Loki/ELK) cair, você ainda tem o
//!    histórico local para investigar incidentes.
//!
//! 3. Em caso de PANIC ou SHUTDOWN, o buffer do logger é explicitamente
//!    drenado (flush) antes do processo morrer. Sem isso, logs em buffer
//!    de um writer não-bloqueante podem ser perdidos justamente no
//!    momento mais importante: quando algo deu errado.
//!
//! O que NÃO é coberto (e é importante saber): um `SIGKILL` ou queda
//! abrupta de energia ainda pode perder o que está no buffer do SO/disco.
//! Para isso, a defesa é o LogUploader (fora de escopo aqui) gravando em
//! near-real-time num coletor remoto, não só local.

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Guarda que precisa ficar viva durante toda a execução do programa.
/// Quando ela é dropada (fim do `main`), o writer não-bloqueante faz o
/// flush final pendente. Por isso `main` deve manter essa variável viva
/// até o último `await`, e fazer drop explícito só depois do shutdown.
pub struct LoggingGuard {
    _file_guard: WorkerGuard,
}

pub fn iniciar_logging(pasta_logs: &std::path::Path) -> anyhow::Result<LoggingGuard> {
    // Rotação diária — evita arquivo único crescendo sem limite e facilita
    // expurgo/retenção (ex: manter só 90 dias por política de LGPD/auditoria).
    let file_appender = tracing_appender::rolling::daily(pasta_logs, "xml_watcher.log");
    let (non_blocking_file, file_guard) = tracing_appender::non_blocking(file_appender);

    // Filtro de nível via env var (RUST_LOG=info,xml_watcher=debug),
    // com default sensato caso não seja definida.
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,xml_watcher=debug"));

    // Camada 1: arquivo em disco, formato JSON — pronto para ingestão por
    // Loki/ELK/qualquer coletor estruturado, sem regex frágil de parsing.
    let camada_arquivo = fmt::layer()
        .json()
        .with_writer(non_blocking_file)
        .with_target(true)
        .with_current_span(true)
        .with_span_list(true);

    // Camada 2: stdout legível por humano — útil para `journalctl`/console
    // durante operação manual e debug local.
    let camada_console = fmt::layer().with_target(false).with_writer(std::io::stdout);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(camada_arquivo)
        .with(camada_console)
        .init();

    // BOA PRÁTICA CRÍTICA: panics não tratados em tasks do tokio não
    // derrubam o processo principal silenciosamente — mas também não
    // aparecem no nosso log estruturado por padrão, só no stderr cru do
    // Rust. Substituímos o panic hook para registrar o panic via tracing
    // ANTES de delegar ao comportamento padrão, garantindo rastreabilidade
    // mesmo de falhas inesperadas.
    let hook_padrao = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!(
            panic.message = %info,
            panic.location = info.location().map(|l| l.to_string()).unwrap_or_default(),
            "PANIC detectado em uma task do watcher"
        );
        hook_padrao(info);
    }));

    Ok(LoggingGuard {
        _file_guard: file_guard,
    })
}

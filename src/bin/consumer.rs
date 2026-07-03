use tracing::info;
use xml_watcher::config::WatcherConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let config = WatcherConfig::from_env_or_default()?;
    config.validar()?;
    config.garantir_estrutura_pastas()?;
    config.validar_diretorios_gravaveis()?;
    let _logging_guard = xml_watcher::logging::iniciar_logging(&config.pasta_logs)?;
    let cancel = tokio_util::sync::CancellationToken::new();
    let executar = xml_watcher::consumer::executar(config, cancel.clone());
    tokio::pin!(executar);

    tokio::select! {
        resultado = &mut executar => resultado?,
        _ = tokio::signal::ctrl_c() => {
            info!("encerrando consumidor downstream");
            cancel.cancel();
            executar.await?;
        }
    }
    Ok(())
}

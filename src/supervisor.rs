//! Supervisor de tasks — o que falta no padrão `tokio::spawn(fn())` solto.
//!
//! Sem isso, se uma task crítica (listener, scanner, heartbeat) sofrer um
//! panic, ela simplesmente desaparece. O resto do processo continua de
//! pé, então nada acusa o problema externamente (o processo "está vivo"),
//! mas o watcher parou de monitorar a pasta. Esse é um dos jeitos mais
//! perigosos de um sistema falhar: silenciosamente.
//!
//! O padrão aqui é inspirado em supervision trees (Erlang/OTP): a task é
//! reiniciada automaticamente com backoff exponencial, e cada reinício é
//! logado como evento de alta severidade — porque reinício automático
//! evita downtime total, mas NÃO substitui investigar a causa raiz.

use std::future::Future;
use std::time::Duration;
use tracing::{error, warn};

pub async fn supervisionar<F, Fut>(
    nome_tarefa: &'static str,
    cancel_token: tokio_util::sync::CancellationToken,
    mut tarefa_factory: F,
) -> ()
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let mut tentativa: u32 = 0;

    loop {
        if cancel_token.is_cancelled() {
            return;
        }

        tentativa += 1;
        let handle = tokio::spawn(tarefa_factory());

        tokio::select! {
            res = handle => {
                match res {
                    Ok(Ok(())) => {
                        if cancel_token.is_cancelled() {
                            return;
                        }
                        warn!(
                            tarefa = nome_tarefa,
                            "task encerrou retornando sucesso, o que não é esperado para uma task de loop contínuo — reiniciando"
                        );
                    }
                    Ok(Err(e)) => {
                        if cancel_token.is_cancelled() {
                            return;
                        }
                        error!(
                            tarefa = nome_tarefa,
                            erro = %e,
                            tentativa,
                            "task retornou erro, reiniciando com backoff"
                        );
                    }
                    Err(join_error) => {
                        if cancel_token.is_cancelled() {
                            return;
                        }
                        error!(
                            tarefa = nome_tarefa,
                            panic = %join_error,
                            tentativa,
                            "task sofreu PANIC, reiniciando com backoff — investigar causa raiz com prioridade"
                        );
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                return;
            }
        }

        let backoff_ms = 500u64.saturating_mul(2u64.saturating_pow(tentativa.min(6)));
        let espera = Duration::from_millis(backoff_ms.min(30_000));

        tokio::select! {
            _ = tokio::time::sleep(espera) => {}
            _ = cancel_token.cancelled() => {
                return;
            }
        }
    }
}

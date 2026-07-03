//! Operações de arquivo — o coração da proteção contra o problema que você
//! relatou (XML "sumindo" porque o watcher tentou agir rápido demais
//! enquanto o Windows ainda copiava).
//!
//! Três garantias implementadas aqui:
//!
//! 1. ESTABILIDADE: nunca tocamos um arquivo só porque um evento disparou.
//!    Confirmamos tamanho + mtime idênticos em leituras sucessivas antes
//!    de qualquer ação.
//!
//! 2. RE-VALIDAÇÃO: se o arquivo sumiu entre a detecção e a ação (caso
//!    clássico do Windows Explorer escrevendo em nome temporário e
//!    renomeando ao final), tratamos isso como RUÍDO ESPERADO, não erro.
//!    O evento do arquivo final (MOVED_TO/CLOSE_WRITE) chega separado.
//!
//! 3. MOVE ATÔMICO: usamos `rename()`, que no mesmo filesystem é uma
//!    operação atômica do SO — nunca existe um instante em que o arquivo
//!    está "meio movido". Isso também resolve a corrida entre o
//!    EventListener e o Scanner de reconciliação: quem conseguir o
//!    rename primeiro "ganhou a posse" do arquivo.

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, instrument, warn};

#[derive(Debug, Error)]
pub enum FileOpsError {
    #[error(
        "arquivo desapareceu durante checagem de estabilidade (provável rename de temporário): {0}"
    )]
    ArquivoDesapareceu(PathBuf),

    #[error("arquivo excede tamanho máximo permitido ({tamanho} bytes, limite {limite})")]
    TamanhoExcedido { tamanho: u64, limite: u64 },

    #[error("arquivo vazio (0 bytes) detectado e rejeitado")]
    TamanhoZero,

    #[error("extensão do arquivo não permitida")]
    ExtensaoInvalida,

    #[error("falha de IO: {0}")]
    Io(#[from] std::io::Error),

    #[error("esgotadas {tentativas} tentativas de mover arquivo: {ultimo_erro}")]
    RetriesEsgotados {
        tentativas: u32,
        ultimo_erro: String,
    },
}

/// Resultado de um arquivo confirmado como estável e pronto para mover.
#[derive(Debug, Clone)]
pub struct ArquivoEstavel {
    pub path: PathBuf,
    pub tamanho_bytes: u64,
    /// SHA-256 do conteúdo. Serve para dois propósitos:
    /// - Deduplicação real (não por nome de arquivo, que pode colidir
    ///   entre clientes diferentes).
    /// - Correlation ID: o mesmo hash acompanha o arquivo em todos os
    ///   logs e na mensagem da fila, permitindo rastrear um XML específico
    ///   do início ao fim do pipeline, inclusive em outros serviços.
    pub hash_sha256: String,
}

/// Aguarda o arquivo ficar "estável": N leituras sucessivas de tamanho e
/// mtime idênticos. Isso é o que evita agir sobre um arquivo que o SO
/// ainda está recebendo via cópia de rede (SMB/CIFS), cenário em que o
/// `CLOSE_WRITE` do inotify pode até já ter disparado do lado do servidor
/// mas o conteúdo ainda não está logicamente completo do ponto de vista
/// da aplicação de origem.
#[instrument(skip(intervalo), fields(path = %path.display()))]
pub async fn aguardar_estabilidade(
    path: &Path,
    leituras_necessarias: u8,
    intervalo: Duration,
    tamanho_maximo: u64,
) -> Result<ArquivoEstavel, FileOpsError> {
    let mut leituras_identicas_seguidas: u8 = 0;
    let mut ultimo_tamanho: Option<u64> = None;
    let mut ultimo_mtime: Option<std::time::SystemTime> = None;

    loop {
        let metadata = match tokio::fs::metadata(path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Não é erro real: muito provavelmente era um arquivo
                // temporário (.tmp / GUID) que o Windows renomeou para o
                // nome final. O evento MOVED_TO desse rename chega
                // separadamente e será tratado como um novo evento.
                return Err(FileOpsError::ArquivoDesapareceu(path.to_path_buf()));
            }
            Err(e) => return Err(FileOpsError::Io(e)),
        };

        let tamanho_atual = metadata.len();
        let mtime_atual = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);

        if tamanho_atual > tamanho_maximo {
            return Err(FileOpsError::TamanhoExcedido {
                tamanho: tamanho_atual,
                limite: tamanho_maximo,
            });
        }

        let estavel_desta_vez =
            Some(tamanho_atual) == ultimo_tamanho && Some(mtime_atual) == ultimo_mtime;

        if estavel_desta_vez {
            leituras_identicas_seguidas += 1;
            debug!(
                leituras_identicas_seguidas,
                leituras_necessarias, "leitura estável confirmada"
            );
        } else {
            if ultimo_tamanho.is_some() {
                debug!(
                    tamanho_anterior = ?ultimo_tamanho,
                    tamanho_atual,
                    "arquivo ainda mudando, reiniciando contagem de estabilidade"
                );
            }
            leituras_identicas_seguidas = 1;
        }

        ultimo_tamanho = Some(tamanho_atual);
        ultimo_mtime = Some(mtime_atual);

        if leituras_identicas_seguidas >= leituras_necessarias {
            break;
        }

        tokio::time::sleep(intervalo).await;
    }

    if ultimo_tamanho == Some(0) {
        return Err(FileOpsError::TamanhoZero);
    }

    // Como o produto agora é um Watcher Genérico (agnóstico de formato), a
    // validação do conteúdo do arquivo (parse de XML, ZIP, etc) passa a ser
    // 100% responsabilidade do Worker/Consumidor final.
    // O watcher apenas garante a entrega íntegra e atômica.

    let hash_sha256 = calcular_hash_sha256(path).await?;

    // O produtor pode voltar a escrever depois da janela de estabilidade.
    // Confirme que o conteúdo usado no hash ainda representa o arquivo atual.
    let metadata_final = tokio::fs::metadata(path).await?;
    let tamanho_final = metadata_final.len();
    let mtime_final = metadata_final.modified().unwrap_or(std::time::UNIX_EPOCH);
    if Some(tamanho_final) != ultimo_tamanho || Some(mtime_final) != ultimo_mtime {
        debug!("arquivo mudou durante o cálculo do hash; reiniciando checagem");
        return Box::pin(aguardar_estabilidade(
            path,
            leituras_necessarias,
            intervalo,
            tamanho_maximo,
        ))
        .await;
    }

    Ok(ArquivoEstavel {
        path: path.to_path_buf(),
        tamanho_bytes: ultimo_tamanho.unwrap_or(0),
        hash_sha256,
    })
}

pub async fn calcular_hash_sha256(path: &Path) -> Result<String, FileOpsError> {
    use tokio::io::AsyncReadExt;
    let mut arquivo = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];

    loop {
        let lidos = arquivo.read(&mut buffer).await?;
        if lidos == 0 {
            break;
        }
        hasher.update(&buffer[..lidos]);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Move o arquivo de forma atômica (`rename`) da quarentena de detecção
/// para a pasta de processamento, com retry exponencial + jitter.
///
/// O nome de destino é prefixado pelo hash para eliminar colisão entre
/// arquivos de clientes diferentes que cheguem com o mesmo nome original
/// — situação comum quando o ERP de origem usa nomenclatura previsível
/// (ex: "nfe.xml") para todos os clientes.
#[instrument(skip(arquivo), fields(hash = %arquivo.hash_sha256, path = %arquivo.path.display()))]
pub async fn mover_atomico_com_retry(
    arquivo: &ArquivoEstavel,
    pasta_destino: &Path,
    max_tentativas: u32,
) -> Result<PathBuf, FileOpsError> {
    let nome_original = arquivo
        .path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "arquivo.xml".to_string());

    let uuid_str = uuid::Uuid::new_v4().to_string();
    let nome_destino = format!(
        "{}__{}__{}",
        &arquivo.hash_sha256[..16],
        &uuid_str[..8],
        nome_original
    );
    let destino = pasta_destino.join(nome_destino);

    let mut ultima_falha: Option<std::io::Error> = None;

    for tentativa in 1..=max_tentativas {
        match tokio::fs::rename(&arquivo.path, &destino).await {
            Ok(()) => {
                debug!(tentativa, destino = %destino.display(), "move atômico concluído");
                return Ok(destino);
            }
            Err(e) => {
                warn!(
                    tentativa,
                    max_tentativas,
                    erro = %e,
                    "falha ao mover arquivo, tentando novamente com backoff"
                );
                ultima_falha = Some(e);

                // Backoff exponencial com jitter: evita que múltiplos
                // arquivos falhando ao mesmo tempo (ex: lentidão pontual
                // de disco/rede) gerem uma rajada sincronizada de retries.
                let base_ms = 200u64 * 2u64.pow(tentativa.saturating_sub(1).min(6));
                let jitter_ms = rand::random::<u64>() % 100;
                tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
            }
        }
    }

    Err(FileOpsError::RetriesEsgotados {
        tentativas: max_tentativas,
        ultimo_erro: ultima_falha
            .map(|e| e.to_string())
            .unwrap_or_else(|| "desconhecido".to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arquivo_teste(nome: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ingestorx_{nome}_{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn rejeita_arquivo_vazio() {
        let path = arquivo_teste("vazio");
        tokio::fs::write(&path, b"").await.unwrap();
        let resultado = aguardar_estabilidade(&path, 2, Duration::from_millis(1), 10).await;
        assert!(matches!(resultado, Err(FileOpsError::TamanhoZero)));
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn calcula_hash_e_move_arquivo_estavel() {
        let path = arquivo_teste("estavel");
        let destino = arquivo_teste("destino");
        tokio::fs::create_dir_all(&destino).await.unwrap();
        tokio::fs::write(&path, b"conteudo").await.unwrap();

        let arquivo = aguardar_estabilidade(&path, 2, Duration::from_millis(1), 1024)
            .await
            .unwrap();
        assert_eq!(arquivo.tamanho_bytes, 8);
        assert_eq!(arquivo.hash_sha256.len(), 64);
        let movido = mover_atomico_com_retry(&arquivo, &destino, 1)
            .await
            .unwrap();
        assert!(!path.exists());
        assert_eq!(tokio::fs::read(&movido).await.unwrap(), b"conteudo");
        tokio::fs::remove_dir_all(destino).await.unwrap();
    }
}

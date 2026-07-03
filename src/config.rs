//! Configuração central do watcher.
//!
//! BOA PRÁTICA: nunca espalhe valores "mágicos" (timeouts, paths, limites)
//! pelo código. Centralizar aqui facilita auditoria, ajuste fino em produção
//! e — futuramente — hot-reload via ConfigSync sem precisar tocar no core.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone)]
pub struct WatcherConfig {
    /// Pasta onde os XMLs chegam (landing zone). NUNCA processamos aqui.
    pub pasta_entrada: PathBuf,

    /// Pasta de quarentena/staging: arquivo é movido para cá assim que
    /// detectado e considerado "estável", antes de qualquer outra ação.
    pub pasta_processando: PathBuf,

    /// Destino final reservado para o consumidor downstream.
    pub pasta_processado: PathBuf,

    /// Arquivos que falharam definitivamente (esgotaram retries) vão para
    /// cá, nunca são descartados silenciosamente.
    pub pasta_erro: PathBuf,

    /// Arquivos com extensões não permitidas são movidos para cá imediatamente.
    pub pasta_ignorados: PathBuf,

    /// Diretório onde os logs em disco são gravados (rolling file).
    pub pasta_logs: PathBuf,

    /// Buffer local de contingência: eventos "publicados" aqui quando a
    /// fila/broker está inacessível. Drenado assim que a fila volta.
    pub pasta_buffer_contingencia: PathBuf,

    /// Recibos duráveis que impedem republicação contínua após restart.
    pub pasta_recibos_publicacao: PathBuf,

    /// Tamanho máximo aceito para um XML de NF-e. Acima disso é tratado
    /// como anômalo/suspeito e vai direto para quarentena de erro —
    /// proteção básica contra arquivo malformado ou ataque de payload.
    pub tamanho_maximo_bytes: u64,

    /// Intervalo entre duas leituras de tamanho/mtime para considerar
    /// um arquivo "estável" (parou de ser escrito).
    pub intervalo_checagem_estabilidade: Duration,

    /// Quantas leituras idênticas seguidas exigimos antes de considerar
    /// o arquivo realmente pronto. 2 já cobre a maioria dos casos; 3 é
    /// mais conservador para cópias de rede lentas/instáveis.
    pub leituras_estaveis_necessarias: u8,

    /// Intervalo do scanner de reconciliação (rede de segurança).
    pub intervalo_scanner_reconciliacao: Duration,

    /// Intervalo do heartbeat enviado ao monitor central.
    pub intervalo_heartbeat: Duration,

    /// Máximo de tentativas antes de mover para pasta_erro definitivamente.
    pub max_tentativas_retry: u32,
    /// Identificador do tenant/cliente ao qual este watcher pertence.
    pub tenant_id: String,

    /// Identificador único deste worker/instância. Usado para isolar a quarentena.
    pub worker_id: String,

    /// Lista de extensões permitidas. Arquivos fora dessa lista vão para ignorados.
    pub extensoes_permitidas: Vec<String>,

    /// URL de conexão do RabbitMQ (ex: amqp://admin:admin@localhost:5672/%2f)
    pub rabbitmq_url: String,

    /// Fila durável declarada pelo watcher antes de publicar.
    pub rabbitmq_fila: String,

    /// Timeout máximo de cada operação no broker.
    pub rabbitmq_timeout: Duration,

    /// CA PEM usada para validar o certificado do RabbitMQ em AMQPS.
    pub rabbitmq_ca_cert: Option<PathBuf>,

    /// Endereço HTTP para health checks e métricas Prometheus.
    pub observabilidade_addr: SocketAddr,

    /// Lock compartilhado que limita o scanner a uma instância por pasta.
    pub pasta_lock_scanner: PathBuf,

    /// Limite global de arquivos em processamento simultâneo. Impõe
    /// backpressure e evita uma task por arquivo sem limite durante rajadas.
    pub max_processamentos_concorrentes: usize,

    /// Quantidade máxima de entregas RabbitMQ processadas em paralelo.
    pub max_consumidores_concorrentes: usize,
    pub postgres_url: String,
    pub object_storage_endpoint: String,
    pub object_storage_region: String,
    pub object_storage_bucket: String,
    pub object_storage_access_key: String,
    pub object_storage_secret_key: String,
}

impl WatcherConfig {
    pub fn from_env_or_default() -> anyhow::Result<Self> {
        // BOA PRÁTICA: em produção isso viria de um arquivo de config
        // (TOML/YAML) versionado e validado, ou de um serviço central
        // (ConfigSync). Aqui usamos defaults + env vars só para manter
        // o exemplo autocontido.
        let base = std::env::var("XML_WATCHER_BASE_DIR").unwrap_or_else(|_| "./dados".to_string());
        let base = PathBuf::from(base);

        let tenant_id =
            std::env::var("XML_WATCHER_TENANT_ID").unwrap_or_else(|_| "tenant_default".to_string());

        let worker_id =
            std::env::var("XML_WATCHER_WORKER_ID").unwrap_or_else(|_| "worker_1".to_string());

        let extensoes_str =
            std::env::var("XML_WATCHER_EXTENSOES_PERMITIDAS").unwrap_or_else(|_| "xml".to_string());
        let extensoes_permitidas = extensoes_str
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .map(|s| s.trim_start_matches('.').to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let rabbitmq_url = construir_url_rabbitmq()?;
        let rabbitmq_ca_cert = std::env::var("XML_WATCHER_RABBITMQ_CA_CERT")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from);
        let rabbitmq_fila = std::env::var("XML_WATCHER_RABBITMQ_FILA")
            .unwrap_or_else(|_| "ingestorx_fila".to_string());
        let rabbitmq_timeout = Duration::from_secs(
            std::env::var("XML_WATCHER_RABBITMQ_TIMEOUT_SECS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .map_err(|e| anyhow::anyhow!("XML_WATCHER_RABBITMQ_TIMEOUT_SECS inválido: {e}"))?,
        );
        let observabilidade_addr = std::env::var("XML_WATCHER_OBSERVABILIDADE_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:9898".to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("XML_WATCHER_OBSERVABILIDADE_ADDR inválido: {e}"))?;
        let max_processamentos_concorrentes =
            parse_usize_env("XML_WATCHER_MAX_PROCESSAMENTOS_CONCORRENTES", 64)?;
        let max_consumidores_concorrentes =
            parse_usize_env("XML_WATCHER_MAX_CONSUMIDORES_CONCORRENTES", 16)?;
        let postgres_url = construir_url_postgres()?;
        let object_storage_endpoint = env_obrigatoria("XML_WATCHER_OBJECT_STORAGE_ENDPOINT")?;
        let object_storage_region = env_obrigatoria("XML_WATCHER_OBJECT_STORAGE_REGION")?;
        let object_storage_bucket = env_obrigatoria("XML_WATCHER_OBJECT_STORAGE_BUCKET")?;
        let object_storage_access_key = env_obrigatoria("XML_WATCHER_OBJECT_STORAGE_ACCESS_KEY")?;
        let object_storage_secret_key = ler_secret_env(
            "XML_WATCHER_OBJECT_STORAGE_SECRET_KEY_FILE",
            "object storage",
        )?;

        Ok(Self {
            pasta_entrada: base.join("entrada"),
            pasta_processando: base.join("processando").join(&worker_id),
            pasta_processado: base.join("processado"),
            pasta_erro: base.join("erro"),
            pasta_ignorados: base.join("ignorados"),
            pasta_logs: base.join("logs"),
            pasta_buffer_contingencia: base.join("buffer_contingencia"),
            pasta_recibos_publicacao: base.join("recibos_publicacao").join(&worker_id),
            tamanho_maximo_bytes: 50 * 1024 * 1024, // 50MB — acima disso é suspeito para um XML de NF-e
            intervalo_checagem_estabilidade: Duration::from_millis(500),
            leituras_estaveis_necessarias: 3,
            intervalo_scanner_reconciliacao: Duration::from_secs(60),
            intervalo_heartbeat: Duration::from_secs(15),
            max_tentativas_retry: 5,
            tenant_id,
            worker_id,
            extensoes_permitidas,
            rabbitmq_url,
            rabbitmq_fila,
            rabbitmq_timeout,
            rabbitmq_ca_cert,
            observabilidade_addr,
            pasta_lock_scanner: base.join(".scanner.lock"),
            max_processamentos_concorrentes,
            max_consumidores_concorrentes,
            postgres_url,
            object_storage_endpoint,
            object_storage_region,
            object_storage_bucket,
            object_storage_access_key,
            object_storage_secret_key,
        })
    }

    pub fn validar(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.tenant_id.trim().is_empty(),
            "tenant_id não pode ser vazio"
        );
        anyhow::ensure!(
            !self.worker_id.trim().is_empty(),
            "worker_id não pode ser vazio"
        );
        anyhow::ensure!(
            !self.extensoes_permitidas.is_empty(),
            "ao menos uma extensão permitida deve ser configurada"
        );
        anyhow::ensure!(
            self.rabbitmq_url.starts_with("amqps://"),
            "RabbitMQ sem TLS não é permitido; use AMQPS"
        );
        let ca = self.rabbitmq_ca_cert.as_ref().ok_or_else(|| {
            anyhow::anyhow!("XML_WATCHER_RABBITMQ_CA_CERT é obrigatório com AMQPS")
        })?;
        anyhow::ensure!(
            ca.is_file(),
            "certificado CA não encontrado: {}",
            ca.display()
        );
        anyhow::ensure!(
            !self.rabbitmq_fila.trim().is_empty(),
            "XML_WATCHER_RABBITMQ_FILA não pode ser vazia"
        );
        anyhow::ensure!(
            self.leituras_estaveis_necessarias > 0,
            "leituras estáveis deve ser maior que zero"
        );
        anyhow::ensure!(
            self.max_tentativas_retry > 0,
            "máximo de retries deve ser maior que zero"
        );
        anyhow::ensure!(
            self.max_processamentos_concorrentes > 0,
            "limite de processamentos concorrentes deve ser maior que zero"
        );
        anyhow::ensure!(
            self.max_consumidores_concorrentes > 0,
            "limite de consumidores concorrentes deve ser maior que zero"
        );
        anyhow::ensure!(
            self.postgres_url.starts_with("postgresql://"),
            "URL PostgreSQL inválida"
        );
        anyhow::ensure!(
            self.object_storage_endpoint.starts_with("http://")
                || self.object_storage_endpoint.starts_with("https://"),
            "endpoint de object storage inválido"
        );
        anyhow::ensure!(
            !self.object_storage_bucket.contains('/') && !self.object_storage_bucket.is_empty(),
            "bucket de object storage inválido"
        );
        Ok(())
    }

    /// Confirma permissão real de escrita sem depender apenas dos metadados.
    pub fn validar_diretorios_gravaveis(&self) -> anyhow::Result<()> {
        for pasta in [
            &self.pasta_entrada,
            &self.pasta_processando,
            &self.pasta_erro,
            &self.pasta_ignorados,
            &self.pasta_logs,
            &self.pasta_buffer_contingencia,
            &self.pasta_recibos_publicacao,
        ] {
            let teste = pasta.join(format!(".write-test-{}", uuid::Uuid::new_v4()));
            std::fs::write(&teste, b"").map_err(|e| {
                anyhow::anyhow!("diretório {} não é gravável: {e}", pasta.display())
            })?;
            std::fs::remove_file(teste)?;
        }
        Ok(())
    }

    /// Garante que toda a estrutura de pastas existe antes do watcher subir.
    /// Falhar cedo aqui é melhor que falhar silenciosamente em produção.
    pub fn garantir_estrutura_pastas(&self) -> std::io::Result<()> {
        for p in [
            &self.pasta_entrada,
            &self.pasta_processando,
            &self.pasta_processado,
            &self.pasta_erro,
            &self.pasta_ignorados,
            &self.pasta_logs,
            &self.pasta_buffer_contingencia,
            &self.pasta_recibos_publicacao,
        ] {
            std::fs::create_dir_all(p)?;
        }
        Ok(())
    }
}

fn parse_usize_env(nome: &str, padrao: usize) -> anyhow::Result<usize> {
    std::env::var(nome)
        .unwrap_or_else(|_| padrao.to_string())
        .parse()
        .map_err(|e| anyhow::anyhow!("{nome} inválido: {e}"))
}

fn env_obrigatoria(nome: &str) -> anyhow::Result<String> {
    let valor =
        std::env::var(nome).map_err(|_| anyhow::anyhow!("variável obrigatória ausente: {nome}"))?;
    anyhow::ensure!(!valor.trim().is_empty(), "variável vazia: {nome}");
    Ok(valor)
}

fn ler_secret_env(nome: &str, descricao: &str) -> anyhow::Result<String> {
    let path = PathBuf::from(env_obrigatoria(nome)?);
    validar_permissao_secret(&path)?;
    let valor = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("falha ao ler secret de {descricao}: {e}"))?;
    anyhow::ensure!(!valor.trim().is_empty(), "secret de {descricao} vazio");
    Ok(valor.trim().to_string())
}

fn construir_url_rabbitmq() -> anyhow::Result<String> {
    let Ok(password_file) = std::env::var("XML_WATCHER_RABBITMQ_PASSWORD_FILE") else {
        return std::env::var("XML_WATCHER_RABBITMQ_URL").map_err(|_| {
            anyhow::anyhow!(
                "credencial RabbitMQ ausente; execute `just setup` ou configure PASSWORD_FILE"
            )
        });
    };
    let password_path = PathBuf::from(password_file);
    validar_permissao_secret(&password_path)?;
    let password = std::fs::read_to_string(&password_path)
        .map_err(|e| anyhow::anyhow!("falha ao ler secret {}: {e}", password_path.display()))?;
    let password = password.trim();
    anyhow::ensure!(!password.is_empty(), "secret do RabbitMQ está vazio");

    let user =
        std::env::var("XML_WATCHER_RABBITMQ_USER").unwrap_or_else(|_| "ingestorx".to_string());
    let host =
        std::env::var("XML_WATCHER_RABBITMQ_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("XML_WATCHER_RABBITMQ_PORT")
        .unwrap_or_else(|_| "5673".to_string())
        .parse()
        .map_err(|e| anyhow::anyhow!("XML_WATCHER_RABBITMQ_PORT inválida: {e}"))?;
    anyhow::ensure!(
        !host.contains(['/', '@', ':']),
        "host RabbitMQ contém caracteres inválidos"
    );
    let encode = |valor: &str| {
        percent_encoding::utf8_percent_encode(valor, percent_encoding::NON_ALPHANUMERIC).to_string()
    };
    Ok(format!(
        "amqps://{}:{}@{}:{}/%2f",
        encode(&user),
        encode(password),
        host,
        port
    ))
}

fn construir_url_postgres() -> anyhow::Result<String> {
    if let Ok(url) = std::env::var("XML_WATCHER_POSTGRES_URL") {
        return Ok(url);
    }
    let password = ler_secret_env("XML_WATCHER_POSTGRES_PASSWORD_FILE", "PostgreSQL")?;
    let user = std::env::var("XML_WATCHER_POSTGRES_USER").unwrap_or_else(|_| "ingestorx".into());
    let host = std::env::var("XML_WATCHER_POSTGRES_HOST").unwrap_or_else(|_| "localhost".into());
    let port: u16 = std::env::var("XML_WATCHER_POSTGRES_PORT")
        .unwrap_or_else(|_| "15432".into())
        .parse()
        .map_err(|e| anyhow::anyhow!("porta PostgreSQL inválida: {e}"))?;
    anyhow::ensure!(!host.contains(['/', '@', ':']), "host PostgreSQL inválido");
    let encode = |valor: &str| {
        percent_encoding::utf8_percent_encode(valor, percent_encoding::NON_ALPHANUMERIC).to_string()
    };
    Ok(format!(
        "postgresql://{}:{}@{}:{}/ingestorx",
        encode(&user),
        encode(&password),
        host,
        port
    ))
}

#[cfg(unix)]
fn validar_permissao_secret(path: &std::path::Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    anyhow::ensure!(
        mode & 0o077 == 0,
        "secret {} deve ser acessível somente pelo proprietário (chmod 600)",
        path.display()
    );
    Ok(())
}

#[cfg(not(unix))]
fn validar_permissao_secret(path: &std::path::Path) -> anyhow::Result<()> {
    anyhow::ensure!(path.is_file(), "secret não encontrado: {}", path.display());
    Ok(())
}

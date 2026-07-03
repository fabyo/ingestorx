set shell := ["bash", "-c"]

# Configuração inicial interativa: gera secrets, TLS e .env
setup:
    bash ./scripts/setup.sh

# Exibe a senha inicial do Grafana para o administrador local
show-grafana-password:
    @test -f ./secrets/grafana_admin_password || { echo "Execute 'just setup' primeiro."; exit 1; }
    @cat ./secrets/grafana_admin_password

# Exibe a credencial local do console MinIO para o administrador
show-minio-password:
    @test -f ./secrets/minio_secret_key || { echo "Execute 'just setup' primeiro."; exit 1; }
    @cat ./secrets/minio_secret_key

# Sobe a infraestrutura completa (RabbitMQ, Loki, Promtail e Grafana)
up:
    @test -f .env || { echo "Execute 'just setup' primeiro."; exit 1; }
    docker compose up -d

# Derruba a infraestrutura
down:
    docker compose down

# Inicia o IngestorX em background (PID salvo em xml_watcher.pid)
start:
    @test -f ./secrets/rabbitmq_password || { echo "Execute 'just setup' primeiro."; exit 1; }
    @mkdir -p ./dados/logs
    @cargo build
    @nohup ./target/debug/xml_watcher > ./dados/logs/console.log 2>&1 & echo $! > xml_watcher.pid
    @echo "IngestorX iniciado em background (PID: $(cat xml_watcher.pid)). Logs em ./dados/logs/console.log"

# Para o IngestorX rodando em background
stop:
    @if [ -f xml_watcher.pid ]; then \
        pid=$(cat xml_watcher.pid); \
        echo "Parando IngestorX (PID: $pid)..."; \
        kill $pid; \
        rm xml_watcher.pid; \
        echo "IngestorX parado com sucesso."; \
    else \
        echo "Nenhum processo IngestorX em background encontrado (xml_watcher.pid ausente)."; \
    fi

# Verifica se o IngestorX em background está rodando
status:
    @if [ -f xml_watcher.pid ]; then \
        pid=$(cat xml_watcher.pid); \
        if ps -p $pid > /dev/null; then \
            echo "IngestorX está RODANDO (PID: $pid)."; \
        else \
            echo "Arquivo PID existe mas o processo $pid NÃO está rodando (limpando PID)."; \
            rm xml_watcher.pid; \
        fi; \
    else \
        echo "IngestorX está PARADO."; \
    fi

# Roda o projeto Rust localmente no foreground
run:
    @test -f ./secrets/rabbitmq_password || { echo "Execute 'just setup' primeiro."; exit 1; }
    cargo run --bin xml_watcher

# Executa o consumidor downstream no foreground
run-consumer:
    @test -f ./secrets/rabbitmq_password || { echo "Execute 'just setup' primeiro."; exit 1; }
    cargo run --bin consumer

# Instala e inicia watcher + consumidor como serviços systemd do usuário
install-systemd:
    bash ./scripts/install-systemd.sh

# Remove os serviços systemd sem apagar dados ou secrets
uninstall-systemd:
    bash ./scripts/uninstall-systemd.sh

# Mostra o estado dos serviços systemd
service-status:
    systemctl --user --no-pager status ingestorx-watcher.service ingestorx-consumer.service

# Gera arquivos de teste (usando bash no WSL/Linux)
test-data:
    bash ./scripts/gerar_arquivos_teste.sh

# Validação completa antes de commitar
check:
    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-targets

# Gera uma rajada controlada (padrão: 10 mil arquivos)
load-test quantidade="10000":
    bash ./scripts/load-test.sh {{quantidade}}

# Backup lógico consistente do PostgreSQL
backup-postgres:
    bash ./scripts/backup-postgres.sh

# Aplica migrações idempotentes no PostgreSQL já existente
migrate-postgres:
    bash ./scripts/migrate-postgres.sh

# Mostra todos os comandos disponíveis
help:
    @just --list

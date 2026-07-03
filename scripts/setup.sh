#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

printf '%s\n' \
  'Configuração inicial segura do IngestorX' \
  '' \
  'Serão criados:' \
  '  - secrets locais com senhas aleatórias;' \
  '  - CA e certificado TLS para localhost;' \
  '  - arquivo .env sem senhas;' \
  '  - diretórios de execução.' \
  ''
read -r -p 'Deseja continuar? [s/N] ' resposta
case "${resposta,,}" in
  s|sim|y|yes) ;;
  *) printf '%s\n' 'Configuração cancelada; nenhum arquivo foi alterado.'; exit 0 ;;
esac

for comando in openssl docker; do
  if ! command -v "$comando" >/dev/null 2>&1; then
    printf 'Erro: comando obrigatório não encontrado: %s\n' "$comando" >&2
    exit 1
  fi
done

mkdir -p secrets docker/certs dados
chmod 700 secrets docker/certs

if [[ ! -s secrets/rabbitmq_password ]]; then
  openssl rand -hex 32 > secrets/rabbitmq_password
fi
if [[ ! -s secrets/grafana_admin_password ]]; then
  openssl rand -hex 24 > secrets/grafana_admin_password
fi
if [[ ! -s secrets/postgres_password ]]; then
  openssl rand -hex 32 > secrets/postgres_password
fi
if [[ ! -s secrets/minio_secret_key ]]; then
  openssl rand -hex 32 > secrets/minio_secret_key
fi
chmod 600 secrets/rabbitmq_password secrets/grafana_admin_password \
  secrets/postgres_password secrets/minio_secret_key

if [[ ! -s docker/certs/ca.pem || ! -s secrets/rabbitmq_server_key || ! -s docker/certs/server.pem ]]; then
  rm -f docker/certs/ca.pem docker/certs/ca-key.pem docker/certs/server.csr \
    docker/certs/server.pem secrets/rabbitmq_server_key

  openssl req -x509 -newkey rsa:4096 -sha256 -nodes -days 3650 \
    -subj '/CN=IngestorX Local CA' \
    -keyout docker/certs/ca-key.pem \
    -out docker/certs/ca.pem

  openssl req -new -newkey rsa:3072 -sha256 -nodes \
    -subj '/CN=localhost' \
    -addext 'subjectAltName=DNS:localhost,DNS:rabbitmq,IP:127.0.0.1' \
    -keyout secrets/rabbitmq_server_key \
    -out docker/certs/server.csr

  printf '%s\n' \
    'subjectAltName=DNS:localhost,DNS:rabbitmq,IP:127.0.0.1' \
    'extendedKeyUsage=serverAuth' \
    'keyUsage=digitalSignature,keyEncipherment' > docker/certs/server.ext

  openssl x509 -req -sha256 -days 825 \
    -in docker/certs/server.csr \
    -CA docker/certs/ca.pem \
    -CAkey docker/certs/ca-key.pem \
    -CAcreateserial \
    -extfile docker/certs/server.ext \
    -out docker/certs/server.pem
fi

chmod 600 docker/certs/ca-key.pem secrets/rabbitmq_server_key
chmod 644 docker/certs/ca.pem docker/certs/server.pem
rm -f docker/certs/server.csr docker/certs/server.ext docker/certs/ca.srl

cat > .env <<'EOF'
XML_WATCHER_BASE_DIR=./dados
XML_WATCHER_TENANT_ID=tenant_default
XML_WATCHER_WORKER_ID=worker_1
XML_WATCHER_EXTENSOES_PERMITIDAS=xml
XML_WATCHER_RABBITMQ_USER=ingestorx
XML_WATCHER_RABBITMQ_PASSWORD_FILE=./secrets/rabbitmq_password
XML_WATCHER_RABBITMQ_HOST=localhost
XML_WATCHER_RABBITMQ_PORT=5673
XML_WATCHER_RABBITMQ_CA_CERT=./docker/certs/ca.pem
XML_WATCHER_RABBITMQ_FILA=ingestorx_fila
XML_WATCHER_RABBITMQ_TIMEOUT_SECS=10
XML_WATCHER_OBSERVABILIDADE_ADDR=127.0.0.1:9898
XML_WATCHER_MAX_PROCESSAMENTOS_CONCORRENTES=64
XML_WATCHER_MAX_CONSUMIDORES_CONCORRENTES=16
XML_WATCHER_POSTGRES_USER=ingestorx
XML_WATCHER_POSTGRES_PASSWORD_FILE=./secrets/postgres_password
XML_WATCHER_POSTGRES_HOST=localhost
XML_WATCHER_POSTGRES_PORT=15432
XML_WATCHER_OBJECT_STORAGE_ENDPOINT=http://127.0.0.1:19000
XML_WATCHER_OBJECT_STORAGE_REGION=us-east-1
XML_WATCHER_OBJECT_STORAGE_BUCKET=ingestorx-xml
XML_WATCHER_OBJECT_STORAGE_ACCESS_KEY=ingestorx
XML_WATCHER_OBJECT_STORAGE_SECRET_KEY_FILE=./secrets/minio_secret_key
MINIO_ACCESS_KEY=ingestorx
RABBITMQ_USER=ingestorx
GRAFANA_ADMIN_USER=admin
RUST_LOG=info,xml_watcher=debug
EOF
chmod 600 .env

# O marcador só existe depois de uma configuração concluída. Assim, uma
# execução interrompida após gerar o secret volta a tratar o volume antigo.
if [[ ! -f secrets/.setup-complete ]] && \
   docker volume inspect ingestorx_rabbitmq_data >/dev/null 2>&1; then
  printf '%s\n' \
    '' \
    'Existe um volume RabbitMQ criado com outra senha.' \
    'Para aplicar a nova credencial, o volume precisa ser recriado.' \
    'ATENÇÃO: mensagens existentes serão apagadas.'
  read -r -p 'Deseja recriar o volume RabbitMQ? [s/N] ' recriar
  case "${recriar,,}" in
    s|sim|y|yes) RABBITMQ_USER=ingestorx GRAFANA_ADMIN_USER=admin docker compose down -v ;;
    *) printf '%s\n' 'Configuração interrompida para preservar o volume existente.'; exit 1 ;;
  esac
fi

touch secrets/.setup-complete
chmod 600 secrets/.setup-complete

instalou_systemd=nao
if command -v systemctl >/dev/null 2>&1; then
  instalar=nao
  read -r -p 'Deseja instalar e iniciar watcher/consumidor como serviços systemd? [s/N] ' instalar || true
  case "${instalar,,}" in
    s|sim|y|yes)
      bash ./scripts/install-systemd.sh --yes
      instalou_systemd=sim
      ;;
  esac
fi

printf '%s\n' \
  '' \
  'Configuração concluída.' \
  'As senhas não foram exibidas e estão em ./secrets (chmod 600).' \
  'Use `just up` para subir a infraestrutura.' \
  "Serviços systemd instalados: ${instalou_systemd}." \
  'Sem systemd, use `just run` e `just run-consumer`.' \
  'Use `just show-grafana-password` quando precisar acessar o Grafana.'

![Logo](https://raw.githubusercontent.com/fabyo/ingestorx/main/logo1.png)

O **IngestorX (Watcher)** é um agente de captura (*file watcher*) de arquivos fiscais (como XMLs de NF-e, CT-e, NFS-e) ultra-resiliente, de alto desempenho e pronto para produção corporativa. Ele atua como o "portão de entrada" seguro de um pipeline de dados, monitorando diretórios locais ou de rede para detectar a chegada de novos arquivos e notificá-los de forma garantida a uma fila de mensageria (como RabbitMQ).

### 🎯 Para que serve e onde usar?
* **Integração de ERPs**: Ideal para capturar notas fiscais eletrônicas geradas por ERPs legados que apenas gravam arquivos em disco.
* **Landing Zone de Filiais**: Excelente para rodar como agente local em filiais ou servidores locais que precisam enviar arquivos de forma centralizada e segura para a nuvem.
* **Garantia de Entrega (Ingestão Segura)**: Projetado para cenários onde a perda de um único XML fiscal representa multas ou paradas na operação de faturamento.

---

## 🛡️ Fortalezas e Tolerância a Falhas (Robustez Extrema)

Diferente de watchers convencionais que quebram facilmente sob carga ou lentidão de rede, o IngestorX foi construído em Rust focando em **tolerância a falhas catastróficas**:

* **Proteção Contra Escrituras Parciais (Network Copy)**: Watchers comuns leem o arquivo assim que ele é criado, gerando arquivos corrompidos se a cópia de rede (via Windows SMB/Samba) for lenta. O IngestorX aguarda a estabilização completa do arquivo (checagem iterativa de tamanho e `mtime`) antes de qualquer ação.
* **Garantia de Não-Duplicação e Sobrescrita Zero**: Utiliza renames atômicos no nível do sistema operacional (`rename`) com prefixos randômicos baseados em UUID. Isso garante que duas threads ou processos rodando paralelamente nunca processarão o mesmo arquivo, e arquivos idênticos nunca sobrescreverão uns aos outros.
* **Buffer de Contingência Persistente com Autocura**: Se o servidor de mensageria (RabbitMQ) cair, o IngestorX não para e não descarta arquivos. Ele grava os eventos localmente com sincronização física no disco (`fsync`). Uma tarefa em background em loop contínuo monitora o restabelecimento do broker para drenar e reenviar tudo de forma automática e ordenada.
* **Árvores de Supervisão (Erlang/OTP Model)**: Todas as tarefas críticas (detectores de eventos, scanners de reconciliação, telemetrias e drenadores) rodam sob um supervisor de processos. Se houver um erro transitório ou pânico de memória, a tarefa é reiniciada automaticamente com backoff exponencial.
* **Desligamento 100% Gracioso (Zero Loss Shutdown)**: Escuta sinais do sistema operacional (Ctrl+C / SIGTERM). Ao ser desligado, ele interrompe imediatamente a varredura de novos arquivos e aguarda de forma limpa a conclusão dos XMLs em trânsito no momento.
* **Segurança e Limpeza Silenciosa**: Detecta e descarta na hora arquivos de metadados inúteis do Windows (como `Zone.Identifier`) para que o pipeline downstream permaneça limpo de ruídos.

---

## 💡 Por que cada decisão foi tomada

| Problema | Decisão | Onde |
|---|---|---|
| Windows escreve em arquivo temporário e renomeia ao final | Filtra eventos para `CLOSE_WRITE` e `MOVED_TO`, ignora `CREATE`/`Open` como gatilho de ação | `watcher.rs::caminho_relevante` |
| Arquivo "some" entre detecção e ação | Tratado como ruído esperado (não erro) — o evento do arquivo final chega separado | `file_ops.rs::FileOpsError::ArquivoDesapareceu` |
| Cópia de rede lenta/instável | Exige N leituras idênticas de tamanho+mtime antes de agir | `file_ops.rs::aguardar_estabilidade` |
| Corrida entre EventListener e Scanner | Os dois convergem na mesma função (`tratar_evento`), protegida por claim set + `rename()` atômico com UUID como garantia final | `watcher.rs` |
| Nomes de arquivo colidindo | Destino prefixado com hash SHA-256 + fragmento UUID curto para evitar sobrescritas | `file_ops.rs::mover_atomico_com_retry` |
| Falha transitória ao mover arquivo | Retry com backoff exponencial + jitter | `file_ops.rs::mover_atomico_com_retry` |
| Broker de fila fora do ar | Buffer de contingência local durável (fsync) e drenador automático periódico | `queue.rs::PublisherComContingencia` |
| Task morre por panic e ninguém percebe | Supervisor com restart automático + backoff + log de alta severidade | `supervisor.rs` |
| Log perdido no momento de uma falha | Panic hook captura o panic via `tracing` antes do handler padrão; guard de flush mantida viva | `logging.rs` |
| Restart/deploy perdendo arquivo em trânsito | Graceful shutdown: escuta SIGTERM/SIGINT, ativa CancellationToken, drena claims antes de sair | `main.rs` |
| Arquivos de metadados indesejados | Exclusão imediata e precoce de arquivos `Zone.Identifier` | `watcher.rs::tratar_evento` |

---

## 📂 Estrutura de Pastas Criada em Runtime

```
{XML_WATCHER_BASE_DIR}/
├── entrada/              # landing zone — arquivos chegam aqui
├── processando/          # destino do move atômico, isolado por worker_id
├── processado/           # reservado para o consumidor downstream usar
├── erro/                 # arquivos que falharam definitivamente
├── ignorados/            # arquivos com extensões não permitidas ou vazios
├── logs/                 # logs JSON rotacionados por dia
└── buffer_contingencia/  # eventos pendentes de publicação (broker fora do ar)
```

---

## 🛠️ Variáveis de Configuração (.env)

O comportamento do IngestorX é controlado por variáveis de ambiente. Você pode criar um arquivo `.env` local copiando o modelo de exemplo:
```bash
cp .env.example .env
```

| Variável | Descrição | Valor Padrão | Exemplo |
|---|---|---|---|
| `XML_WATCHER_BASE_DIR` | Caminho base onde as pastas de trabalho serão criadas. | `./dados` | `/mnt/dados_erp` ou `C:\IngestorX\dados` |
| `XML_WATCHER_RABBITMQ_URL`| URL de conexão do broker RabbitMQ. | `amqp://...` | `amqp://user:password@rabbitmq.empresa.com:5672/%2f` |
| `XML_WATCHER_EXTENSOES_PERMITIDAS`| Extensões de arquivo aceitas (separadas por vírgula).| `xml` | `xml,zip,7z` |
| `XML_WATCHER_TENANT_ID` | Identificador do cliente/empresa (para multi-tenant). | `tenant_default` | `empresa_a_ltda` |
| `XML_WATCHER_WORKER_ID` | Identificador único desta instância do watcher. | `worker_1` | `filial_sp_01` |

---

## 🚀 Como Executar e Implantar

O projeto utiliza um `Justfile` para gerenciar tarefas locais. Liste os comandos rodando `just --list` no terminal.

### Desenho de Desenvolvimento Local
1. Suba os containers locais (RabbitMQ, Loki, Grafana, Promtail):
   ```bash
   just up
   ```
2. Execute o watcher localmente (foreground):
   ```bash
   just run
   ```
3. (Opcional) Teste o envio de arquivos e contingência rodando `just test-data`.

---

### Cenário 1: Implantação via Docker / Docker Compose (Nuvem ou SaaS)

Ideal para servidores em nuvem ou ambientes que usam containers.

1. **Dockerfile de Produção**:
   ```dockerfile
   FROM rust:1.75 AS builder
   WORKDIR /app
   COPY . .
   RUN cargo build --release

   FROM alpine:latest
   RUN apk add --no-cache libgcc
   COPY --from=builder /app/target/release/xml_watcher /usr/local/bin/xml_watcher
   CMD ["xml_watcher"]
   ```

2. **Compose de Exemplo (`docker-compose.yml`)**:
   ```yaml
   services:
     ingestorx:
       image: empresa/ingestorx:latest
       container_name: Ingestorx_agent
       restart: always
       volumes:
         - /mnt/pasta_compartilhada_erp:/app/dados/entrada
       environment:
         - XML_WATCHER_BASE_DIR=/app/dados
         - XML_WATCHER_RABBITMQ_URL=amqp://admin:senha@192.168.1.100:5672/%2f
         - XML_WATCHER_EXTENSOES_PERMITIDAS=xml,zip
         - XML_WATCHER_TENANT_ID=cliente_prod_01
         - XML_WATCHER_WORKER_ID=agent_docker_01
   ```

---

### Cenário 2: Instalação como Agente Local (Windows Server ou Linux VM)

Ideal para rodar diretamente nos servidores locais do cliente (onde o ERP gera os arquivos).

#### 1. Gerar o Binário Nativo
```bash
cargo build --release
```
O executável compilado estará em `./target/release/xml_watcher` (ou `xml_watcher.exe` no Windows).

#### 2. Instalação e Execução em Background (Justfile)
Copie o binário compilado e o arquivo `.env` configurado para a pasta de instalação (ex: `/opt/ingestorx` ou `C:\IngestorX`).

* **Iniciar Watcher**: `just start` (Salva o PID localmente e inicia em background).
* **Verificar Status**: `just status` (Indica se está rodando ou parado).
* **Parar Watcher**: `just stop` (Finaliza o processo de forma limpa).

#### 3. Rodando como Serviço do Sistema (Autostart)

* **Linux (Systemd)**: Crie `/etc/systemd/system/ingestorx.service`:
  ```ini
  [Unit]
  Description=IngestorX File Watcher Agent
  After=network.target

  [Service]
  Type=simple
  WorkingDirectory=/opt/ingestorx
  ExecStart=/opt/ingestorx/xml_watcher
  Restart=always

  [Install]
  WantedBy=multi-user.target
  ```
  Ative executando: `sudo systemctl enable --now ingestorx`

* **Windows (Windows Service)**:
  Utilize o **NSSM (Non-Sucking Service Manager)**:
  1. No prompt do Windows como Administrador, rode: `nssm install IngestorX`
  2. Configure a aba **Application**:
     * **Path**: `C:\IngestorX\xml_watcher.exe`
     * **Startup directory**: `C:\IngestorX`
  3. Instale o serviço e inicie-o via services.msc ou `net start IngestorX`.

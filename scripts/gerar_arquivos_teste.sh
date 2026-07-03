#!/bin/bash
# Script para Gerar Cenários de Teste do IngestorX

PASTA_ENTRADA="./dados/entrada"

# Garante que a pasta existe
mkdir -p "$PASTA_ENTRADA"

echo -e "\e[36mIniciando geração de massa de testes...\e[0m"

# ====================================================================
# 1. TESTES QUE VÃO FALHAR NO WATCHER (Bloqueio na Raiz)
# ====================================================================

# 1.1 Arquivo 0 KB (Regra Universal)
touch "$PASTA_ENTRADA/teste_zero_bytes.xml"
echo -e "\e[33mCriado: Arquivo 0 KB (Deve ir para 'ignorados')\e[0m"

touch "$PASTA_ENTRADA/teste_zero_bytes.zip"
echo -e "\e[33mCriado: Arquivo 0 KB ZIP (Deve ir para 'ignorados')\e[0m"

# 1.2 Arquivos com Extensões não cadastradas no .env (PDF, DOC, XLS)
for ext in pdf doc xls; do
    echo "Conteúdo falso do $ext" > "$PASTA_ENTRADA/teste_extensao.$ext"
    echo -e "\e[33mCriado: Arquivo .$ext (Deve ir para 'ignorados')\e[0m"
done

# ====================================================================
# 2. TESTES QUE VÃO PASSAR NO WATCHER (Vão para a Fila do RabbitMQ)
# ====================================================================

# 2.1 Arquivo XML Corrompido / Falso (Watcher aprova, Worker C# vai falhar)
echo "Isso não é um XML de verdade, é um texto qualquer" > "$PASTA_ENTRADA/teste_xml_falso.xml"
echo -e "\e[32mCriado: XML Corrompido (Watcher APROVA, joga na fila e move pra 'processando')\e[0m"

# 2.2 Arquivo ZIP Corrompido / Senha (Watcher aprova, Worker C# vai falhar)
echo "1234567890_isso_corrompe_o_header_do_zip" > "$PASTA_ENTRADA/teste_zip_corrompido.zip"
echo -e "\e[32mCriado: ZIP Corrompido (Watcher APROVA, joga na fila e move pra 'processando')\e[0m"

# 2.3 XML Válido Real (Sucesso Total)
cat <<EOF > "$PASTA_ENTRADA/nota_fiscal_valida.xml"
<?xml version="1.0" encoding="UTF-8"?>
<nota>
    <valor>150.00</valor>
    <cliente>João</cliente>
</nota>
EOF
echo -e "\e[32mCriado: XML Válido (Watcher APROVA, joga na fila e move pra 'processando')\e[0m"

echo -e "\n\e[36mTestes gerados com sucesso na pasta '$PASTA_ENTRADA'!\e[0m"
echo -e "\e[36mSe o seu Watcher estiver rodando, ele já deve ter capturado tudo neste exato segundo.\e[0m"

# Script para Gerar Cenários de Teste do IngestorX
# Rode este script com o Watcher já ligado para ver a mágica acontecer!

$PastaEntrada = ".\dados\entrada"

# Garante que a pasta existe
if (!(Test-Path $PastaEntrada)) {
    New-Item -ItemType Directory -Force -Path $PastaEntrada
}

Write-Host "Iniciando geração de massa de testes..." -ForegroundColor Cyan

# ====================================================================
# 1. TESTES QUE VÃO FALHAR NO WATCHER (Bloqueio na Raiz)
# ====================================================================

# 1.1 Arquivo 0 KB (Regra Universal)
$Path0kbXml = Join-Path $PastaEntrada "teste_zero_bytes.xml"
New-Item -ItemType File -Path $Path0kbXml -Force | Out-Null
Write-Host "Criado: Arquivo 0 KB (Deve ir para 'ignorados')" -ForegroundColor Yellow

$Path0kbZip = Join-Path $PastaEntrada "teste_zero_bytes.zip"
New-Item -ItemType File -Path $Path0kbZip -Force | Out-Null
Write-Host "Criado: Arquivo 0 KB ZIP (Deve ir para 'ignorados')" -ForegroundColor Yellow

# 1.2 Arquivos com Extensões não cadastradas no .env (PDF, DOC, XLS)
$ExtensoesNaoPermitidas = @("pdf", "doc", "xls")
foreach ($ext in $ExtensoesNaoPermitidas) {
    $PathExt = Join-Path $PastaEntrada "teste_extensao.$ext"
    Set-Content -Path $PathExt -Value "Conteúdo falso do $ext"
    Write-Host "Criado: Arquivo .$ext (Deve ir para 'ignorados')" -ForegroundColor Yellow
}


# ====================================================================
# 2. TESTES QUE VÃO PASSAR NO WATCHER (Vão para a Fila do RabbitMQ)
# ====================================================================

# 2.1 Arquivo XML Corrompido / Falso (Watcher aprova, Worker C# vai falhar)
$PathFalsoXml = Join-Path $PastaEntrada "teste_xml_falso.xml"
Set-Content -Path $PathFalsoXml -Value "Isso não é um XML de verdade, é um texto qualquer"
Write-Host "Criado: XML Corrompido (Watcher APROVA, joga na fila e move pra 'processando')" -ForegroundColor Green

# 2.2 Arquivo ZIP Corrompido / Senha (Watcher aprova, Worker C# vai falhar)
# Simulando um arquivo corrompido que se passa por ZIP
$PathFalsoZip = Join-Path $PastaEntrada "teste_zip_corrompido.zip"
Set-Content -Path $PathFalsoZip -Value "1234567890_isso_corrompe_o_header_do_zip"
Write-Host "Criado: ZIP Corrompido (Watcher APROVA, joga na fila e move pra 'processando')" -ForegroundColor Green

# 2.3 XML Válido Real (Sucesso Total)
$PathXmlReal = Join-Path $PastaEntrada "nota_fiscal_valida.xml"
$XmlContent = @"
<?xml version="1.0" encoding="UTF-8"?>
<nota>
    <valor>150.00</valor>
    <cliente>João</cliente>
</nota>
"@
Set-Content -Path $PathXmlReal -Value $XmlContent -Encoding UTF8
Write-Host "Criado: XML Válido (Watcher APROVA, joga na fila e move pra 'processando')" -ForegroundColor Green


Write-Host "`nTestes gerados com sucesso na pasta '$PastaEntrada'!" -ForegroundColor Cyan
Write-Host "Se o seu Watcher estiver rodando, ele já deve ter capturado tudo neste exato segundo." -ForegroundColor Cyan

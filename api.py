import uuid
import os
import subprocess
import sys
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware # <--- Importante para o Dashboard
from pydantic import BaseModel
import robot  # Importa o robot.py para ler configurações e status

app = FastAPI(title="Robô Diário Oficial - V15 Bala de Prata")

# --- 🔓 LIBERAR ACESSO (CORS) ---
# Isso permite que seu Dashboard HTML converse com a API sem bloqueios.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, troque "*" pelo IP do cliente por segurança
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- MODELOS DE DADOS ---
class PedidoBusca(BaseModel):
    cidade: str
    url_alvo: str
    palavras_chave: str
    forcar_reprocessamento: bool = False

# --- ROTA 1: INICIAR O ROBÔ (COMANDO DO SISTEMA) ---
@app.post("/1-iniciar-busca", summary="1. Iniciar Robô (Isolamento Total)")
def iniciar_busca(pedido: PedidoBusca):
    """
    Inicia o robô como um comando de sistema independente (CLI).
    Isso garante que o processamento pesado NÃO trave a API.
    """
    task_id = str(uuid.uuid4())
    
    # 1. Cria o registro inicial no arquivo JSON
    robot.salvar_status_tarefa(task_id, "INICIANDO")
    
    # 2. Prepara o comando para lançar o Python isolado
    # Formato: /caminho/do/python robot.py [ID] [CIDADE] [URL] [KWS] [FORCAR]
    comando = [
        sys.executable, "robot.py", 
        task_id, 
        pedido.cidade, 
        pedido.url_alvo, 
        pedido.palavras_chave, 
        str(pedido.forcar_reprocessamento)
    ]
    
    # 3. Dispara o processo e solta (não espera terminar)
    subprocess.Popen(comando)
    
    return {
        "mensagem": "Comando enviado ao servidor com sucesso!",
        "PROTOCOLO": task_id,
        "dica": "O robô está rodando em um processo separado. Use o protocolo para ver o status."
    }

# --- ROTA 2: CONSULTAR STATUS (LEITURA DE ARQUIVO) ---
@app.get("/2-verificar-resultado/{protocolo}", summary="2. Consultar Status")
def verificar_status(protocolo: str):
    """
    Lê o arquivo status_tarefas.json.
    Resposta instantânea, pois não depende da CPU do robô.
    """
    todos = robot.ler_status()
    tarefa = todos.get(protocolo)
    
    if not tarefa:
        raise HTTPException(status_code=404, detail="Protocolo não encontrado. Verifique se copiou corretamente.")
    
    status = tarefa.get("status")
    
    if status == "INICIANDO":
        return {
            "status": "🚀 INICIANDO", 
            "msg": "O robô está aquecendo os motores...",
            "updated_at": tarefa.get("updated_at")
        }
    
    if status == "RODANDO":
        return {
            "status": "⏳ RODANDO", 
            "msg": "O robô está processando os PDFs. Isso pode levar alguns minutos.", 
            "desde": tarefa.get("updated_at")
        }
    
    if status == "CONCLUIDO":
        return {
            "status": "✅ FINALIZADO", 
            "relatorio": tarefa.get("resultado"),
            "concluido_em": tarefa.get("updated_at")
        }
    
    if status == "ERRO":
        return {
            "status": "❌ ERRO", 
            "erro": tarefa.get("resultado"),
            "horario": tarefa.get("updated_at")
        }
    
    return {"status": "DESCONHECIDO", "dados": tarefa}

# --- ROTA 3: LISTAR ARQUIVOS ---
@app.get("/3-listar-pdfs", summary="3. Ver arquivos baixados")
def listar_pdfs():
    if not os.path.exists(robot.PASTA_PDFS):
        return {"aviso": "A pasta de PDFs ainda não foi criada."}
    
    arquivos = [f for f in os.listdir(robot.PASTA_PDFS) if f.endswith(".pdf")]
    # Ordena por data de modificação (mais recente primeiro)
    arquivos.sort(key=lambda x: os.path.getmtime(os.path.join(robot.PASTA_PDFS, x)), reverse=True)
    
    return {
        "total": len(arquivos),
        "arquivos": arquivos
    }

# --- ROTA 4: BAIXAR ARQUIVO ---
@app.get("/4-baixar-pdf/{nome_arquivo}", summary="4. Baixar um PDF")
def baixar_pdf(nome_arquivo: str):
    caminho = os.path.join(robot.PASTA_PDFS, nome_arquivo)
    
    if os.path.exists(caminho):
        return FileResponse(caminho, media_type="application/pdf", filename=nome_arquivo)
    
    raise HTTPException(status_code=404, detail="Arquivo não encontrado no servidor.")

if __name__ == "__main__":
    # Roda o servidor na porta 8000 acessível externamente
    uvicorn.run(app, host="0.0.0.0", port=8000)

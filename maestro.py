import requests
import time
import json
import pandas as pd
import sys

# URL da sua API local
API_URL = "http://127.0.0.1:8000"

# O nome exato do seu arquivo Excel
ARQUIVO_EXCEL = "ESTADOS HAB ATÉ 20K.xlsx"
ABA_ESTADO = "SÃO PAULO" 

# A palavra que você quer testar no lote todo
TERMO_TESTE = "Licitação" 

# --- CONFIGURAÇÃO DE TEMPO MÁXIMO ---
TEMPO_MAXIMO_SEGUNDOS = 120  # 2 minutos por cidade

def carregar_cidades_xlsx(caminho_arquivo, nome_aba):
    print(f"📂 Abrindo Excel: {caminho_arquivo} | Aba: {nome_aba}...")
    lista_cidades = []
    
    try:
        df = pd.read_excel(caminho_arquivo, sheet_name=nome_aba, skiprows=4)
        df = df.fillna("")
        
        for index, linha in df.iterrows():
            municipio = str(linha.get("Município", "")).strip()
            url = str(linha.get("DIÁRIO OFICIAL", "")).strip()
            status = str(linha.get("STATUS", "")).strip()
            captcha = str(linha.get("TEM CAPTCHA?", "")).strip()
            
            if status == "ENCONTRADO" and captcha == "NÃO" and url.startswith("http"):
                lista_cidades.append({
                    "cidade": municipio,
                    "url_alvo": url,
                    "palavras_chave": TERMO_TESTE,
                    "forcar_reprocessamento": False
                })
                
        print(f"✅ {len(lista_cidades)} cidades válidas filtradas com sucesso!")
        return lista_cidades

    except Exception as e:
        print(f"❌ Erro ao ler o arquivo Excel: {e}")
        return []

def rodar_lote():
    lista_cidades = carregar_cidades_xlsx(ARQUIVO_EXCEL, ABA_ESTADO)
    
    if not lista_cidades:
        print("⚠️ Nenhuma cidade para processar. Cancelando.")
        return

    print("-" * 50)
    print(f"🚀 Iniciando Maestro de Lote! Total: {len(lista_cidades)} cidades.")
    print("-" * 50)
    
    relatorio_final = {}

    for item in lista_cidades:
        cidade = item["cidade"]
        print(f"⏳ [{len(relatorio_final) + 1}/{len(lista_cidades)}] Processando: {cidade.upper()}...")
        
        try:
            resp = requests.post(f"{API_URL}/1-iniciar-busca", json=item)
            dados = resp.json()
            protocolo = dados.get("PROTOCOLO")
            
            if not protocolo:
                print(f"   ❌ Erro ao iniciar. Pulando...")
                relatorio_final[cidade] = "ERRO - Falha ao gerar protocolo"
                continue
                
        except Exception as e:
            print(f"   ❌ Erro de conexão com a API: {e}")
            relatorio_final[cidade] = f"ERRO API - {e}"
            continue

        tempo_inicio = time.time()
        
        while True:
            time.sleep(3) # Checa a cada 3 segundos
            tempo_decorrido = int(time.time() - tempo_inicio)
            
            # --- REGRA DE TIMEOUT (ABORTAR SE DEMORAR MUITO) ---
            if tempo_decorrido > TEMPO_MAXIMO_SEGUNDOS:
                print() # Pula a linha
                print(f"   ⏰ TIMEOUT! A cidade demorou mais de {TEMPO_MAXIMO_SEGUNDOS}s. Abortando e indo para a próxima...")
                relatorio_final[cidade] = f"❌ TIMEOUT - Excedeu {TEMPO_MAXIMO_SEGUNDOS}s"
                break
            
            try:
                resp_status = requests.get(f"{API_URL}/2-verificar-resultado/{protocolo}").json()
                status_atual = resp_status.get("status", "")
                
                if "FINALIZADO" in status_atual or "ERRO" in status_atual:
                    print() # Limpa a linha
                    resultado = resp_status.get("relatorio") or resp_status.get("erro", "Erro Desconhecido")
                    
                    if "Achou" in resultado:
                        print(f"   🟢 ENCONTRADO! ({tempo_decorrido}s) Resultado: {resultado}")
                    elif "CRÍTICO" in resultado:
                        print(f"   🚨 ALERTA LAYOUT: ({tempo_decorrido}s) {resultado}")
                    else:
                        print(f"   ⚪ NADA CONSTA (Limpo em {tempo_decorrido}s)")
                        
                    relatorio_final[cidade] = resultado
                    break
                else:
                    # Relógio rodando com aviso do limite
                    sys.stdout.write(f"\r   🔄 Status: {status_atual} | Tempo: {tempo_decorrido}s / {TEMPO_MAXIMO_SEGUNDOS}s...")
                    sys.stdout.flush()
                    
            except Exception as e:
                print(f"\n   ❌ Erro ao checar status: {e}")
                time.sleep(5)

    print("-" * 50)
    
    nome_arquivo = f"relatorio_{ABA_ESTADO.replace(' ', '_')}_{time.strftime('%Y%m%d_%H%M')}.json"
    with open(nome_arquivo, "w", encoding="utf-8") as f:
        json.dump(relatorio_final, f, indent=4, ensure_ascii=False)
        
    print(f"🎉 Lote de {ABA_ESTADO} finalizado! Resumo salvo em: {nome_arquivo}")

if __name__ == "__main__":
    rodar_lote()

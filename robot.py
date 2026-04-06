import os
import sys
import json

# --- 🛑 CONFIGURAÇÃO ANTI-TRAVAMENTO ---
os.environ["OMP_THREAD_LIMIT"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import time
import hashlib
import requests
import re
from datetime import date, datetime
from unidecode import unidecode
import pdfplumber
from playwright.sync_api import sync_playwright
from sqlalchemy import text
from database import SessionLocal, engine
import concurrent.futures
import pytesseract
from pdf2image import convert_from_path
from PIL import Image, ImageOps

# --- CONFIGURAÇÕES ---
PASTA_PDFS = "pdfs_baixados"
ARQUIVO_STATUS = "status_tarefas.json"
ARQUIVO_DEBUG = "debug_leitura.txt"

if not os.path.exists(PASTA_PDFS):
    os.makedirs(PASTA_PDFS)

def ler_status():
    if not os.path.exists(ARQUIVO_STATUS): return {}
    try:
        with open(ARQUIVO_STATUS, "r") as f: return json.load(f)
    except: return {}

def salvar_status_tarefa(task_id, status, dados=None):
    try:
        todos_status = ler_status()
        tarefa_atual = todos_status.get(task_id, {})
        tarefa_atual["status"] = status
        tarefa_atual["updated_at"] = str(time.time())
        if dados: tarefa_atual["resultado"] = dados
        todos_status[task_id] = tarefa_atual
        with open(ARQUIVO_STATUS, "w") as f: json.dump(todos_status, f, indent=4)
    except: pass

def log_debug(mensagem):
    try:
        with open(ARQUIVO_DEBUG, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now()}] {mensagem}\n{'-'*50}\n")
    except: pass

def garantir_keyword_manual():
    session = SessionLocal()
    try:
        existente = session.execute(text("SELECT id FROM keywords WHERE id = 9999")).fetchone()
        if not existente:
            session.execute(text("INSERT INTO keywords (id, termo, ativo) VALUES (9999, 'Busca Manual', true)"))
            session.commit()
    except Exception as e:
        session.rollback()
        log_debug(f"Erro ao inserir Keyword Manual: {e}")
    finally: session.close()

def calcular_hash_arquivo(caminho_arquivo):
    hash_md5 = hashlib.md5()
    with open(caminho_arquivo, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""): hash_md5.update(chunk)
    return hash_md5.hexdigest()

def limpar_padrao(texto):
    if not texto: return ""
    t = unidecode(texto).lower()
    t = re.sub(r'[^\w\s]', ' ', t)
    return " ".join(t.split())

def limpar_super_cola(texto):
    if not texto: return ""
    t = unidecode(texto).lower()
    return re.sub(r'[^a-z0-9]', '', t)

def normalizar_leetspeak(texto):
    if not texto: return ""
    t = limpar_super_cola(texto)
    t = t.replace('0', 'o').replace('1', 'i').replace('3', 'e').replace('4', 'a').replace('5', 's').replace('@', 'a')
    return t

def verificar_match_v28(termo_busca, texto_pagina_padrao, texto_pagina_cola, texto_raw):
    termo_exato = " ".join(termo_busca.split()).lower()
    texto_exato = " ".join(texto_raw.split()).lower()
    if termo_exato in texto_exato: return True

    letras = re.sub(r'[^a-z]', '', termo_busca.lower())
    if len(letras) <= 2:
        numeros_busca = re.findall(r'\d+', termo_busca)
        if numeros_busca:
            regex_num = r'\D{0,3}'.join(numeros_busca)
            regex_num = r'\b' + regex_num + r'\b'
            if re.search(regex_num, texto_pagina_padrao): return True
        return False 

    busca_padrao = limpar_padrao(termo_busca)
    busca_cola = limpar_super_cola(termo_busca)
    busca_leet = normalizar_leetspeak(termo_busca)
    texto_pagina_leet = normalizar_leetspeak(texto_pagina_cola)

    if len(busca_cola) >= 6 and not busca_cola.isdigit():
        if busca_cola in texto_pagina_cola: return True
    if len(busca_leet) >= 6 and not busca_leet.isdigit():
        if busca_leet in texto_pagina_leet: return True
    
    palavras_busca = busca_padrao.split()
    STOPWORDS = {'dos', 'das', 'aos', 'nas', 'nos', 'com', 'por', 'que', 'para', 'sob', 'uma', 'uns', 'del', 'dla', 'ele', 'ela'}
    
    if len(palavras_busca) > 1:
        palavras_achadas_leet = []
        total_validas = 0
        
        for p_busca in palavras_busca:
            if (len(p_busca) > 2 and p_busca not in STOPWORDS) or p_busca.isdigit(): 
                total_validas += 1
                p_leet = normalizar_leetspeak(p_busca)
                
                if p_busca in texto_pagina_padrao or p_leet in texto_pagina_leet:
                    palavras_achadas_leet.append(p_leet)
        
        if total_validas < 1: total_validas = 1
        encontradas = len(palavras_achadas_leet)
        
        if total_validas <= 2: limite = 1.0   
        elif total_validas == 3: limite = 0.66  
        elif total_validas == 4: limite = 0.75  
        elif total_validas == 5: limite = 0.80  
        else: limite = 0.50  
        
        if (encontradas / total_validas) >= limite:
            regex_pattern = r'.{0,150}'.join(palavras_achadas_leet)
            if re.search(regex_pattern, texto_pagina_leet):
                return True

    return False

def extrair_texto_v29(caminho_pdf, pagina_numero, pagina_obj_pdfplumber, termos_busca_objs, forcar_ocr_total):
    texto_raw_digital = pagina_obj_pdfplumber.extract_text() or ""
    texto_padrao = limpar_padrao(texto_raw_digital)
    texto_cola = limpar_super_cola(texto_raw_digital)
    texto_raw_completo = texto_raw_digital 

    achou_digital = False
    if not forcar_ocr_total and termos_busca_objs:
        for item in termos_busca_objs:
            if verificar_match_v28(item['termo'], texto_padrao, texto_cola, texto_raw_completo):
                achou_digital = True
                break
    
    # --- V29: A MÁGICA DA VELOCIDADE ESTÁ AQUI ---
    precisa_ocr = False
    if forcar_ocr_total: 
        precisa_ocr = True
    elif not achou_digital:
        # Só vai rodar OCR se a página for um escaneamento puro (quase nada de texto digital)
        # Ele ignora se tem imagens ou não, focando apenas se o texto falhou!
        if len(texto_padrao) < 150: 
            precisa_ocr = True

    texto_ocr_padrao = ""
    texto_ocr_cola = ""

    if precisa_ocr:
        try:
            imagens = convert_from_path(caminho_pdf, first_page=pagina_numero, last_page=pagina_numero, dpi=300)
            if imagens:
                img = imagens[0].convert('L')
                img = img.point(lambda x: 0 if x < 160 else 255, '1') 

                cfg_auto = r'--oem 3 --psm 3 -c tessedit_parallelize=0'
                # --- TIMEOUT DE 20 SEGUNDOS NO TESSERACT (Trava de Segurança) ---
                raw_auto = pytesseract.image_to_string(img, lang='por', config=cfg_auto, timeout=20)
                
                cfg_bloco = r'--oem 3 --psm 6 -c tessedit_parallelize=0'
                raw_bloco = pytesseract.image_to_string(img, lang='por', config=cfg_bloco, timeout=20)

                tudo_junto_raw = raw_auto + " \n " + raw_bloco
                texto_raw_completo += " \n " + tudo_junto_raw
                
                texto_ocr_padrao = limpar_padrao(tudo_junto_raw)
                texto_ocr_cola = limpar_super_cola(tudo_junto_raw)
        except: pass

    final_padrao = texto_padrao + " " + texto_ocr_padrao
    final_cola = texto_cola + texto_ocr_cola
    
    return final_padrao, final_cola, texto_raw_completo

def worker_processar_pdf(dados_pacote):
    # Isolamento crítico de conexão para ProcessPoolExecutor no Windows
    from database import DATABASE_URL
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import NullPool
    
    local_engine = create_engine(DATABASE_URL, poolclass=NullPool)
    LocalSession = sessionmaker(autocommit=False, autoflush=False, bind=local_engine)
    session = LocalSession()
    
    link_pdf = dados_pacote['link']
    cidade_nome = dados_pacote['cidade']
    contador = dados_pacote['contador']
    palavras_extras = dados_pacote['palavras_extras']
    modo_bruto = dados_pacote.get('modo_bruto', False) 
    try:
        sufixo = f"_{contador}"
        nome_arquivo = f"{cidade_nome.replace(' ', '_')}_{date.today()}{sufixo}.pdf"
        caminho_final = os.path.join(PASTA_PDFS, nome_arquivo)
        
        try:
            resp = requests.get(link_pdf, stream=True, verify=False, timeout=60)
            if resp.status_code == 200:
                with open(caminho_final, "wb") as f:
                    for chunk in resp.iter_content(8192): f.write(chunk)
            else: return f"❌ Erro HTTP {contador}"
        except Exception as e: return f"❌ Erro Download {contador}: {e}"

        hash_arq = calcular_hash_arquivo(caminho_final)
        try:
            did = session.execute(text("INSERT INTO diarios_log (cidade, data_publicacao, link_pdf, hash_arquivo, processado_em) VALUES (:c, :d, :l, :h, NOW()) RETURNING id"), {"c": cidade_nome, "d": date.today(), "l": link_pdf, "h": hash_arq}).scalar()
            session.commit()
        except:
            session.rollback()
            existente = session.execute(text("SELECT id FROM diarios_log WHERE hash_arquivo = :h"), {"h": hash_arq}).fetchone()
            if existente: did = existente[0]
            else: return f"❌ Erro Banco DB {contador}"

        kws_banco = session.execute(text("SELECT id, termo FROM keywords WHERE ativo = true")).fetchall()
        lista_busca = []
        for k in kws_banco:
            if k.id != 9999: 
                lista_busca.append({"id": k.id, "termo": k.termo})
        
        if palavras_extras:
            extras_list = [p.strip() for p in palavras_extras.replace(",", ";").split(";") if p.strip()]
            for p in extras_list: lista_busca.append({"id": 9999, "termo": p})
        
        achados_doc = []
        try:
            with pdfplumber.open(caminho_final) as pdf:
                for i, pagina in enumerate(pdf.pages):
                    numero_pag = i + 1
                    
                    txt_padrao, txt_cola, txt_raw = extrair_texto_v29(caminho_final, numero_pag, pagina, lista_busca, modo_bruto)
                    
                    for item in lista_busca:
                        if verificar_match_v28(item['termo'], txt_padrao, txt_cola, txt_raw):
                            primeira_palavra = limpar_padrao(item['termo'].split()[0])
                            idx = txt_padrao.find(primeira_palavra)
                            if idx == -1: idx = 0
                            
                            inicio = max(0, idx - 50)
                            trecho = txt_padrao[inicio : inicio + 200]
                            if not trecho: trecho = "Termo encontrado (V29 Match)"

                            session.execute(text("INSERT INTO ocorrencias (diario_id, keyword_id, pagina, trecho_encontrado) VALUES (:d, :k, :p, :t)"), {"d": did, "k": item['id'], "p": numero_pag, "t": trecho})
                            session.commit()
                            achados_doc.append(item['termo'])
        except Exception as e: return f"❌ Erro Leitura PDF {contador}: {e}"
            
        achados_unicos = list(set(achados_doc))
        if achados_unicos: return f"✅ Doc {contador}: Achou {', '.join(achados_unicos)}"
        else: return f"✅ Doc {contador}: Limpo"
    except Exception as e: return f"❌ Erro Geral Worker {contador}: {e}"
    finally: 
        session.close()
        local_engine.dispose()

def extrair_links_imprensa_oficial(page, alvo_url):
    """Layout padrão: imprensaoficialmunicipal.com.br"""
    links_candidatos = []
    seen_links = set()
    termos_interesse = ["visualizar", "exibe_do", "pdf", "anexo", "integra", "download", "arquivo", "publicacao"]
    elementos = page.locator("a").all()
    for link in elementos:
        try:
            href = link.get_attribute("href")
            if not href or href.startswith("javascript"): continue
            if any(t in href.lower() for t in termos_interesse):
                if not href.startswith("http"): href = "https://imprensaoficialmunicipal.com.br" + href
                if href not in seen_links:
                    links_candidatos.append(href)
                    seen_links.add(href)
        except: continue
    return links_candidatos

def extrair_links_portalfacil(page, alvo_url):
    """Layout portalfacil / invista.valadares — chama API AjaxPro diretamente para obter GUIDs dos PDFs"""
    import re
    from urllib.parse import urlparse

    links_candidatos = []
    seen_links = set()

    # Intercepta a resposta AjaxPro que contém a DataTable com os arquivos
    ajax_data = []
    def on_response(res):
        if "ajaxpro/diel_diel_lis" in res.url:
            try:
                body = res.body().decode("utf-8", errors="ignore")
                if "NMARQUIVO" in body or "NMARQUIVO" in body:
                    ajax_data.append(body)
            except: pass

    page.on("response", on_response)
    page.wait_for_timeout(2000)

    # Clica no primeiro GetDiario para disparar a requisição AJAX
    links = page.locator("a").all()
    for l in links:
        try:
            href = l.get_attribute("href") or ""
            if "GetDiario(1)" in href:
                l.click(timeout=5000)
                page.wait_for_timeout(4000)
                break
        except: pass

    page.remove_listener("response", on_response)

    # Extrai GUIDs do tipo {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX} das respostas AJAX
    base_domain = urlparse(alvo_url).netloc  # ex: invista.valadares.mg.gov.br
    # Tenta montar URL do blob storage a partir do domínio
    # Padrão: portalfacilarquivos.blob.core.windows.net/uploads/CIDADE/diario/{GUID}/{GUID}.pdf
    # Extrai o nome da cidade do domínio (ex: GOVERNADORVALADARES)
    cidade_blob = base_domain.split(".")[0].upper().replace("-", "")

    guids_encontrados = []
    for body in ajax_data:
        guids = re.findall(r'\{([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})\}', body)
        guids_encontrados.extend(guids)

    for guid in guids_encontrados:
        guid_upper = guid.upper()
        url_pdf = f"https://{base_domain}/abrir_arquivo.aspx?cdLocal=12&arquivo=%7B{guid_upper}%7D.pdf"
        if url_pdf not in seen_links:
            links_candidatos.append(url_pdf)
            seen_links.add(url_pdf)

    # Fallback: tenta links diretos com termos de interesse
    if not links_candidatos:
        termos_interesse = ["pdf", "download", "arquivo", "diario", "publicacao", "edicao", "visualizar", "anexo"]
        from urllib.parse import urljoin
        for link in page.locator("a").all():
            try:
                href = link.get_attribute("href") or ""
                if href.startswith("javascript"): continue
                if any(t in href.lower() for t in termos_interesse) and href not in seen_links:
                    if not href.startswith("http"):
                        href = urljoin(alvo_url, href)
                    links_candidatos.append(href)
                    seen_links.add(href)
            except: continue

    return links_candidatos

def extrair_links_barbacena(page, alvo_url):
    """Layout barbacena.mg.gov.br/portal/diario-oficial — navega na listagem de arquivos"""
    links_candidatos = []
    seen_links = set()
    
    page.wait_for_timeout(2000)
    
    # Pega todos os links da página
    elementos = page.locator("a").all()
    termos = ["arquivos", "pdf", "download", "diario", "publicacao", "edicao", "visualizar"]
    for link in elementos:
        try:
            href = link.get_attribute("href") or ""
            if not href.startswith("http"):
                from urllib.parse import urljoin
                href = urljoin(alvo_url, href)
            if any(t in href.lower() for t in termos) and href not in seen_links:
                links_candidatos.append(href)
                seen_links.add(href)
        except: continue

    # Se achou página de listagem de arquivos, entra nela para pegar PDFs
    paginas_arquivo = [l for l in links_candidatos if "arquivos" in l.lower()]
    links_pdf_finais = [l for l in links_candidatos if l.lower().endswith(".pdf")]
    
    if paginas_arquivo and not links_pdf_finais:
        try:
            page.goto(paginas_arquivo[0], timeout=30000)
            page.wait_for_timeout(2000)
            for link in page.locator("a").all():
                try:
                    href = link.get_attribute("href") or ""
                    if not href.startswith("http"):
                        from urllib.parse import urljoin
                        href = urljoin(alvo_url, href)
                    if ("pdf" in href.lower() or "download" in href.lower()) and href not in seen_links:
                        links_pdf_finais.append(href)
                        seen_links.add(href)
                except: continue
        except: pass
        return links_pdf_finais

    return links_candidatos

def extrair_links_controlemunicipal(page, alvo_url):
    """Layout ingadigital/controlemunicipal — links de publicacao.php que contêm o PDF"""
    links_candidatos = []
    seen_links = set()
    
    page.wait_for_timeout(2000)
    elementos = page.locator("a").all()
    
    for link in elementos:
        try:
            href = link.get_attribute("href") or ""
            if "controlemunicipal.com.br/site/diario/publicacao.php" in href and href not in seen_links:
                links_candidatos.append(href)
                seen_links.add(href)
        except: continue

    # Para cada página de publicação, extrai o link direto do PDF
    links_pdf = []
    for pub_url in links_candidatos[:10]:  # limita a 10 mais recentes
        try:
            p2 = page.context.new_page()
            p2.goto(pub_url, timeout=20000)
            p2.wait_for_timeout(2000)
            for link in p2.locator("a").all():
                try:
                    href = link.get_attribute("href") or ""
                    if href.lower().endswith(".pdf") and href not in seen_links:
                        if not href.startswith("http"):
                            href = "http://www.controlemunicipal.com.br" + href
                        links_pdf.append(href)
                        seen_links.add(href)
                except: continue
            # Também tenta iframe com PDF embutido
            for iframe in p2.locator("iframe").all():
                try:
                    src = iframe.get_attribute("src") or ""
                    if src.lower().endswith(".pdf") and src not in seen_links:
                        links_pdf.append(src)
                        seen_links.add(src)
                except: continue
            p2.close()
        except: pass

    return links_pdf if links_pdf else links_candidatos

def detectar_layout_e_extrair(page, alvo_url):
    """Detecta o tipo de site e usa o extrator correto."""
    url_lower = alvo_url.lower()
    
    if "ingadigital.com.br" in url_lower or "controlemunicipal.com.br" in url_lower:
        return extrair_links_controlemunicipal(page, alvo_url)
    
    if "barbacena.mg.gov.br" in url_lower:
        return extrair_links_barbacena(page, alvo_url)
    
    if "portalfacil.com.br" in url_lower or "invista." in url_lower or "valadares.mg.gov.br" in url_lower:
        return extrair_links_portalfacil(page, alvo_url)
    
    # Default: imprensa oficial municipal
    return extrair_links_imprensa_oficial(page, alvo_url)

def processar_cidade(cidade_nome, alvo_url, palavras_chave_manual="", forcar=False):
    relatorio_geral = []
    garantir_keyword_manual()
    
    if os.path.exists(ARQUIVO_DEBUG): os.remove(ARQUIVO_DEBUG)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(alvo_url, timeout=60000)
            page.wait_for_timeout(3000)
            
            links_candidatos = detectar_layout_e_extrair(page, alvo_url)
            
            if not links_candidatos: 
                try: page.screenshot(path=f"erro_layout_{cidade_nome}.png")
                except: pass
                return "🚨 CRÍTICO: Zero arquivos encontrados! O site pode ter mudado o layout ou está offline. Verifique manualmente."

            session_main = SessionLocal()
            tarefas = []
            contador = 0
            try:
                for link in links_candidatos:
                    contador += 1
                    if not forcar:
                        if session_main.execute(text("SELECT id FROM diarios_log WHERE link_pdf = :link"), {"link": link}).fetchone(): continue
                    else:
                        ja_existe = session_main.execute(text("SELECT id FROM diarios_log WHERE link_pdf = :link"), {"link": link}).fetchone()
                        if ja_existe:
                            did = ja_existe[0]
                            session_main.execute(text("DELETE FROM ocorrencias WHERE diario_id = :did"), {"did": did})
                            session_main.execute(text("DELETE FROM diarios_log WHERE id = :did"), {"did": did})
                            session_main.commit()

                    tarefas.append({
                        "link": link, 
                        "cidade": cidade_nome, 
                        "contador": contador, 
                        "palavras_extras": palavras_chave_manual,
                        "modo_bruto": forcar 
                    })
            finally: session_main.close()

            if not tarefas: return "⚠️ Nenhum arquivo novo."

            with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
                resultados = list(executor.map(worker_processar_pdf, tarefas))
            
            relatorio_geral.extend(resultados)

        except Exception as e: return f"❌ ERRO CRÍTICO: {str(e)}"
        finally: browser.close()
    
    return " | ".join(relatorio_geral)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        task_id = sys.argv[1]
        cidade = sys.argv[2]
        url = sys.argv[3]
        kws = sys.argv[4]
        forcar = sys.argv[5].lower() == 'true'
        try:
            salvar_status_tarefa(task_id, "RODANDO")
            resultado = processar_cidade(cidade, url, kws, forcar)
            salvar_status_tarefa(task_id, "CONCLUIDO", resultado)
        except Exception as e:
            salvar_status_tarefa(task_id, "ERRO", str(e))
    else:
        print("Robô V29 pronto.")

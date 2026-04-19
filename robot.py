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

EXTENSOES_NAO_PDF = {'.pptx', '.docx', '.xlsx', '.ppt', '.doc', '.xls',
                     '.odt', '.ods', '.odp', '.csv', '.zip', '.rar'}


def _is_non_pdf_extension(url):
    """Check if a URL ends with a known non-PDF document extension."""
    from urllib.parse import urlparse
    path = urlparse(url).path.lower()
    _, ext = os.path.splitext(path)
    return ext in EXTENSOES_NAO_PDF

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
        if "logs" not in tarefa_atual: tarefa_atual["logs"] = []
        todos_status[task_id] = tarefa_atual
        with open(ARQUIVO_STATUS, "w") as f: json.dump(todos_status, f, indent=4)
    except: pass

def adicionar_log(task_id, mensagem):
    """Adiciona uma linha de log em tempo real para acompanhamento."""
    try:
        todos_status = ler_status()
        tarefa = todos_status.get(task_id, {})
        if "logs" not in tarefa: tarefa["logs"] = []
        tarefa["logs"].append({"ts": str(time.time()), "msg": mensagem})
        # Mantém no máximo 100 logs
        if len(tarefa["logs"]) > 100: tarefa["logs"] = tarefa["logs"][-100:]
        todos_status[task_id] = tarefa
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

def _baixar_via_playwright(url, referer, caminho_destino):
    """Fallback: usa Playwright para baixar PDF contornando proteções anti-bot."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            if referer:
                page.goto(referer, timeout=20000)
                page.wait_for_timeout(1000)
            with page.expect_download(timeout=30000) as dl_info:
                page.goto(url, timeout=30000)
            download = dl_info.value
            download.save_as(caminho_destino)
            browser.close()
            # Verifica se é PDF válido
            with open(caminho_destino, 'rb') as f:
                return f.read(4) == b'%PDF'
    except:
        return False

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
            cookies_req = dados_pacote.get('cookies', {})
            referer_url = dados_pacote.get('referer', '')
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': referer_url
            }
            resp = requests.get(link_pdf, stream=True, verify=False, timeout=60, headers=headers, cookies=cookies_req)
            if resp.status_code == 200:
                content_type = resp.headers.get('Content-Type', '')
                # Detecta se é realmente um PDF (Content-Type ou magic bytes %PDF)
                is_pdf_content_type = 'pdf' in content_type.lower() or 'octet-stream' in content_type.lower()
                
                # Coleta os primeiros bytes para verificar magic bytes independente do Content-Type
                primeiros_bytes = b''
                chunks_restantes = []
                for chunk in resp.iter_content(8192):
                    if not primeiros_bytes:
                        primeiros_bytes = chunk
                    else:
                        chunks_restantes.append(chunk)
                
                if not is_pdf_content_type and not primeiros_bytes.startswith(b'%PDF'):
                    # Fallback: tenta baixar via Playwright (contorna anti-bot)
                    baixado = _baixar_via_playwright(link_pdf, referer_url, caminho_final)
                    if not baixado:
                        return f"⚠️ Doc {contador}: não é PDF ({content_type.split(';')[0].strip()}), ignorado"
                else:
                    with open(caminho_final, "wb") as f:
                        f.write(primeiros_bytes)
                        for chunk in chunks_restantes: f.write(chunk)
            else: return f"❌ Erro HTTP {contador} (status {resp.status_code})"
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

def extrair_links_universal(page, alvo_url):
    """Extrator universal de PDFs — funciona com qualquer layout de site."""
    import re
    from urllib.parse import urljoin, urlparse
    
    parsed = urlparse(alvo_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    
    links_pdf = []
    links_promissores = []
    seen = set()
    
    # === FASE 1: Intercepta PDFs carregados via rede (AJAX, JS dinâmico) ===
    pdfs_rede = []
    ajax_data = []
    
    def on_response(res):
        url = res.url
        # Captura PDFs diretos na rede
        content_type = res.headers.get("content-type", "")
        if ".pdf" in url.lower() or "pdf" in content_type.lower():
            if url not in seen:
                pdfs_rede.append(url)
                seen.add(url)
        # Captura respostas AJAX que podem conter GUIDs (portalfacil)
        if "ajaxpro" in url and "diel_diel_lis" in url:
            try:
                body = res.body().decode("utf-8", errors="ignore")
                if "NMARQUIVO" in body:
                    ajax_data.append(body)
            except: pass
    
    page.on("response", on_response)
    page.wait_for_timeout(2000)
    
    # Espera extra para SPAs — se a URL tem hash (#), espera o JS carregar o conteúdo
    if '#' in alvo_url:
        page.wait_for_timeout(5000)
    
    # === FASE 2: Coleta todos os links da página ===
    elementos = page.locator("a").all()
    for link in elementos:
        try:
            href = link.get_attribute("href") or ""
            if not href or href == "#" or href.startswith("javascript:void"): continue
            
            href_lower = href.lower()
            href_full = urljoin(base_url, href) if not href.startswith("http") else href
            
            # Links javascript com funções de diário (portalfacil)
            if href.startswith("javascript:") and "GetDiario(" in href and "Calendar" not in href:
                try:
                    link.click(timeout=5000)
                    page.wait_for_timeout(3000)
                except: pass
                continue
            
            if href.startswith("javascript:"): continue
            
            # PDF direto
            if ".pdf" in href_lower:
                if href_full not in seen:
                    links_pdf.append(href_full)
                    seen.add(href_full)
                continue
            
            # Links promissores para navegar depois
            termos = ["download", "visualizar", "prepara-pdf", "publicacao", "exibe_do", 
                      "integra", "anexo", "edicao", "doe.php", "diario", "baixar", "/ver/"]
            
            # Links de download direto (sem extensão = provavelmente PDF servido dinamicamente)
            is_download_link = "download" in href_lower or "baixar" in href_lower or "downloadencrypted" in href_lower
            has_no_extension = '.' not in href_full.split('/')[-1].split('?')[0] or 'download' in href_lower.split('?')[0]
            
            if is_download_link and has_no_extension and not _is_non_pdf_extension(href_full):
                if href_full not in seen:
                    links_pdf.append(href_full)
                    seen.add(href_full)
            elif any(t in href_lower for t in termos):
                if href_full not in seen:
                    links_promissores.append(href_full)
                    seen.add(href_full)
        except: continue
    
    # Captura iframes com PDFs
    for iframe in page.locator("iframe").all():
        try:
            src = iframe.get_attribute("src") or ""
            if ".pdf" in src.lower():
                src_full = urljoin(base_url, src) if not src.startswith("http") else src
                if src_full not in seen:
                    links_pdf.append(src_full)
                    seen.add(src_full)
        except: continue
    
    # === FASE 2.5: Busca links no HTML renderizado (SPAs geram links via JS) ===
    try:
        html = page.content()
        # Procura URLs de download em qualquer atributo ou string JS (não só href)
        download_urls = re.findall(r'["\']([^"\']*(?:download|baixar)[^"\']*)["\']', html, re.IGNORECASE)
        for href in download_urls:
            if href.startswith("javascript") or href == "#" or len(href) < 10: continue
            if not href.startswith("http") and not href.startswith("/"):continue
            href_full = urljoin(base_url, href) if not href.startswith("http") else href
            if href_full not in seen and not _is_non_pdf_extension(href_full):
                links_pdf.append(href_full)
                seen.add(href_full)
    except: pass
    
    page.remove_listener("response", on_response)
    
    # Adiciona PDFs capturados via rede
    links_pdf.extend(pdfs_rede)
    
    # === FASE 3: Processa GUIDs do AjaxPro (portalfacil) ===
    if ajax_data:
        guids = []
        for body in ajax_data:
            guids += re.findall(r'\{([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})\}', body)
        for guid in guids:
            guid_upper = guid.upper()
            url_pdf = f"https://{parsed.netloc}/abrir_arquivo.aspx?cdLocal=12&arquivo=%7B{guid_upper}%7D.pdf"
            if url_pdf not in seen:
                links_pdf.append(url_pdf)
                seen.add(url_pdf)
    
    # === FASE 4: Se não achou PDFs diretos, navega nos links promissores ===
    if not links_pdf and links_promissores:
        for pag_url in links_promissores[:10]:
            try:
                p2 = page.context.new_page()
                
                # Intercepta PDFs na rede da sub-página
                sub_pdfs = []
                def on_sub_response(res):
                    if ".pdf" in res.url.lower() or "pdf" in res.headers.get("content-type", "").lower():
                        if res.url not in seen:
                            sub_pdfs.append(res.url)
                            seen.add(res.url)
                p2.on("response", on_sub_response)
                
                p2.goto(pag_url, timeout=20000)
                p2.wait_for_timeout(2000)
                
                # Procura PDFs nos links e iframes da sub-página
                for el in p2.locator("a, iframe").all():
                    try:
                        href = el.get_attribute("href") or el.get_attribute("src") or ""
                        href_full = urljoin(pag_url, href) if not href.startswith("http") else href
                        href_lower = href.lower()
                        
                        # PDF direto
                        if ".pdf" in href_lower:
                            if href_full not in seen and not _is_non_pdf_extension(href_full):
                                links_pdf.append(href_full)
                                seen.add(href_full)
                        # Link de download sem extensão (provavelmente PDF dinâmico)
                        elif ("download" in href_lower or "baixar" in href_lower):
                            has_no_ext = '.' not in href_full.split('/')[-1].split('?')[0]
                            if has_no_ext and href_full not in seen and not _is_non_pdf_extension(href_full):
                                links_pdf.append(href_full)
                                seen.add(href_full)
                    except: continue
                
                links_pdf.extend(sub_pdfs)
                p2.close()
            except: pass
    
    # === FASE 5: Fallback — links com termos de interesse que sobraram ===
    if not links_pdf:
        termos_fallback = ["pdf", "download", "arquivo", "publicacao"]
        for link in page.locator("a").all():
            try:
                href = link.get_attribute("href") or ""
                if href.startswith("javascript") or href == "#": continue
                href_full = urljoin(base_url, href) if not href.startswith("http") else href
                if any(t in href.lower() for t in termos_fallback) and href_full not in seen:
                    if not _is_non_pdf_extension(href_full):
                        links_pdf.append(href_full)
                        seen.add(href_full)
            except: continue
    
    return [link for link in links_pdf if not _is_non_pdf_extension(link)]

def detectar_layout_e_extrair(page, alvo_url):
    """Usa o extrator universal para qualquer site."""
    return extrair_links_universal(page, alvo_url)

def processar_cidade(cidade_nome, alvo_url, palavras_chave_manual="", forcar=False, task_id=None):
    relatorio_geral = []
    garantir_keyword_manual()
    
    if os.path.exists(ARQUIVO_DEBUG): os.remove(ARQUIVO_DEBUG)

    def _log(msg):
        if task_id: adicionar_log(task_id, msg)

    _log(f"🌐 Acessando {alvo_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(alvo_url, timeout=60000)
            page.wait_for_timeout(3000)
            
            _log("🔍 Procurando links de PDF na página...")
            links_candidatos = detectar_layout_e_extrair(page, alvo_url)
            
            if not links_candidatos: 
                _log("❌ Nenhum PDF encontrado na página")
                try: page.screenshot(path=f"erro_layout_{cidade_nome}.png")
                except: pass
                return "🚨 CRÍTICO: Zero arquivos encontrados! O site pode ter mudado o layout ou está offline."

            _log(f"📄 {len(links_candidatos)} link(s) encontrado(s)")

            cookies_playwright = page.context.cookies()
            cookies_dict = {c['name']: c['value'] for c in cookies_playwright}

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
                        "modo_bruto": forcar,
                        "cookies": cookies_dict,
                        "referer": alvo_url
                    })
            finally: session_main.close()

            if not tarefas:
                _log("⚠️ Nenhum arquivo novo para processar")
                return "⚠️ Nenhum arquivo novo."

            _log(f"⬇️ Baixando e processando {len(tarefas)} documento(s)...")

            with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
                resultados = list(executor.map(worker_processar_pdf, tarefas))
            
            for r in resultados:
                _log(r)
            relatorio_geral.extend(resultados)

        except Exception as e:
            _log(f"❌ ERRO: {str(e)}")
            return f"❌ ERRO CRÍTICO: {str(e)}"
        finally: browser.close()
    
    _log("✅ Processamento concluído")
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
            adicionar_log(task_id, f"🚀 Iniciando busca em {cidade}")
            resultado = processar_cidade(cidade, url, kws, forcar, task_id=task_id)
            salvar_status_tarefa(task_id, "CONCLUIDO", resultado)
        except Exception as e:
            salvar_status_tarefa(task_id, "ERRO", str(e))
    else:
        print("Robô V29 pronto.")

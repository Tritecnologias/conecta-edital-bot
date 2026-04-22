"""
Diagnóstico de URL — analisa qualquer site e mostra o que o robô vê.
Roda como subprocess para não travar a API.
"""
import sys
import json
import re
import os
import time
import requests
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright

ARQUIVO_STATUS = "status_tarefas.json"

def ler_status():
    if not os.path.exists(ARQUIVO_STATUS): return {}
    try:
        with open(ARQUIVO_STATUS, "r") as f: return json.load(f)
    except: return {}

def salvar_diag(task_id, dados):
    try:
        todos = ler_status()
        tarefa = todos.get(task_id, {})
        tarefa.update(dados)
        tarefa["updated_at"] = str(time.time())
        todos[task_id] = tarefa
        with open(ARQUIVO_STATUS, "w") as f: json.dump(todos, f, indent=4)
    except: pass

def add_log(task_id, msg):
    try:
        todos = ler_status()
        tarefa = todos.get(task_id, {})
        if "logs" not in tarefa: tarefa["logs"] = []
        tarefa["logs"].append({"ts": str(time.time()), "msg": msg})
        todos[task_id] = tarefa
        with open(ARQUIVO_STATUS, "w") as f: json.dump(todos, f, indent=4)
    except: pass

def diagnosticar(task_id, url_alvo):
    salvar_diag(task_id, {"status": "RODANDO"})
    add_log(task_id, f"🔍 Analisando: {url_alvo}")
    
    parsed = urlparse(url_alvo)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    resultado = {
        "url": url_alvo,
        "base": base_url,
        "is_spa": "#" in url_alvo,
        "links_pdf_direto": [],
        "links_download": [],
        "links_promissores": [],
        "links_diario": [],
        "iframes": [],
        "pdfs_na_rede": [],
        "downloadEncrypted": [],
        "botoes_baixar": 0,
        "total_links": 0,
        "html_size": 0,
    }
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            
            # Intercepta rede
            pdfs_rede = []
            def on_response(res):
                ct = res.headers.get("content-type", "")
                if "pdf" in ct.lower():
                    pdfs_rede.append({"url": res.url[:200], "content_type": ct})
            page.on("response", on_response)
            
            add_log(task_id, "🌐 Carregando página...")
            page.goto(url_alvo, timeout=30000)
            
            wait_time = 10000 if "#" in url_alvo else 5000
            add_log(task_id, f"⏳ Aguardando {wait_time//1000}s para JS carregar...")
            page.wait_for_timeout(wait_time)
            
            html = page.content()
            resultado["html_size"] = len(html)
            add_log(task_id, f"📄 HTML: {len(html)} chars")
            
            # Screenshot
            page.screenshot(path=f"diag_{task_id[:8]}.png", full_page=True)
            
            # Analisa todos os links
            links = page.locator("a").all()
            resultado["total_links"] = len(links)
            add_log(task_id, f"🔗 {len(links)} links encontrados")
            
            for link in links:
                try:
                    href = link.get_attribute("href") or ""
                    txt = link.inner_text().strip()[:60]
                    if not href or href == "#" or href.startswith("javascript:void"): continue
                    
                    href_lower = href.lower()
                    href_full = urljoin(base_url, href) if not href.startswith("http") else href
                    
                    if href.startswith("javascript:"): continue
                    
                    info = {"href": href_full[:200], "texto": txt}
                    
                    if ".pdf" in href_lower:
                        resultado["links_pdf_direto"].append(info)
                    elif "download" in href_lower or "baixar" in href_lower:
                        resultado["links_download"].append(info)
                    elif any(t in href_lower for t in ["diario", "publicac", "edicao", "edição"]):
                        resultado["links_diario"].append(info)
                    elif any(t in href_lower for t in ["visualizar", "prepara-pdf", "exibe_do", "integra", "anexo", "ver/"]):
                        resultado["links_promissores"].append(info)
                except: continue
            
            # Iframes
            for iframe in page.locator("iframe").all():
                try:
                    src = iframe.get_attribute("src") or ""
                    if src and src != "None":
                        resultado["iframes"].append(src[:200])
                except: pass
            
            # downloadEncrypted no HTML
            encrypted = re.findall(r'["\']([^"\']*downloadEncrypted[^"\']*)["\']', html, re.IGNORECASE)
            resultado["downloadEncrypted"] = [e[:200] for e in encrypted]
            
            # Botões Baixar
            baixar_count = html.lower().count("baixar")
            resultado["botoes_baixar"] = baixar_count
            
            # PDFs na rede
            resultado["pdfs_na_rede"] = pdfs_rede
            
            page.remove_listener("response", on_response)
            
            # Testa Content-Type dos primeiros links
            add_log(task_id, "🧪 Testando Content-Type dos links encontrados...")
            links_para_testar = (resultado["links_pdf_direto"][:3] + 
                                resultado["links_download"][:3] + 
                                resultado["links_diario"][:2])
            
            testes = []
            for link_info in links_para_testar:
                try:
                    r = requests.head(link_info["href"], verify=False, timeout=10, allow_redirects=True,
                                     headers={"User-Agent": "Mozilla/5.0"})
                    ct = r.headers.get("content-type", "?")
                    disp = r.headers.get("content-disposition", "")
                    testes.append({
                        "url": link_info["href"][:150],
                        "status": r.status_code,
                        "content_type": ct,
                        "disposition": disp[:100],
                        "is_pdf": "pdf" in ct.lower()
                    })
                    add_log(task_id, f"  {'✅' if 'pdf' in ct.lower() else '❌'} {link_info['href'][:80]} → {ct.split(';')[0]}")
                except Exception as e:
                    testes.append({"url": link_info["href"][:150], "erro": str(e)[:100]})
            
            resultado["testes_content_type"] = testes
            
            # Resumo
            add_log(task_id, "")
            add_log(task_id, "📊 RESUMO DO DIAGNÓSTICO:")
            add_log(task_id, f"  PDFs diretos: {len(resultado['links_pdf_direto'])}")
            add_log(task_id, f"  Links download: {len(resultado['links_download'])}")
            add_log(task_id, f"  Links diário: {len(resultado['links_diario'])}")
            add_log(task_id, f"  Links promissores: {len(resultado['links_promissores'])}")
            add_log(task_id, f"  Iframes: {len(resultado['iframes'])}")
            add_log(task_id, f"  downloadEncrypted: {len(resultado['downloadEncrypted'])}")
            add_log(task_id, f"  Botões 'Baixar': {resultado['botoes_baixar']}")
            add_log(task_id, f"  PDFs na rede: {len(resultado['pdfs_na_rede'])}")
            add_log(task_id, f"  É SPA: {'Sim' if resultado['is_spa'] else 'Não'}")
            
            pdfs_reais = [t for t in testes if t.get("is_pdf")]
            if pdfs_reais:
                add_log(task_id, f"  ✅ {len(pdfs_reais)} PDF(s) real(is) confirmado(s)")
            else:
                add_log(task_id, f"  ⚠️ Nenhum PDF real confirmado via Content-Type")
            
            context.close()
            browser.close()
    except Exception as e:
        add_log(task_id, f"❌ ERRO: {str(e)}")
        resultado["erro"] = str(e)
    
    add_log(task_id, "✅ Diagnóstico concluído")
    salvar_diag(task_id, {"status": "CONCLUIDO", "resultado": json.dumps(resultado, ensure_ascii=False)})

if __name__ == "__main__":
    task_id = sys.argv[1]
    url = sys.argv[2]
    try:
        diagnosticar(task_id, url)
    except Exception as e:
        salvar_diag(task_id, {"status": "ERRO", "resultado": str(e)})

from playwright.sync_api import sync_playwright

URL = "https://imprensaoficialmunicipal.com.br/rio_claro"

with sync_playwright() as p:
    print("🕵️ Investigando o site...")
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    
    # Acessa e espera a rede parar de carregar coisas (networkidle)
    page.goto(URL)
    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except:
        print("⚠️ O site demorou a carregar, mas vamos tentar ler mesmo assim.")

    print(f"PAGE TITLE: {page.title()}")
    
    # Tira um print pra salvar caso precise baixar depois
    page.screenshot(path="debug_tela.png")
    
    # Procura TODOS os links da página
    links = page.locator("a").all()
    print(f"🔗 Total de links encontrados: {len(links)}")
    
    print("--- 10 PRIMEIROS LINKS ENCONTRADOS ---")
    count = 0
    for link in links:
        try:
            href = link.get_attribute("href")
            text = link.inner_text().strip()
            if href and len(text) > 0:
                print(f"Texto: '{text}' | Link: {href}")
                count += 1
                if count >= 10: break
        except:
            pass

    browser.close()

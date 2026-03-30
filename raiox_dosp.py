from playwright.sync_api import sync_playwright

# Link real que descobrimos na etapa anterior
URL_DOSP = "https://dosp.com.br/exibe_do.php?i=NzYxMTcw"

with sync_playwright() as p:
    print(f"🕵️ Invadindo o DOSP: {URL_DOSP}")
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    
    page.goto(URL_DOSP)
    # Espera o visualizador carregar
    page.wait_for_timeout(5000)

    print("\n--- 1. BUSCANDO O ARQUIVO PDF (Embed/Iframe) ---")
    # Geralmente fica dentro de um embed ou iframe
    elementos = page.locator("iframe, embed, object").all()
    for el in elementos:
        src = el.get_attribute("src") or el.get_attribute("data")
        print(f"📄 Achei um visualizador: {src}")

    print("\n--- 2. BUSCANDO BOTÃO DE DOWNLOAD ---")
    # Procura botões com ícone de download ou texto baixar
    botoes = page.locator("a[href*='download'], a[href*='.pdf'], a#download").all()
    for b in botoes:
        href = b.get_attribute("href")
        print(f"⬇️ Botão de Download: {href}")

    print("\n--- 3. VARREDURA NO HTML (Scripts escondidos) ---")
    html = page.content()
    if ".pdf" in html:
        # Pega a posição onde aparece .pdf pra gente ver o contexto
        pos = html.find(".pdf")
        print(f"💡 Dica no código fonte: ...{html[pos-80:pos+20]}...")

    browser.close()

from playwright.sync_api import sync_playwright

URL = "https://imprensaoficialmunicipal.com.br/rio_claro"

with sync_playwright() as p:
    print(f"🕵️ Varrendo: {URL}")
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto(URL)
    
    # Espera carregar bem
    page.wait_for_timeout(5000) 

    print("\n--- TENTATIVA 1: Links com 'pdf' ou 'visualizar' ---")
    links = page.locator("a").all()
    
    encontrados = 0
    for link in links:
        try:
            href = link.get_attribute("href")
            texto = link.inner_text().strip()
            
            # Filtra só o que interessa
            if href and ("pdf" in href.lower() or "visualizar" in href.lower() or "edicao" in href.lower()):
                print(f"🎯 ALVO: Texto='{texto}' | Link='{href}'")
                encontrados += 1
        except:
            pass

    if encontrados == 0:
        print("⚠️ Nenhum link óbvio encontrado nos textos.")

    print("\n--- TENTATIVA 2: Imagens que são links (Capa do Jornal) ---")
    # Muitas vezes o diário é uma imagem clicável
    imagens_link = page.locator("a img").all()
    for img in imagens_link:
        try:
            pai = img.locator("..") # Pega o elemento 'a' pai da imagem
            href = pai.get_attribute("href")
            alt = img.get_attribute("alt")
            print(f"🖼️ Imagem Link: Alt='{alt}' | Destino='{href}'")
        except:
            pass
            
    browser.close()

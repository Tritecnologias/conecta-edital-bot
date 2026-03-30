from sqlalchemy import text
from database import SessionLocal

def obter_relatorio_api():
    """
    Função otimizada para a API.
    Retorna uma lista de objetos (JSON) em vez de imprimir texto.
    """
    session = SessionLocal()
    
    # SQL query para pegar tudo o que foi achado
    sql = text("""
        SELECT d.cidade, d.data_publicacao, d.link_pdf, k.termo, o.pagina, o.trecho_encontrado
        FROM ocorrencias o
        JOIN diarios_log d ON o.diario_id = d.id
        JOIN keywords k ON o.keyword_id = k.id
        ORDER BY d.processado_em DESC, o.pagina ASC
        LIMIT 50
    """)
    
    resultados = session.execute(sql).fetchall()
    session.close()

    dados_formatados = []
    
    for linha in resultados:
        # Cria um dicionário bonito para cada alerta
        item = {
            "cidade": linha.cidade,
            "data": str(linha.data_publicacao),
            "termo_buscado": linha.termo,
            "pagina": linha.pagina,
            "trecho_encontrado": linha.trecho_encontrado.strip(),
            "link_original": linha.link_pdf
        }
        dados_formatados.append(item)
        
    return dados_formatados

# Mantivemos a função antiga caso você queira rodar no terminal (opcional)
if __name__ == "__main__":
    dados = obter_relatorio_api()
    print(f"Foram encontrados {len(dados)} alertas.")

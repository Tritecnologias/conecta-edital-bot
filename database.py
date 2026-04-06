import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# Configuração da Conexão
# Estrutura: postgresql://usuario:senha@localhost/nome_do_banco
DATABASE_URL = "postgresql://crawler_user:MudarEssaSenha123@localhost/diario_db"

# Cria a "Engine" (o motor de conexão)
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)

# Cria a "Session" (a sessão que vamos usar para mandar comandos)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db_connection():
    """Testa se a conexão está funcionando"""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version()"))
            print("✅ Conexão com Banco de Dados: SUCESSO!")
            print(f"Versão: {result.fetchone()[0]}")
    except Exception as e:
        print("❌ Erro ao conectar no banco:")
        print(e)

if __name__ == "__main__":
    get_db_connection()

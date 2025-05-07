# src/database/db_connector.py
import os
import psycopg2
from psycopg2 import OperationalError

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database using DATABASE_URL.

    Returns:
        psycopg2.connection or None: A connection object or None if connection fails.
    """
    if not DATABASE_URL:
        print("Erro: A variável de ambiente DATABASE_URL não está definida.")
        return None
    try:
        print(f"Tentando conectar ao PostgreSQL com DATABASE_URL: {DATABASE_URL[:DATABASE_URL.find('://')+3]}******** e timeout de 10 segundos...")
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        print("Conexão com o banco de dados PostgreSQL estabelecida com sucesso!")
        return conn
    except OperationalError as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def test_db_connection():
    """Tests the database connection and prints the PostgreSQL version.

    Returns:
        bool: True if connection is successful and version is retrieved, False otherwise.
    """
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                db_version = cur.fetchone()
                print(f"Versão do PostgreSQL: {db_version}")
            conn.close()
            return True
        else:
            return False
    except (Exception, psycopg2.Error) as error:
        print(f"Erro durante o teste de conexão com o PostgreSQL: {error}")
        if conn:
            conn.close()
        return False

if __name__ == '__main__':
    print("Iniciando teste de conexão com o banco de dados...")
    # Para testar localmente, você precisaria definir a DATABASE_URL no seu ambiente
    # Exemplo: export DATABASE_URL="postgresql://user:password@host:port/dbname"
    if "DATABASE_URL" not in os.environ:
        print("AVISO: DATABASE_URL não está definida no ambiente.")
        print("Para um teste efetivo, defina DATABASE_URL antes de executar este script.")
        print("Exemplo: export DATABASE_URL=\"postgresql://user:password@host:port/dbname\"")
    else:
        print(f"Tentando conectar usando DATABASE_URL: {DATABASE_URL[:DATABASE_URL.find('://')+3]}********")
        
    if test_db_connection():
        print("Teste de conexão com o banco de dados bem-sucedido.")
    else:
        print("Falha no teste de conexão com o banco de dados.")


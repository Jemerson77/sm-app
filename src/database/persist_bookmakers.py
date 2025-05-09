import os
import logging
from datetime import datetime
from src.database.db_connector import get_db_connection

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def upsert_bookmakers(conn, bookmakers_data):
    if not bookmakers_data:
        logging.info("Nenhum dado de bookmaker para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO "Bookmakers" (id_bookmaker, nome_bookmaker, logo_url, atualizado_em)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id_bookmaker)
        DO UPDATE SET
            nome_bookmaker = EXCLUDED.nome_bookmaker,
            logo_url = EXCLUDED.logo_url,
            atualizado_em = EXCLUDED.atualizado_em;
        """
        for bookmaker in bookmakers_data:
            try:
                cursor.execute(sql, (
                    bookmaker.get('id_bookmaker'),
                    bookmaker.get('nome_bookmaker'),
                    bookmaker.get('logo_url'),
                    datetime.now()
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT do bookmaker ID {bookmaker.get('id_bookmaker')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de Bookmakers: {e}")
        if conn:
            conn.rollback()
        failure_count = len(bookmakers_data) - success_count
    finally:
        if cursor:
            cursor.close()
    
    logging.info(f"UPSERT de Bookmakers concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

if __name__ == "__main__":
    logging.info("Iniciando script de persistência de Bookmakers (exemplo)...")
    
    sample_bookmakers_data = [
        {'id_bookmaker': 1, 'nome_bookmaker': 'Bet365', 'logo_url': 'https://example.com/logo_bet365.png'},
        {'id_bookmaker': 2, 'nome_bookmaker': 'Pinnacle', 'logo_url': 'https://example.com/logo_pinnacle.png'},
        {'id_bookmaker': 8, 'nome_bookmaker': 'Betfair', 'logo_url': 'https://example.com/logo_betfair.png'},
        # Adicione outros bookmakers conforme necessário da API SportsMonks
    ]

    conn = None
    try:
        conn = get_db_connection()
        if conn:
            logging.info("Processando dados de Bookmakers...")
            upsert_bookmakers(conn, sample_bookmakers_data)
            logging.info("Script de persistência de Bookmakers (exemplo) concluído.")
        else:
            logging.error("Não foi possível conectar ao banco de dados.")
    except Exception as e:
        logging.error(f"Erro fatal no script de persistência de Bookmakers: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


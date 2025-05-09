import os
import logging
from datetime import datetime
from src.database.db_connector import get_db_connection

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def upsert_markets(conn, markets_data):
    if not markets_data:
        logging.info("Nenhum dado de market para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO "Markets" (id_market, nome_market, descricao_market, atualizado_em)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id_market)
        DO UPDATE SET
            nome_market = EXCLUDED.nome_market,
            descricao_market = EXCLUDED.descricao_market,
            atualizado_em = EXCLUDED.atualizado_em;
        """
        for market in markets_data:
            try:
                cursor.execute(sql, (
                    market.get('id_market'),
                    market.get('nome_market'),
                    market.get('descricao_market'),
                    datetime.now()
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT do market ID {market.get('id_market')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de Markets: {e}")
        if conn:
            conn.rollback()
        failure_count = len(markets_data) - success_count
    finally:
        if cursor:
            cursor.close()
    
    logging.info(f"UPSERT de Markets concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

if __name__ == "__main__":
    logging.info("Iniciando script de persistência de Markets (exemplo)...")
    
    sample_markets_data = [
        {'id_market': 1, 'nome_market': 'Match Winner', 'descricao_market': 'Aposta no vencedor da partida (1X2)'},
        {'id_market': 2, 'nome_market': 'Asian Handicap', 'descricao_market': 'Aposta com handicap asiático'},
        {'id_market': 3, 'nome_market': 'Over/Under Goals', 'descricao_market': 'Aposta no total de gols (Acima/Abaixo)'},
        # Adicione outros markets conforme necessário da API SportsMonks
    ]

    conn = None
    try:
        conn = get_db_connection()
        if conn:
            logging.info("Processando dados de Markets...")
            upsert_markets(conn, sample_markets_data)
            logging.info("Script de persistência de Markets (exemplo) concluído.")
        else:
            logging.error("Não foi possível conectar ao banco de dados.")
    except Exception as e:
        logging.error(f"Erro fatal no script de persistência de Markets: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


import os
import logging
from datetime import datetime
from src.database.db_connector import get_db_connection

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def upsert_status_partida(conn, status_data):
    if not status_data:
        logging.info("Nenhum dado de status de partida para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO "StatusPartida" (id_status_partida, nome_status, codigo_curto_status, atualizado_em)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id_status_partida)
        DO UPDATE SET
            nome_status = EXCLUDED.nome_status,
            codigo_curto_status = EXCLUDED.codigo_curto_status,
            atualizado_em = EXCLUDED.atualizado_em;
        """
        for status in status_data:
            try:
                cursor.execute(sql, (
                    status.get('id_status_partida'),
                    status.get('nome_status'),
                    status.get('codigo_curto_status'),
                    datetime.now()
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT do status ID {status.get('id_status_partida')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de StatusPartida: {e}")
        if conn:
            conn.rollback()
        failure_count = len(status_data) - success_count
    finally:
        if cursor:
            cursor.close()
    
    logging.info(f"UPSERT de StatusPartida concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

def upsert_status_partida_traducoes(conn, traducoes_data):
    if not traducoes_data:
        logging.info("Nenhum dado de tradução de status de partida para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO "StatusPartida_Traducoes" (id_status_partida, codigo_idioma, nome_traduzido)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_status_partida, codigo_idioma)
        DO UPDATE SET
            nome_traduzido = EXCLUDED.nome_traduzido;
        """
        for traducao in traducoes_data:
            try:
                cursor.execute(sql, (
                    traducao.get('id_status_partida'),
                    traducao.get('codigo_idioma'),
                    traducao.get('nome_traduzido')
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT da tradução do status ID {traducao.get('id_status_partida')}, idioma {traducao.get('codigo_idioma')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de Traduções de StatusPartida: {e}")
        if conn:
            conn.rollback()
        failure_count = len(traducoes_data) - success_count
    finally:
        if cursor:
            cursor.close()

    logging.info(f"UPSERT de Traduções de StatusPartida concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

if __name__ == "__main__":
    logging.info("Iniciando script de persistência de StatusPartida (exemplo)...")
    
    sample_status_data = [
        {'id_status_partida': 1, 'nome_status': 'Not Started', 'codigo_curto_status': 'NS'},
        {'id_status_partida': 2, 'nome_status': 'Live', 'codigo_curto_status': 'LIVE'},
        {'id_status_partida': 3, 'nome_status': 'Half Time', 'codigo_curto_status': 'HT'},
        {'id_status_partida': 4, 'nome_status': 'Finished', 'codigo_curto_status': 'FT'},
        {'id_status_partida': 5, 'nome_status': 'Postponed', 'codigo_curto_status': 'POSTP'},
        {'id_status_partida': 6, 'nome_status': 'Cancelled', 'codigo_curto_status': 'CANC'},
        {'id_status_partida': 17, 'nome_status': 'Finished (Pens)', 'codigo_curto_status': 'FT_PEN'},
        # Adicione outros status conforme necessário da API SportsMonks
    ]

    sample_traducoes_data = [
        {'id_status_partida': 1, 'codigo_idioma': 'pt', 'nome_traduzido': 'Não Iniciada'},
        {'id_status_partida': 1, 'codigo_idioma': 'en', 'nome_traduzido': 'Not Started'},
        {'id_status_partida': 1, 'codigo_idioma': 'es', 'nome_traduzido': 'No Empezado'},
        {'id_status_partida': 2, 'codigo_idioma': 'pt', 'nome_traduzido': 'Ao Vivo'},
        {'id_status_partida': 4, 'codigo_idioma': 'pt', 'nome_traduzido': 'Finalizada'},
        {'id_status_partida': 17, 'codigo_idioma': 'pt', 'nome_traduzido': 'Finalizada (Pênaltis)'},
    ]

    conn = None
    try:
        conn = get_db_connection()
        if conn:
            logging.info("Processando dados de StatusPartida...")
            upsert_status_partida(conn, sample_status_data)
            
            logging.info("Processando traduções de StatusPartida...")
            upsert_status_partida_traducoes(conn, sample_traducoes_data)
            
            logging.info("Script de persistência de StatusPartida (exemplo) concluído.")
        else:
            logging.error("Não foi possível conectar ao banco de dados.")
    except Exception as e:
        logging.error(f"Erro fatal no script de persistência de StatusPartida: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


import os
import logging
from datetime import datetime
# Importar o conector do banco de dados
from src.database.db_connector import get_db_connection

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def upsert_seasons(conn, seasons_data):
    """
    Insere ou atualiza dados na tabela Temporadas.
    A lógica de UPSERT (INSERT ON CONFLICT) garante que registros existentes sejam atualizados.
    `seasons_data` deve ser uma lista de dicionários, onde cada dicionário representa uma temporada.
    """
    if not seasons_data:
        logging.info("Nenhum dado de temporada para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        # Colunas conforme proposta_banco_de_dados.md: 
        # id_temporada (PK), id_liga (FK), nome_temporada, data_inicio, data_fim, eh_temporada_atual, criado_em, atualizado_em
        # A coluna criado_em pode ter um DEFAULT CURRENT_TIMESTAMP no banco, então não precisa ser explicitamente inserida/atualizada aqui se for o caso.
        # A coluna atualizado_em será gerenciada pelo script.
        sql = """
        INSERT INTO Temporadas (id_temporada, id_liga, nome_temporada, data_inicio, data_fim, eh_temporada_atual, atualizado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_temporada)
        DO UPDATE SET
            id_liga = EXCLUDED.id_liga,
            nome_temporada = EXCLUDED.nome_temporada,
            data_inicio = EXCLUDED.data_inicio,
            data_fim = EXCLUDED.data_fim,
            eh_temporada_atual = EXCLUDED.eh_temporada_atual,
            atualizado_em = EXCLUDED.atualizado_em;
        """
        for season in seasons_data:
            try:
                cursor.execute(sql, (
                    season.get('id_temporada'),
                    season.get('id_liga'),
                    season.get('nome_temporada'),
                    season.get('data_inicio'),
                    season.get('data_fim'),
                    season.get('eh_temporada_atual'),
                    datetime.now()
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT da temporada ID {season.get('id_temporada')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de Temporadas: {e}")
        if conn:
            conn.rollback()
        failure_count = len(seasons_data) - success_count
    finally:
        if cursor:
            cursor.close()
    
    logging.info(f"UPSERT de Temporadas concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

if __name__ == "__main__":
    logging.info("Iniciando script de persistência de Temporadas (exemplo)...")
    
    # Dados de exemplo - substitua isso pela chamada ao seu processador de temporadas
    # Ex: processed_data = process_seasons_data(api_data) # api_data viria do seu coletor
    sample_processed_seasons_data = [
        {'id_temporada': 21646, 'id_liga': 8, 'nome_temporada': '2023/2024', 'data_inicio': '2023-08-11', 'data_fim': '2024-05-19', 'eh_temporada_atual': True},
        {'id_temporada': 19734, 'id_liga': 8, 'nome_temporada': '2022/2023', 'data_inicio': '2022-08-05', 'data_fim': '2023-05-28', 'eh_temporada_atual': False},
        {'id_temporada': 21833, 'id_liga': 82, 'nome_temporada': '2023/2024', 'data_inicio': '2023-08-18', 'data_fim': '2024-05-18', 'eh_temporada_atual': True},
    ]

    conn = None
    try:
        conn = get_db_connection()
        if conn:
            logging.info("Processando dados das Temporadas...")
            upsert_seasons(conn, sample_processed_seasons_data)
            logging.info("Script de persistência de Temporadas (exemplo) concluído.")
        else:
            logging.error("Não foi possível conectar ao banco de dados.")
    except Exception as e:
        logging.error(f"Erro fatal no script de persistência de Temporadas: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


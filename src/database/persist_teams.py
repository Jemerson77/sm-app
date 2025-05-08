import os
import logging
from datetime import datetime
# Importar o conector do banco de dados e o processador de times
# Ajuste o caminho do import se a estrutura do seu projeto for diferente
from src.database.db_connector import get_db_connection
# Supondo que você tenha um processador de times similar ao de ligas
# from src.processors.teams_processor import process_teams_data # Descomente e ajuste se necessário

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def upsert_teams(conn, teams_data):
    """
    Insere ou atualiza dados na tabela Times.
    A lógica de UPSERT (INSERT ON CONFLICT) garante que registros existentes sejam atualizados.
    """
    if not teams_data:
        logging.info("Nenhum dado de time principal para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO Times (id_time, id_pais, nome_original, logo_url, fundacao, estadio_nome, estadio_capacidade, atualizado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_time)
        DO UPDATE SET
            id_pais = EXCLUDED.id_pais,
            nome_original = EXCLUDED.nome_original,
            logo_url = EXCLUDED.logo_url,
            fundacao = EXCLUDED.fundacao,
            estadio_nome = EXCLUDED.estadio_nome,
            estadio_capacidade = EXCLUDED.estadio_capacidade,
            atualizado_em = EXCLUDED.atualizado_em;
        """
        for team in teams_data:
            try:
                cursor.execute(sql, (
                    team.get('id_time'),
                    team.get('id_pais'),
                    team.get('nome_original'),
                    team.get('logo_url'),
                    team.get('fundacao'),
                    team.get('estadio_nome'),
                    team.get('estadio_capacidade'),
                    datetime.now()
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT do time ID {team.get('id_time')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de Times: {e}")
        if conn:
            conn.rollback()
        # Se o erro for na conexão ou no cursor, todos os times podem ser considerados falhas
        failure_count = len(teams_data) - success_count 
    finally:
        if cursor:
            cursor.close()
    
    logging.info(f"UPSERT de Times concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

def upsert_team_translations(conn, translations_data):
    """
    Insere ou atualiza dados na tabela Times_Traducoes.
    """
    if not translations_data:
        logging.info("Nenhum dado de tradução de time para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO Times_Traducoes (id_time, codigo_idioma, nome_traduzido)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_time, codigo_idioma)
        DO UPDATE SET
            nome_traduzido = EXCLUDED.nome_traduzido;
        """
        for translation in translations_data:
            try:
                cursor.execute(sql, (
                    translation.get('id_time'),
                    translation.get('codigo_idioma'),
                    translation.get('nome_traduzido')
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT da tradução do time ID {translation.get('id_time')}, idioma {translation.get('codigo_idioma')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de Traduções de Times: {e}")
        if conn:
            conn.rollback()
        failure_count = len(translations_data) - success_count
    finally:
        if cursor:
            cursor.close()

    logging.info(f"UPSERT de Traduções de Times concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

if __name__ == "__main__":
    logging.info("Iniciando script de persistência de Times (exemplo)...")
    
    # Dados de exemplo - substitua isso pela chamada ao seu processador de times
    # Ex: processed_data = process_teams_data(api_data) # api_data viria do seu coletor
    sample_processed_teams_data = {
        "main_data": [
            {"id_time": 101, "id_pais": 1, "nome_original": "FC Exemplo A", "logo_url": "http://example.com/logo_A.png", "fundacao": 1901, "estadio_nome": "Estádio Exemplo A", "estadio_capacidade": 50001},
            {"id_time": 102, "id_pais": 2, "nome_original": "SC Exemplo B", "logo_url": "http://example.com/logo_B.png", "fundacao": 1921, "estadio_nome": "Arena Exemplo B", "estadio_capacidade": 30001},
        ],
        "translations_data": [
            {"id_time": 101, "codigo_idioma": "pt", "nome_traduzido": "FC Exemplo A (PT)"},
            {"id_time": 101, "codigo_idioma": "en", "nome_traduzido": "FC Example A (EN)"},
            {"id_time": 102, "codigo_idioma": "pt", "nome_traduzido": "SC Exemplo B (PT)"},
            {"id_time": 102, "codigo_idioma": "en", "nome_traduzido": "SC Example B (EN)"},
        ]
    }

    teams_to_persist = sample_processed_teams_data.get("main_data", [])
    translations_to_persist = sample_processed_teams_data.get("translations_data", [])

    conn = None
    try:
        conn = get_db_connection()
        if conn:
            logging.info("Processando dados principais dos Times...")
            upsert_teams(conn, teams_to_persist)
            
            logging.info("Processando traduções dos Times...")
            upsert_team_translations(conn, translations_to_persist)
            
            logging.info("Script de persistência de Times (exemplo) concluído.")
        else:
            logging.error("Não foi possível conectar ao banco de dados.")
    except Exception as e:
        logging.error(f"Erro fatal no script de persistência de Times: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


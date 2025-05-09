import os
import logging
from datetime import datetime
# Importar o conector do banco de dados
from src.database.db_connector import get_db_connection

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def upsert_fixtures(conn, fixtures_data):
    """
    Insere ou atualiza dados na tabela Partidas.
    """
    if not fixtures_data:
        logging.info("Nenhum dado de partida principal para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        # Colunas conforme create_tables_fixtures.sql
        sql = """
        INSERT INTO Partidas (
            id_partida, id_liga, id_temporada, id_estadio, id_time_casa, 
            id_time_visitante, data_hora_inicio, id_status_partida, 
            placar_casa, placar_visitante, placar_agregado_casa, 
            placar_agregado_visitante, nome_arbitro, informacoes_adicionais, 
            atualizado_em
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_partida)
        DO UPDATE SET
            id_liga = EXCLUDED.id_liga,
            id_temporada = EXCLUDED.id_temporada,
            id_estadio = EXCLUDED.id_estadio,
            id_time_casa = EXCLUDED.id_time_casa,
            id_time_visitante = EXCLUDED.id_time_visitante,
            data_hora_inicio = EXCLUDED.data_hora_inicio,
            id_status_partida = EXCLUDED.id_status_partida,
            placar_casa = EXCLUDED.placar_casa,
            placar_visitante = EXCLUDED.placar_visitante,
            placar_agregado_casa = EXCLUDED.placar_agregado_casa,
            placar_agregado_visitante = EXCLUDED.placar_agregado_visitante,
            nome_arbitro = EXCLUDED.nome_arbitro,
            informacoes_adicionais = EXCLUDED.informacoes_adicionais,
            atualizado_em = EXCLUDED.atualizado_em;
        """
        for fixture in fixtures_data:
            try:
                # Convertendo informacoes_adicionais para JSON string se for um dict
                info_adicionais = fixture.get('informacoes_adicionais')
                if isinstance(info_adicionais, dict):
                    import json
                    info_adicionais = json.dumps(info_adicionais)
                
                cursor.execute(sql, (
                    fixture.get('id_partida'),
                    fixture.get('id_liga'),
                    fixture.get('id_temporada'),
                    fixture.get('id_estadio'),
                    fixture.get('id_time_casa'),
                    fixture.get('id_time_visitante'),
                    fixture.get('data_hora_inicio'),
                    fixture.get('id_status_partida'),
                    fixture.get('placar_casa'),
                    fixture.get('placar_visitante'),
                    fixture.get('placar_agregado_casa'),
                    fixture.get('placar_agregado_visitante'),
                    fixture.get('nome_arbitro'),
                    info_adicionais, # Já convertido para JSON string ou None
                    datetime.now()
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT da partida ID {fixture.get('id_partida')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de Partidas: {e}")
        if conn:
            conn.rollback()
        failure_count = len(fixtures_data) - success_count
    finally:
        if cursor:
            cursor.close()
    
    logging.info(f"UPSERT de Partidas concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

def upsert_fixture_translations(conn, translations_data):
    """
    Insere ou atualiza dados na tabela Partidas_Traducoes.
    """
    if not translations_data:
        logging.info("Nenhum dado de tradução de partida para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO Partidas_Traducoes (id_partida, codigo_idioma, nome_partida_traduzido)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_partida, codigo_idioma)
        DO UPDATE SET
            nome_partida_traduzido = EXCLUDED.nome_partida_traduzido;
        """
        for translation in translations_data:
            try:
                cursor.execute(sql, (
                    translation.get('id_partida'),
                    translation.get('codigo_idioma'),
                    translation.get('nome_partida_traduzido')
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT da tradução da partida ID {translation.get('id_partida')}, idioma {translation.get('codigo_idioma')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de Traduções de Partidas: {e}")
        if conn:
            conn.rollback()
        failure_count = len(translations_data) - success_count
    finally:
        if cursor:
            cursor.close()

    logging.info(f"UPSERT de Traduções de Partidas concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

if __name__ == "__main__":
    logging.info("Iniciando script de persistência de Partidas (exemplo)...")
    
    sample_processed_fixtures_data = {
        "main_data": [
            {
                'id_partida': 1001,
                'id_liga': 8, # Ex: Premier League
                'id_temporada': 21646, # Ex: Temporada 2023/2024 da Premier League
                'id_estadio': 201, # Ex: Anfield
                'id_time_casa': 101, # Ex: Liverpool
                'id_time_visitante': 102, # Ex: Chelsea
                'data_hora_inicio': '2024-08-17T14:00:00Z', # Formato ISO 8601
                'id_status_partida': 1, # Ex: Não Iniciada (NS)
                'placar_casa': None,
                'placar_visitante': None,
                'placar_agregado_casa': None,
                'placar_agregado_visitante': None,
                'nome_arbitro': 'Michael Oliver',
                'informacoes_adicionais': {'rodada': 1, 'clima': 'Ensolarado'}
            },
            {
                'id_partida': 1002,
                'id_liga': 82, # Ex: Bundesliga
                'id_temporada': 21833, # Ex: Temporada 2023/2024 da Bundesliga
                'id_estadio': 301, # Ex: Allianz Arena
                'id_time_casa': 201, # Ex: Bayern Munich
                'id_time_visitante': 202, # Ex: Borussia Dortmund
                'data_hora_inicio': '2024-08-18T18:30:00Z',
                'id_status_partida': 17, # Ex: Finalizada (FT)
                'placar_casa': 2,
                'placar_visitante': 1,
                'placar_agregado_casa': None,
                'placar_agregado_visitante': None,
                'nome_arbitro': 'Felix Brych',
                'informacoes_adicionais': None
            }
        ],
        "translations_data": [
            {'id_partida': 1001, 'codigo_idioma': 'pt', 'nome_partida_traduzido': 'Liverpool vs Chelsea (PT)'},
            {'id_partida': 1001, 'codigo_idioma': 'en', 'nome_partida_traduzido': 'Liverpool vs Chelsea (EN)'},
            {'id_partida': 1002, 'codigo_idioma': 'pt', 'nome_partida_traduzido': 'Bayern de Munique vs Borussia Dortmund (PT)'},
        ]
    }

    fixtures_to_persist = sample_processed_fixtures_data.get("main_data", [])
    translations_to_persist = sample_processed_fixtures_data.get("translations_data", [])

    conn = None
    try:
        conn = get_db_connection()
        if conn:
            logging.info("Processando dados principais das Partidas...")
            upsert_fixtures(conn, fixtures_to_persist)
            
            logging.info("Processando traduções das Partidas...")
            upsert_fixture_translations(conn, translations_to_persist)
            
            logging.info("Script de persistência de Partidas (exemplo) concluído.")
        else:
            logging.error("Não foi possível conectar ao banco de dados.")
    except Exception as e:
        logging.error(f"Erro fatal no script de persistência de Partidas: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


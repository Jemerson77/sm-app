import os
import logging
from datetime import datetime
import json # Para o campo dados_adicionais
from src.database.db_connector import get_db_connection

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def upsert_odds_partida(conn, odds_data_list):
    if not odds_data_list:
        logging.info("Nenhum dado de odd de partida para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        # Colunas conforme create_table_odds_partida.sql
        # id_partida, id_bookmaker, id_market, nome_opcao_aposta, valor_odd, 
        # valor_odd_convertido_decimal, ultima_atualizacao_api, eh_odd_ao_vivo, dados_adicionais, atualizado_em
        # id_odd_partida é SERIAL, não precisa ser inserido explicitamente a menos que queiramos controlar o ID.
        # Para UPSERT, precisamos de uma chave de conflito. Usaremos a UNIQUE constraint:
        # (id_partida, id_bookmaker, id_market, nome_opcao_aposta, eh_odd_ao_vivo)
        
        sql = """
        INSERT INTO "OddsPartida" (
            id_partida, id_bookmaker, id_market, nome_opcao_aposta, valor_odd, 
            valor_odd_convertido_decimal, ultima_atualizacao_api, eh_odd_ao_vivo, dados_adicionais, 
            atualizado_em
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_partida, id_bookmaker, id_market, nome_opcao_aposta, eh_odd_ao_vivo)
        DO UPDATE SET
            valor_odd = EXCLUDED.valor_odd,
            valor_odd_convertido_decimal = EXCLUDED.valor_odd_convertido_decimal,
            ultima_atualizacao_api = EXCLUDED.ultima_atualizacao_api,
            dados_adicionais = EXCLUDED.dados_adicionais,
            atualizado_em = EXCLUDED.atualizado_em;
        """
        for odd_data in odds_data_list:
            try:
                dados_adicionais_json = None
                if isinstance(odd_data.get('dados_adicionais'), dict):
                    dados_adicionais_json = json.dumps(odd_data.get('dados_adicionais'))
                elif isinstance(odd_data.get('dados_adicionais'), str):
                    dados_adicionais_json = odd_data.get('dados_adicionais')

                cursor.execute(sql, (
                    odd_data.get('id_partida'),
                    odd_data.get('id_bookmaker'),
                    odd_data.get('id_market'),
                    odd_data.get('nome_opcao_aposta'),
                    odd_data.get('valor_odd'),
                    odd_data.get('valor_odd_convertido_decimal'),
                    odd_data.get('ultima_atualizacao_api'), # Pode ser None
                    odd_data.get('eh_odd_ao_vivo', False),
                    dados_adicionais_json,
                    datetime.now()
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT da odd para partida ID {odd_data.get('id_partida')}, bookmaker ID {odd_data.get('id_bookmaker')}, market ID {odd_data.get('id_market')}, opção {odd_data.get('nome_opcao_aposta')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de OddsPartida: {e}")
        if conn:
            conn.rollback()
        # Assegura que a contagem de falhas seja precisa em caso de erro geral
        failure_count = len(odds_data_list) - success_count
    finally:
        if cursor:
            cursor.close()
    
    logging.info(f"UPSERT de OddsPartida concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

if __name__ == "__main__":
    logging.info("Iniciando script de persistência de OddsPartida (exemplo)...")
    
    # IDs de exemplo que devem existir nas tabelas referenciadas:
    # id_partida: 1001 (da tabela Partidas)
    # id_bookmaker: 1 (da tabela Bookmakers, ex: Bet365)
    # id_market: 1 (da tabela Markets, ex: Match Winner)

    sample_odds_data = [
        {
            'id_partida': 1001, 
            'id_bookmaker': 1, 
            'id_market': 1, 
            'nome_opcao_aposta': 'Casa Vence',
            'valor_odd': 2.50,
            'valor_odd_convertido_decimal': None,
            'ultima_atualizacao_api': datetime.now(),
            'eh_odd_ao_vivo': False,
            'dados_adicionais': None
        },
        {
            'id_partida': 1001, 
            'id_bookmaker': 1, 
            'id_market': 1, 
            'nome_opcao_aposta': 'Empate',
            'valor_odd': 3.20,
            'valor_odd_convertido_decimal': None,
            'ultima_atualizacao_api': datetime.now(),
            'eh_odd_ao_vivo': False,
            'dados_adicionais': None
        },
        {
            'id_partida': 1001, 
            'id_bookmaker': 1, 
            'id_market': 1, 
            'nome_opcao_aposta': 'Visitante Vence',
            'valor_odd': 2.80,
            'valor_odd_convertido_decimal': None,
            'ultima_atualizacao_api': datetime.now(),
            'eh_odd_ao_vivo': False,
            'dados_adicionais': None
        },
        {
            'id_partida': 1001, 
            'id_bookmaker': 2, # Pinnacle
            'id_market': 3, # Over/Under Goals
            'nome_opcao_aposta': 'Acima de 2.5',
            'valor_odd': 1.95,
            'valor_odd_convertido_decimal': None,
            'ultima_atualizacao_api': datetime.now(),
            'eh_odd_ao_vivo': False,
            'dados_adicionais': {'total_gols': 2.5, 'tipo_aposta': 'Acima'}
        }
    ]

    conn = None
    try:
        conn = get_db_connection()
        if conn:
            logging.info("Processando dados de OddsPartida...")
            upsert_odds_partida(conn, sample_odds_data)
            logging.info("Script de persistência de OddsPartida (exemplo) concluído.")
        else:
            logging.error("Não foi possível conectar ao banco de dados.")
    except Exception as e:
        logging.error(f"Erro fatal no script de persistência de OddsPartida: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


import os
import logging
from datetime import datetime
import json # Para o campo dados_adicionais
from src.database.db_connector import get_db_connection

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def upsert_eventos_partida(conn, eventos_data):
    if not eventos_data:
        logging.info("Nenhum dado de evento de partida para processar.")
        return 0, 0

    success_count = 0
    failure_count = 0
    cursor = None

    try:
        cursor = conn.cursor()
        # Colunas conforme create_tables_eventos_partida.sql
        # id_evento_partida, id_partida, id_time_evento, id_jogador_principal_api, nome_jogador_principal, 
        # id_jogador_assistente_api, nome_jogador_assistente, id_tipo_evento_api, nome_tipo_evento, 
        # minuto_evento, segundo_extra_evento, periodo_evento, comentario_evento, dados_adicionais, atualizado_em
        sql = """
        INSERT INTO "EventosPartida" (
            id_evento_partida, id_partida, id_time_evento, id_jogador_principal_api, nome_jogador_principal, 
            id_jogador_assistente_api, nome_jogador_assistente, id_tipo_evento_api, nome_tipo_evento, 
            minuto_evento, segundo_extra_evento, periodo_evento, comentario_evento, dados_adicionais, 
            atualizado_em
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_evento_partida)
        DO UPDATE SET
            id_partida = EXCLUDED.id_partida,
            id_time_evento = EXCLUDED.id_time_evento,
            id_jogador_principal_api = EXCLUDED.id_jogador_principal_api,
            nome_jogador_principal = EXCLUDED.nome_jogador_principal,
            id_jogador_assistente_api = EXCLUDED.id_jogador_assistente_api,
            nome_jogador_assistente = EXCLUDED.nome_jogador_assistente,
            id_tipo_evento_api = EXCLUDED.id_tipo_evento_api,
            nome_tipo_evento = EXCLUDED.nome_tipo_evento,
            minuto_evento = EXCLUDED.minuto_evento,
            segundo_extra_evento = EXCLUDED.segundo_extra_evento,
            periodo_evento = EXCLUDED.periodo_evento,
            comentario_evento = EXCLUDED.comentario_evento,
            dados_adicionais = EXCLUDED.dados_adicionais,
            atualizado_em = EXCLUDED.atualizado_em;
        """
        for evento in eventos_data:
            try:
                dados_adicionais_json = None
                if isinstance(evento.get('dados_adicionais'), dict):
                    dados_adicionais_json = json.dumps(evento.get('dados_adicionais'))
                elif isinstance(evento.get('dados_adicionais'), str): # Se já for uma string JSON
                    dados_adicionais_json = evento.get('dados_adicionais')

                cursor.execute(sql, (
                    evento.get('id_evento_partida'),
                    evento.get('id_partida'),
                    evento.get('id_time_evento'),
                    evento.get('id_jogador_principal_api'),
                    evento.get('nome_jogador_principal'),
                    evento.get('id_jogador_assistente_api'),
                    evento.get('nome_jogador_assistente'),
                    evento.get('id_tipo_evento_api'),
                    evento.get('nome_tipo_evento'),
                    evento.get('minuto_evento'),
                    evento.get('segundo_extra_evento'),
                    evento.get('periodo_evento'),
                    evento.get('comentario_evento'),
                    dados_adicionais_json,
                    datetime.now()
                ))
                success_count += 1
            except Exception as e:
                logging.error(f"Erro ao fazer UPSERT do evento ID {evento.get('id_evento_partida')}: {e}")
                failure_count += 1
        conn.commit()
    except Exception as e:
        logging.error(f"Erro geral durante o UPSERT de EventosPartida: {e}")
        if conn:
            conn.rollback()
        failure_count = len(eventos_data) - success_count # Assume all failed in this case
    finally:
        if cursor:
            cursor.close()
    
    logging.info(f"UPSERT de EventosPartida concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    return success_count, failure_count

if __name__ == "__main__":
    logging.info("Iniciando script de persistência de EventosPartida (exemplo)...")
    
    # IDs de exemplo que devem existir nas tabelas referenciadas:
    # id_partida: 1001 (da tabela Partidas)
    # id_time_evento: 101, 102 (da tabela times)
    # id_jogador_principal_api, id_jogador_assistente_api: IDs da API (não são FKs diretas neste modelo, mas devem ser consistentes)
    # id_tipo_evento_api: IDs da API (não é FK direta neste modelo, mas deve ser consistente)

    sample_eventos_data = [
        {
            'id_evento_partida': 5001, 
            'id_partida': 1001, 
            'id_time_evento': 101, 
            'id_jogador_principal_api': 12345, # Exemplo de ID de jogador da API
            'nome_jogador_principal': 'Jogador Exemplo Um',
            'id_jogador_assistente_api': 67890, # Exemplo de ID de jogador da API
            'nome_jogador_assistente': 'Jogador Exemplo Assistente',
            'id_tipo_evento_api': 1, # Exemplo de ID de tipo de evento da API
            'nome_tipo_evento': 'Goal',
            'minuto_evento': 30,
            'segundo_extra_evento': 0,
            'periodo_evento': '1H',
            'comentario_evento': 'Gol de cabeça após escanteio.',
            'dados_adicionais': {'tipo_gol': 'Cabeça', 'de_dentro_da_area': True}
        },
        {
            'id_evento_partida': 5002, 
            'id_partida': 1001, 
            'id_time_evento': 102, 
            'id_jogador_principal_api': 54321, # Exemplo de ID de jogador da API
            'nome_jogador_principal': 'Jogador Exemplo Dois',
            'id_jogador_assistente_api': None,
            'nome_jogador_assistente': None,
            'id_tipo_evento_api': 2, # Exemplo de ID de tipo de evento da API
            'nome_tipo_evento': 'Yellow Card',
            'minuto_evento': 45,
            'segundo_extra_evento': 1,
            'periodo_evento': '1H',
            'comentario_evento': 'Falta dura no meio-campo.',
            'dados_adicionais': {'motivo_cartao': 'Falta tática'}
        }
    ]

    conn = None
    try:
        conn = get_db_connection()
        if conn:
            logging.info("Processando dados de EventosPartida...")
            upsert_eventos_partida(conn, sample_eventos_data)
            logging.info("Script de persistência de EventosPartida (exemplo) concluído.")
        else:
            logging.error("Não foi possível conectar ao banco de dados.")
    except Exception as e:
        logging.error(f"Erro fatal no script de persistência de EventosPartida: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


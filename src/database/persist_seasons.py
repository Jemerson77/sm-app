import psycopg2
import os
from datetime import datetime

def get_db_connection():
    # Esta função deve retornar uma conexão com o banco de dados.
    # Adapte conforme sua configuração.
    conn = psycopg2.connect(
        dbname="your_db_name", 
        user="your_user", 
        password="your_password", 
        host="your_host", 
        port="your_port"
    )
    return conn

def upsert_seasons(conn, seasons_data):
    """
    Insere ou atualiza dados de temporadas na tabela 'Temporadas'.
    Se uma temporada com o mesmo 'id_temporada' já existir, seus dados serão atualizados.
    Caso contrário, uma nova temporada será inserida.
    """
    if not seasons_data:
        print("Nenhum dado de temporada para processar.")
        return

    cursor = conn.cursor()
    upsert_query = """
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
            cursor.execute(upsert_query, (
                season.get('id_temporada'),
                season.get('id_liga'),
                season.get('nome_temporada'),
                season.get('data_inicio'),
                season.get('data_fim'),
                season.get('eh_temporada_atual'),
                datetime.now()
            ))
            print(f"Dados da temporada {season.get('id_temporada')} inseridos/atualizados com sucesso.")
        except Exception as e:
            print(f"Erro ao inserir/atualizar temporada {season.get('id_temporada')}: {e}")
            conn.rollback() # Reverte a transação em caso de erro em uma única inserção/atualização
            # Considere se deseja parar o script inteiro ou continuar com outros dados
            # raise # Para o script se um erro for crítico
            continue # Continua com o próximo registro
    conn.commit()
    cursor.close()

# Exemplo de como chamar a função (simulação de dados de entrada)
if __name__ == "__main__":
    # Este é um exemplo de como você pode usar a função upsert_seasons.
    # Você precisará adaptar isso para sua fonte de dados real (API, arquivo CSV, etc.)
    example_seasons_data = [
        {'id_temporada': 1, 'id_liga': 101, 'nome_temporada': '2023/2024', 'data_inicio': '2023-08-11', 'data_fim': '2024-05-19', 'eh_temporada_atual': True},
        {'id_temporada': 2, 'id_liga': 101, 'nome_temporada': '2022/2023', 'data_inicio': '2022-08-05', 'data_fim': '2023-05-28', 'eh_temporada_atual': False},
        # Adicione mais dados de temporadas conforme necessário
    ]
    
    conn = None
    try:
        conn = get_db_connection() # Assume que get_db_connection() está definida e retorna uma conexão válida
        upsert_seasons(conn, example_seasons_data)
        print("Processamento de temporadas concluído.")
    except Exception as e:
        print(f"Erro no processamento das temporadas: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")
# src/database/persist_leagues.py
import psycopg2
from .db_connector import get_db_connection
# Supondo que os processadores estarão em um local importável
# Para execução local pelo usuário, ele precisará garantir que os caminhos de importação funcionem
# ou que os processadores sejam copiados para o mesmo diretório ou um PYTHONPATH adequado.
# from ..processors.leagues_processor import process_leagues_data_for_db # Exemplo de caminho relativo

# Esta função assume que `processed_leagues_data` é o output do `leagues_processor.py`
# {'leagues_main': [...], 'leagues_translations': [...]}

def upsert_leagues(processed_leagues_main_data):
    """
    Performs UPSERT operation for the Ligas table.
    Args:
        processed_leagues_main_data (list): A list of league dicts for the Ligas table.
                                            Each dict should have {"id_liga", "logo_url", "id_pais_sportmonks"}.
    """
    if not processed_leagues_main_data:
        print("Nenhum dado principal de liga para persistir.")
        return

    conn = get_db_connection()
    if not conn:
        print("Falha ao obter conexão com o banco de dados para Ligas.")
        return

    success_count = 0
    failure_count = 0

    try:
        with conn.cursor() as cur:
            for league_data in processed_leagues_main_data:
                try:
                    cur.execute(
                        """
                        INSERT INTO "Ligas" (id_liga, logo_url, id_pais_sportmonks)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (id_liga) DO UPDATE SET
                            logo_url = EXCLUDED.logo_url,
                            id_pais_sportmonks = EXCLUDED.id_pais_sportmonks,
                            ultima_atualizacao_registro = CURRENT_TIMESTAMP;
                        """,
                        (
                            league_data.get("id_liga"),
                            league_data.get("logo_url"),
                            league_data.get("id_pais_sportmonks"),
                        ),
                    )
                    success_count += 1
                except psycopg2.Error as e:
                    print(f"Erro ao fazer UPSERT da liga ID {league_data.get('id_liga')}: {e}")
                    failure_count += 1
            conn.commit()
            print(f"UPSERT de Ligas concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    except psycopg2.Error as e:
        print(f"Erro geral na transação de Ligas: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def upsert_league_translations(processed_leagues_translations_data):
    """
    Performs UPSERT operation for the Ligas_Traducoes table.
    Args:
        processed_leagues_translations_data (list): List of league translation dicts.
                                                    Each dict: {"id_liga", "codigo_idioma", "nome_liga_traduzido"}.
    """
    if not processed_leagues_translations_data:
        print("Nenhum dado de tradução de liga para persistir.")
        return

    conn = get_db_connection()
    if not conn:
        print("Falha ao obter conexão com o banco de dados para Traduções de Ligas.")
        return

    success_count = 0
    failure_count = 0

    try:
        with conn.cursor() as cur:
            for trans_data in processed_leagues_translations_data:
                try:
                    cur.execute(
                        """
                        INSERT INTO "Ligas_Traducoes" (id_liga, codigo_idioma, nome_liga_traduzido)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (id_liga, codigo_idioma) DO UPDATE SET
                            nome_liga_traduzido = EXCLUDED.nome_liga_traduzido,
                            ultima_atualizacao_registro = CURRENT_TIMESTAMP;
                        """,
                        (
                            trans_data.get("id_liga"),
                            trans_data.get("codigo_idioma"),
                            trans_data.get("nome_liga_traduzido"),
                        ),
                    )
                    success_count += 1
                except psycopg2.Error as e:
                    print(f"Erro ao fazer UPSERT da tradução da liga ID {trans_data.get('id_liga')}, idioma {trans_data.get('codigo_idioma')}: {e}")
                    failure_count += 1
            conn.commit()
            print(f"UPSERT de Traduções de Ligas concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    except psycopg2.Error as e:
        print(f"Erro geral na transação de Traduções de Ligas: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Exemplo de como o usuário executaria este script
    print("Iniciando script de persistência de Ligas (exemplo de execução)...")
    print("Este script deve ser executado em um ambiente com DATABASE_URL configurado e acesso ao banco.")
    print("Você precisará fornecer os dados processados das ligas.")

    # Dados de exemplo que viriam do leagues_processor.py
    sample_processed_data = {
        "leagues_main": [
            {"id_liga": 8, "logo_url": "https://cdn.sportmonks.com/images/soccer/leagues/8.png", "id_pais_sportmonks": 462},
            {"id_liga": 501, "logo_url": "https://cdn.sportmonks.com/images/soccer/leagues/501.png", "id_pais_sportmonks": 32}
        ],
        "leagues_translations": [
            {"id_liga": 8, "codigo_idioma": "pt", "nome_liga_traduzido": "Premier League (PT)"},
            {"id_liga": 8, "codigo_idioma": "en", "nome_liga_traduzido": "Premier League (EN)"},
            {"id_liga": 501, "codigo_idioma": "pt", "nome_liga_traduzido": "Bundesliga (PT)"},
            {"id_liga": 501, "codigo_idioma": "en", "nome_liga_traduzido": "Bundesliga (EN)"}
        ]
    }

    print("\nProcessando dados principais das Ligas...")
    upsert_leagues(sample_processed_data["leagues_main"])

    print("\nProcessando traduções das Ligas...")
    upsert_league_translations(sample_processed_data["leagues_translations"])

    print("\nScript de persistência de Ligas (exemplo) concluído.")


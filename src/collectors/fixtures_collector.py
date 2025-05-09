import os
import time
import logging
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api_client.sportmonks_client import SportMonksAPIClient, SportMonksAPIError
from utils.rate_limiter import RateLimiter
from utils.collection_priority_manager import CollectionPriorityManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class FixturesCollector:
    def __init__(self, api_client: SportMonksAPIClient, collection_type="dynamic_fixtures"):
        """
        Inicializa o Coletor de Partidas (Fixtures).

        :param api_client: Uma instância do SportMonksAPIClient.
        :param collection_type: O tipo de coleção para gerenciamento de prioridade.
        """
        self.api_client = api_client
        self.collection_type = collection_type

        # Configuração do RateLimiter
        default_max_requests = int(os.getenv("FIXTURES_COLLECTOR_MAX_REQUESTS", 30)) # Ex: 30 requisições
        default_time_window = int(os.getenv("FIXTURES_COLLECTOR_TIME_WINDOW_SECONDS", 60)) # Ex: por minuto
        self.rate_limiter = RateLimiter(max_requests=default_max_requests, time_window_seconds=default_time_window)
        
        # Inicialização do CollectionPriorityManager (pode ser usado para lógicas mais complexas no futuro)
        self.priority_manager = CollectionPriorityManager()
        logging.info(f"FixturesCollector initialized. Rate Limiter: {default_max_requests} req / {default_time_window}s. Collection Type: {self.collection_type}")

    def fetch_fixtures_by_date(self, date_str, page=1, per_page=50, include=None):
        """
        Busca partidas para uma data específica, respeitando o rate limit e prioridade.
        """
        endpoint = f"fixtures/date/{date_str}"
        params = {"page": page, "per_page": per_page}
        if include:
            params["include"] = include
        
        logging.info(f"Attempting to fetch fixtures for date: {date_str} - Page: {page}, Include: {include} (Collection Type: {self.collection_type})")
        
        current_priority = self.priority_manager.get_priority(self.collection_type)
        # A lógica de prioridade aqui seria: se a prioridade for baixa e o rate limit estiver quase no fim, não fazer a chamada.
        # Por enquanto, apenas verificamos o rate_limiter.allow_request()

        if not self.rate_limiter.allow_request():
            logging.warning(f"Rate limit exceeded for {self.collection_type} (date {date_str}, page {page}). Collection attempt blocked. Priority: {current_priority}")
            return None # Conforme solicitado pelo usuário: apenas registrar e seguir

        logging.info(f"Rate limit check passed for {self.collection_type} (date {date_str}, page {page}). Proceeding with API call.")
        try:
            data = self.api_client.get_data(endpoint, params=params)
            # self.rate_limiter.increment_request_count() # O RateLimiter já faz isso internamente ao chamar allow_request
            return data
        except SportMonksAPIError as e:
            logging.error(f"API Error fetching fixtures for date ({date_str}, page {page}): {e}")
            # Não re-incrementar o contador de falha aqui, pois a requisição foi feita.
            raise
        except Exception as e:
            logging.error(f"Unexpected error fetching fixtures for date ({date_str}, page {page}): {e}")
            raise

    def fetch_all_fixtures_by_date_range(self, start_date_str, end_date_str, per_page=50, include=None):
        """
        Busca todas as partidas dentro de um intervalo de datas, lidando com paginação.
        Este método agora respeitará o rate limit para cada chamada de página.
        """
        all_fixtures_for_date = []
        current_page = 1
        date_to_fetch = start_date_str 
        logging.info(f"Starting fetch of all fixtures for date: {date_to_fetch}. Include: {include} (Collection Type: {self.collection_type})")

        while True:
            # A verificação do rate limit é feita dentro de fetch_fixtures_by_date
            response_data = self.fetch_fixtures_by_date(date_to_fetch, page=current_page, per_page=per_page, include=include)
            
            if response_data is None: # Rate limit atingido ou outro erro que resultou em None
                logging.warning(f"Fetching fixtures for {date_to_fetch}, page {current_page} was blocked or failed. Stopping pagination for this date.")
                break

            try:
                if "data" in response_data:
                    all_fixtures_for_date.extend(response_data["data"])
                    logging.info(f"  Page {current_page} for {date_to_fetch} processed, {len(response_data['data'])} fixtures added. Total: {len(all_fixtures_for_date)}")

                    if not response_data.get("pagination", {}).get("has_more"):
                        logging.info(f"  No more pages to fetch for date {date_to_fetch}.")
                        break
                    current_page += 1
                else:
                    logging.info(f"  No data found in response for {date_to_fetch}, page {current_page}. Stopping.")
                    break
            except Exception as e: # Captura exceções que podem ocorrer ao processar response_data, se não for um dict esperado
                logging.error(f"Error processing response data for {date_to_fetch}, page {current_page}: {e}. Stopping pagination.")
                break
        
        logging.info(f"Fetch of all fixtures for {date_to_fetch} completed. Total: {len(all_fixtures_for_date)}")
        return all_fixtures_for_date

if __name__ == '__main__':
    import datetime
    logging.info("Starting FixturesCollector test...")
    
    # Configurar variáveis de ambiente para teste do RateLimiter
    os.environ["FIXTURES_COLLECTOR_MAX_REQUESTS"] = "5"
    os.environ["FIXTURES_COLLECTOR_TIME_WINDOW_SECONDS"] = "20" 
    # Para CollectionPriorityManager, os tipos de coleta são definidos internamente ou via construtor.

    try:
        # Simular SportMonksAPIClient
        class MockSportMonksAPIClient:
            def __init__(self):
                self.request_count = 0
            def get_data(self, endpoint, params=None):
                self.request_count +=1
                logging.info(f"MockSportMonksAPIClient: Calling endpoint {endpoint} with params {params}. Request count: {self.request_count}")
                # Simular paginação
                page = params.get("page", 1)
                if endpoint.startswith("fixtures/date/"):
                    if page > 2: # Simular que não há mais páginas após a página 2
                        return {"data": [], "pagination": {"has_more": False}}
                    return {"data": [{ "id": f"fixture_{page}_{i+1}", "name": f"Game {i+1} on page {page}"} for i in range(params.get("per_page",3))], "pagination": {"has_more": True}}
                return {"data": []}

        client = MockSportMonksAPIClient()
        fixtures_collector = FixturesCollector(api_client=client, collection_type="test_dynamic_fixtures")
        
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        include_params = "league;participants;state"

        logging.info(f"\nTesting fetch_fixtures_by_date for {today_str} with include='{include_params}'...")
        
        # Testar o rate limiter
        for i in range(7):
            logging.info(f"Attempt {i+1} to fetch fixtures by date...")
            first_page_fixtures = fixtures_collector.fetch_fixtures_by_date(today_str, page=1, per_page=2, include=include_params)
            if first_page_fixtures and first_page_fixtures.get("data"):
                logging.info(f"  Successfully fetched {len(first_page_fixtures['data'])} fixtures.")
            elif first_page_fixtures is None:
                logging.info("  Fetch blocked by rate limiter or failed as expected after limit.")
            else:
                logging.info("  No fixtures found or unexpected data format.")
            time.sleep(1) # Pequena pausa entre tentativas

        logging.info(f"\nWaiting for rate limit window to reset ({os.getenv('FIXTURES_COLLECTOR_TIME_WINDOW_SECONDS')}s)...")
        time.sleep(int(os.getenv("FIXTURES_COLLECTOR_TIME_WINDOW_SECONDS", 20)) + 2)

        logging.info("\nAttempting fetch after rate limit window reset...")
        first_page_fixtures_after_reset = fixtures_collector.fetch_fixtures_by_date(today_str, page=1, per_page=2, include=include_params)
        if first_page_fixtures_after_reset and first_page_fixtures_after_reset.get("data"):
            logging.info(f"  Successfully fetched {len(first_page_fixtures_after_reset['data'])} fixtures after reset.")
        else:
            logging.info("  Fetch failed or was blocked after reset.")

        logging.info(f"\nTesting fetch_all_fixtures_by_date_range (simulating single date pagination) for {today_str}...")
        # Resetar o contador do mock client para este teste
        client.request_count = 0 
        # Reinstanciar o coletor para resetar o rate limiter para este teste específico
        fixtures_collector_paginated_test = FixturesCollector(api_client=client, collection_type="test_dynamic_fixtures_pagination")
        all_fixtures = fixtures_collector_paginated_test.fetch_all_fixtures_by_date_range(today_str, today_str, per_page=2, include=include_params)
        logging.info(f"Total fixtures fetched with pagination: {len(all_fixtures)}")
        if all_fixtures:
            logging.info(f"First few fetched fixtures: {all_fixtures[:3]}")

    except ValueError as e:
        logging.error(f"Configuration error: {e}")
    except SportMonksAPIError as e:
        logging.error(f"API error during collector test: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during collector test: {e}", exc_info=True)


import time
import random
import logging

# Configuração básica de logging para o módulo
logger = logging.getLogger(__name__)

class RecoverableError(Exception):
    """Exceção base para erros que podem acionar retries."""
    pass

class NonRecoverableError(Exception):
    """Exceção base para erros que NÃO devem acionar retries."""
    pass

def execute_with_retry(operation, max_retries=3, initial_delay=1, backoff_factor=2, jitter_fraction=0.1, recoverable_exceptions=(RecoverableError,)):
    """
    Executa uma operação com uma lógica de retry e exponential backoff.

    Args:
        operation (callable): A função/operação a ser executada.
        max_retries (int): Número máximo de tentativas (além da inicial).
        initial_delay (float): Delay inicial em segundos antes da primeira retry.
        backoff_factor (float): Fator pelo qual o delay aumenta a cada retry.
        jitter_fraction (float): Fração do delay a ser usada como jitter (0 a 1).
        recoverable_exceptions (tuple): Tupla de tipos de exceção que devem acionar um retry.

    Returns:
        O resultado da operação, se bem-sucedida.

    Raises:
        A última exceção encontrada se todas as tentativas falharem, ou uma NonRecoverableError.
    """
    attempts = 0
    current_delay = initial_delay

    while attempts <= max_retries:
        try:
            logger.debug(f"Tentativa {attempts + 1} de {max_retries + 1} para a operação {operation.__name__ if hasattr(operation, '__name__') else 'anônima'}.")
            return operation()
        except recoverable_exceptions as e:
            attempts += 1
            if attempts > max_retries:
                logger.error(f"Máximo de {max_retries + 1} tentativas atingido para a operação. Erro final: {e}")
                raise
            
            # Calcula o tempo de espera com exponential backoff e jitter
            wait_time = current_delay
            if jitter_fraction > 0:
                jitter = random.uniform(0, wait_time * jitter_fraction)
                wait_time_with_jitter = wait_time + jitter
            else:
                wait_time_with_jitter = wait_time
            
            logger.warning(f"Operação falhou com erro recuperável: {e}. Tentativa {attempts} de {max_retries}. Esperando {wait_time_with_jitter:.2f}s antes da próxima tentativa.")
            time.sleep(wait_time_with_jitter)
            current_delay *= backoff_factor # Aumenta o delay para a próxima tentativa
        except NonRecoverableError as e:
            logger.error(f"Erro não recuperável durante a operação: {e}. Não haverá novas tentativas.")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado e não classificado como recuperável durante a operação: {e}. Não haverá novas tentativas.")
            raise # Re-levanta exceções não previstas para não mascarar outros problemas

# Exemplo de uso (será removido ou comentado em produção)
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    # Simula uma operação que pode falhar
    test_attempts_count = 0
    def flaky_operation():
        global test_attempts_count
        test_attempts_count += 1
        if test_attempts_count < 3:
            logger.info(f"flaky_operation: Falhando na tentativa {test_attempts_count}")
            raise RecoverableError(f"Falha simulada na tentativa {test_attempts_count}")
        elif test_attempts_count == 4:
             logger.info(f"flaky_operation: Erro não recuperável na tentativa {test_attempts_count}")
             raise NonRecoverableError("Erro não recuperável simulado")
        logger.info(f"flaky_operation: Sucesso na tentativa {test_attempts_count}")
        return "Operação bem-sucedida!"

    logger.info("Testando execute_with_retry com sucesso após falhas:")
    try:
        test_attempts_count = 0 # Reset
        result = execute_with_retry(flaky_operation, max_retries=3, initial_delay=0.1, backoff_factor=2)
        logger.info(f"Resultado final: {result}")
    except Exception as e:
        logger.error(f"Exceção final capturada: {e}")

    logger.info("\nTestando execute_with_retry com falha máxima de retries:")
    try:
        test_attempts_count = 0 # Reset
        def always_fail_recoverable():
            global test_attempts_count
            test_attempts_count += 1
            raise RecoverableError(f"Falha recuperável constante {test_attempts_count}")
        execute_with_retry(always_fail_recoverable, max_retries=2, initial_delay=0.1)
    except Exception as e:
        logger.error(f"Exceção final capturada (esperado): {e}")

    logger.info("\nTestando execute_with_retry com erro não recuperável:")
    try:
        test_attempts_count = 0 # Reset
        result = execute_with_retry(flaky_operation, max_retries=3, initial_delay=0.1)
        logger.info(f"Resultado final: {result}")
    except NonRecoverableError as e:
        logger.error(f"Exceção não recuperável capturada (esperado): {e}")
    except Exception as e:
        logger.error(f"Outra exceção capturada: {e}")


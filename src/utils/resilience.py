"""
Patterns de résilience pour ETL enterprise
"""
import time
import functools
from datetime import datetime, timedelta
from typing import Callable, Any, Optional
from dataclasses import dataclass, field

@dataclass
class CircuitBreakerState:
    """État du circuit breaker"""
    failures: int = 0
    last_failure_time: Optional[datetime] = None
    state: str = "closed"  # closed, open, half_open
    success_threshold: int = 2
    failure_threshold: int = 5
    timeout: int = 60  # secondes avant retry

class CircuitBreaker:
    """
    Pattern Circuit Breaker pour éviter cascades de pannes
    
    États:
    - CLOSED: Opération normale
    - OPEN: Trop d'échecs, rejette immédiatement
    - HALF_OPEN: Test si service récupéré
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: int = 60
    ):
        self.state = CircuitBreakerState(
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            timeout=timeout
        )
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute fonction avec circuit breaker"""
        
        # Si circuit ouvert, vérifier timeout
        if self.state.state == "open":
            if self._should_attempt_reset():
                self.state.state = "half_open"
                print(f"⚡ Circuit breaker: Tentative de récupération (half-open)")
            else:
                raise Exception(
                    f"Circuit breaker OPEN - Réessayer dans "
                    f"{self._time_until_retry():.0f}s"
                )
        
        # Exécuter fonction
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """Callback succès"""
        if self.state.state == "half_open":
            self.state.failures = 0
            self.state.state = "closed"
            print("✅ Circuit breaker: Fermé (service récupéré)")
        
        self.state.failures = max(0, self.state.failures - 1)
    
    def _on_failure(self):
        """Callback échec"""
        self.state.failures += 1
        self.state.last_failure_time = datetime.now()
        
        if self.state.failures >= self.state.failure_threshold:
            self.state.state = "open"
            print(
                f"🔴 Circuit breaker: Ouvert après {self.state.failures} échecs "
                f"(timeout: {self.state.timeout}s)"
            )
    
    def _should_attempt_reset(self) -> bool:
        """Vérifie si on peut tenter une récupération"""
        if not self.state.last_failure_time:
            return True
        
        elapsed = (datetime.now() - self.state.last_failure_time).total_seconds()
        return elapsed >= self.state.timeout
    
    def _time_until_retry(self) -> float:
        """Temps avant prochain retry"""
        if not self.state.last_failure_time:
            return 0
        
        elapsed = (datetime.now() - self.state.last_failure_time).total_seconds()
        return max(0, self.state.timeout - elapsed)
    
    def reset(self):
        """Reset manuel du circuit breaker"""
        self.state.failures = 0
        self.state.state = "closed"
        self.state.last_failure_time = None


def retry_with_backoff(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,)
):
    """
    Décorateur retry avec backoff exponentiel
    
    Args:
        max_attempts: Nombre max de tentatives
        initial_delay: Délai initial (secondes)
        backoff_factor: Multiplicateur pour chaque retry
        max_delay: Délai maximum (secondes)
        exceptions: Tuple d'exceptions à retry
    
    Exemple:
        @retry_with_backoff(max_attempts=5, initial_delay=2)
        def fetch_data():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts:
                        print(
                            f"❌ Échec définitif après {max_attempts} tentatives: "
                            f"{func.__name__}"
                        )
                        raise
                    
                    print(
                        f"⚠️  Tentative {attempt}/{max_attempts} échouée: "
                        f"{func.__name__} - Retry dans {delay:.1f}s"
                    )
                    print(f"   Erreur: {str(e)[:200]}")
                    
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
            
            raise last_exception
        
        return wrapper
    return decorator


def timeout_decorator(seconds: int):
    """
    Décorateur timeout (Windows-compatible avec threading)
    
    Args:
        seconds: Timeout en secondes
    
    Exemple:
        @timeout_decorator(300)  # 5 minutes max
        def long_running_task():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import threading
            
            result = [None]
            exception = [None]
            
            def target():
                try:
                    result[0] = func(*args, **kwargs)
                except Exception as e:
                    exception[0] = e
            
            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()
            thread.join(seconds)
            
            if thread.is_alive():
                raise TimeoutError(
                    f"Fonction {func.__name__} a dépassé {seconds}s"
                )
            
            if exception[0]:
                raise exception[0]
            
            return result[0]
        
        return wrapper
    return decorator


class TransactionManager:
    """
    Gestionnaire de transactions avec rollback automatique
    
    Exemple:
        with TransactionManager(conn) as tx:
            cursor.execute("INSERT ...")
            cursor.execute("UPDATE ...")
            # Commit automatique si pas d'exception
    """
    
    def __init__(self, connection, logger=None):
        self.conn = connection
        self.logger = logger
        self.savepoint_id = None
    
    def __enter__(self):
        try:
            # Créer savepoint si dans une transaction
            self.savepoint_id = f"sp_{int(time.time() * 1000)}"
            cursor = self.conn.cursor()
            cursor.execute(f"SAVE TRANSACTION {self.savepoint_id}")
            cursor.close()
            
            if self.logger:
                self.logger.log_step("transaction", "begin", "started")
            
        except Exception:
            # Si pas de transaction active, pas de savepoint
            self.savepoint_id = None
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # Succès - commit
            self.conn.commit()
            if self.logger:
                self.logger.log_step("transaction", "commit", "success")
        else:
            # Échec - rollback
            if self.savepoint_id:
                cursor = self.conn.cursor()
                cursor.execute(f"ROLLBACK TRANSACTION {self.savepoint_id}")
                cursor.close()
            else:
                self.conn.rollback()
            
            if self.logger:
                self.logger.log_step(
                    "transaction", 
                    "rollback", 
                    "failed",
                    error=str(exc_val)
                )
            
            print(f"🔄 Transaction rollback: {str(exc_val)[:200]}")
        
        return False  # Propage l'exception


# Exemples d'utilisation

def example_circuit_breaker():
    """Exemple circuit breaker"""
    breaker = CircuitBreaker(failure_threshold=3, timeout=30)
    
    def unstable_service():
        import random
        if random.random() < 0.7:  # 70% échecs
            raise ConnectionError("Service indisponible")
        return "OK"
    
    for i in range(10):
        try:
            result = breaker.call(unstable_service)
            print(f"✅ Appel {i+1}: {result}")
        except Exception as e:
            print(f"❌ Appel {i+1}: {e}")
        
        time.sleep(1)


@retry_with_backoff(max_attempts=5, initial_delay=1, backoff_factor=2)
def example_retry():
    """Exemple retry avec backoff"""
    import random
    
    if random.random() < 0.8:  # 80% échecs
        raise ConnectionError("Connexion refusée")
    
    return "Données extraites"


@timeout_decorator(5)
def example_timeout():
    """Exemple timeout"""
    time.sleep(10)  # Simuler tâche longue
    return "Terminé"
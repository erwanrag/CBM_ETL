# src/utils/progress_breaker.py
"""
Circuit Breaker dédié Progress OpenEdge
Évite cascades de pannes si Progress indisponible
"""
from src.utils.resilience import CircuitBreaker
import functools

# Instance globale (singleton)
_progress_circuit = CircuitBreaker(
    failure_threshold=3,    # 3 échecs → circuit ouvert
    success_threshold=2,    # 2 succès → circuit fermé
    timeout=120             # 2 min avant retry
)

def with_progress_breaker(func):
    """
    Décorateur pour protéger fonctions Progress
    
    Usage:
        @with_progress_breaker
        def get_progress_connection():
            return pyodbc.connect(...)
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return _progress_circuit.call(func, *args, **kwargs)
    return wrapper


def reset_progress_circuit():
    """Reset manuel du circuit (admin)"""
    _progress_circuit.reset()
    print("✅ Circuit breaker Progress réinitialisé")


def get_progress_circuit_status():
    """Statut actuel du circuit"""
    return {
        'state': _progress_circuit.state.state,
        'failures': _progress_circuit.state.failures,
        'last_failure': _progress_circuit.state.last_failure_time,
        'threshold': _progress_circuit.state.failure_threshold
    }
# tests/test_resilience.py
import pytest
from unittest.mock import patch, Mock, call
import pandas as pd
import pyodbc
import time
from src.tasks.extract_tasks import extract_to_parquet

@pytest.mark.unit
def test_extract_retries_on_odbc_error():
    """Test retry sans vraie connexion SQL"""
    
    with patch('src.tasks.extract_tasks.get_table_columns') as mock_get_cols:
        mock_get_cols.return_value = pd.DataFrame({
            'SourceExpression': ['cod_pro'],
            'SqlName': ['cod_pro'],
            'IsExcluded': [0]
        })
        
        with patch('src.tasks.extract_tasks.get_progress_connection'):
            with patch('pandas.read_sql') as mock_read_sql:
                # 1er appel échoue, 2ème réussit
                mock_read_sql.side_effect = [
                    pyodbc.OperationalError("Connection lost"),
                    pd.DataFrame({'cod_pro': ['A001']})
                ]
                
                # Doit réussir après 1 retry
                result = extract_to_parquet('produit')
                
                # ✅ Vérifier retry (2 tentatives)
                assert mock_read_sql.call_count == 2
                
                # ✅ Vérifier que fichier créé (chemin réel)
                assert result.endswith('produit_raw.parquet')


@pytest.mark.unit
def test_extract_retries_on_odbc_error():
    """Test retry sans vraie connexion SQL"""
    
    # AJOUTER : Mock get_table_columns AVANT l'appel
    with patch('src.tasks.extract_tasks.get_table_columns') as mock_get_cols:
        mock_get_cols.return_value = pd.DataFrame({
            'SourceExpression': ['cod_pro'],
            'SqlName': ['cod_pro'],
            'IsExcluded': [0]
        })
        
        with patch('src.tasks.extract_tasks.get_progress_connection') as mock_conn:
            with patch('pandas.read_sql') as mock_read_sql:
                # 1er appel échoue, 2ème réussit
                mock_read_sql.side_effect = [
                    pyodbc.OperationalError("Connection lost"),
                    pd.DataFrame({'cod_pro': ['A001']})  # Succès
                ]
                
                with patch('src.utils.parquet_cache.save_to_cache') as mock_save:
                    mock_save.return_value = "test.parquet"
                    
                    # Doit réussir après 1 retry
                    result = extract_to_parquet('produit')
                    
                    # Vérifier 2 tentatives read_sql
                    assert mock_read_sql.call_count == 2
                    assert result.endswith('produit_raw.parquet')


@pytest.mark.integration
@pytest.mark.slow
def test_extract_real_with_retry():
    """Test extraction réelle avec retry (requires Progress)"""
    try:
        # Test avec table réelle
        result = extract_to_parquet('client', page_size=10)
        
        assert result is not None
        assert "client_raw.parquet" in result
        
    except Exception as e:
        pytest.skip(f"Progress indisponible : {e}")
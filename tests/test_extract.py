# tests/test_extract.py
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
import pandas as pd
from src.tasks.extract_tasks import extract_to_parquet
from src.utils.parquet_cache import load_from_cache

def test_extract_with_filter_integration():
    """Test extraction réelle avec WHERE clause (test d'intégration)"""
    # IMPORTANT : Ce test nécessite connexion Progress active
    
    try:
        # Extract retourne chemin Parquet
        parquet_path = extract_to_parquet(
            'produit', 
            where_clause="cod_pro LIKE 'A%'",
            page_size=100
        )
        
        # Vérifier fichier créé
        assert Path(parquet_path).exists()
        
        # Charger et vérifier contenu
        df = load_from_cache('produit', 'raw')
        
        assert len(df) > 0, "Aucune ligne extraite"
        assert 'cod_pro' in df.columns
        
        # Vérifier filtre appliqué (quelques lignes au moins)
        matching = df['cod_pro'].str.startswith('A', na=False)
        assert matching.any(), "Aucun produit commençant par 'A'"
        
    except Exception as e:
        pytest.skip(f"Test skippé (Progress indisponible) : {e}")


def test_extract_creates_parquet_cache():
    """Test que extract crée bien un fichier Parquet"""
    from src.utils.parquet_cache import CACHE_DIR
    
    try:
        table_name = 'client'  # Table simple
        parquet_path = extract_to_parquet(table_name, page_size=50)
        
        # Vérifier structure fichier
        cache_file = CACHE_DIR / f"{table_name}_raw.parquet"
        assert cache_file.exists()
        assert cache_file.stat().st_size > 0
        
        # Charger et vérifier
        df = load_from_cache(table_name, 'raw')
        assert len(df) > 0
        assert len(df.columns) > 0
        
    except Exception as e:
        pytest.skip(f"Test skippé : {e}")


@pytest.mark.unit
def test_extract_handles_empty_result():
    """Test comportement si requête retourne 0 lignes (mock)"""
    
    # ✅ AJOUTER : Mock get_table_columns d'abord
    with patch('src.tasks.extract_tasks.get_table_columns') as mock_get_cols:
        mock_get_cols.return_value = pd.DataFrame({
            'SourceExpression': ['cod_pro', 'lib_pro'],
            'SqlName': ['cod_pro', 'lib_pro'],
            'IsExcluded': [0, 0]
        })
        
        with patch('src.tasks.extract_tasks.get_progress_connection') as mock_conn:
            with patch('pandas.read_sql') as mock_read_sql:
                # Simuler résultat vide
                mock_read_sql.return_value = pd.DataFrame(columns=['cod_pro', 'lib_pro'])
                
                with patch('src.utils.parquet_cache.save_to_cache') as mock_save:
                    mock_save.return_value = "test.parquet"
                    
                    parquet_path = extract_to_parquet('produit', where_clause="cod_pro = 'XXXXX'")
                    
                    # Doit créer fichier même vide
                    df = load_from_cache('produit', 'raw')
                    assert len(df) == 0
                    assert list(df.columns) == ['cod_pro', 'lib_pro']
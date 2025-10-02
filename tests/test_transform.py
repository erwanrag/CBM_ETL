# tests/test_transform.py
import pytest
from unittest.mock import Mock
import pandas as pd
from datetime import datetime
from src.tasks.transform_tasks import transform_from_parquet
from src.utils.parquet_cache import save_to_cache, load_from_cache

def test_hashdiff_added():
    """Test que transform ajoute bien hashdiff + colonnes techniques"""
    
    # 1. Créer données test
    df_test = pd.DataFrame({
        'cod_pro': ['A001', 'A002', 'A003'],
        'lib_pro': ['Produit 1', 'Produit 2', 'Produit 3'],
        'pri_ven': [10.5, 20.0, 15.75]
    })
    
    # 2. Sauvegarder dans cache (simulate extract)
    save_to_cache(df_test, 'test_table', 'raw')
    
    # 3. Mock config
    config = Mock()
    config.TableName = 'test_table'
    config.HasTimestamps = False
    config.DateModifCol = None
    
    # 4. Transformer
    parquet_path = transform_from_parquet(config)
    
    # 5. Vérifier résultat
    df_transformed = load_from_cache('test_table', 'transformed')
    
    # Colonnes techniques présentes
    assert 'hashdiff' in df_transformed.columns
    assert 'ts_source' in df_transformed.columns
    assert 'load_ts' in df_transformed.columns
    
    # Hashdiff non null
    assert df_transformed['hashdiff'].notna().all()
    assert df_transformed['hashdiff'].str.len().eq(40).all()  # SHA1 = 40 chars
    
    # load_ts renseigné
    assert df_transformed['load_ts'].notna().all()
    assert pd.api.types.is_datetime64_any_dtype(df_transformed['load_ts'])


def test_hashdiff_with_timestamps():
    """Test transform avec colonnes timestamp source"""
    
    df_test = pd.DataFrame({
        'cod_cli': ['C001', 'C002'],
        'nom_cli': ['Client A', 'Client B'],
        'dat_mod': ['2025-01-15 10:30:00', '2025-01-16 14:20:00']
    })
    
    save_to_cache(df_test, 'test_client', 'raw')
    
    config = Mock()
    config.TableName = 'test_client'
    config.HasTimestamps = True
    config.DateModifCol = 'dat_mod'
    
    transform_from_parquet(config)
    
    df_result = load_from_cache('test_client', 'transformed')
    
    # ts_source doit être renseigné depuis dat_mod
    assert df_result['ts_source'].notna().all()
    assert pd.api.types.is_datetime64_any_dtype(df_result['ts_source'])


def test_hashdiff_deterministic():
    """Test que hashdiff est identique pour mêmes données"""
    
    df = pd.DataFrame({
        'col1': ['A', 'B'],
        'col2': [1, 2]
    })
    
    save_to_cache(df, 'test_det', 'raw')
    
    config = Mock()
    config.TableName = 'test_det'
    config.HasTimestamps = False
    config.DateModifCol = None
    
    # Transformer 2 fois
    transform_from_parquet(config)
    df1 = load_from_cache('test_det', 'transformed')
    
    transform_from_parquet(config)
    df2 = load_from_cache('test_det', 'transformed')
    
    # Hashdiff identiques (sauf load_ts qui change)
    assert df1['hashdiff'].equals(df2['hashdiff'])


def test_hashdiff_detects_changes():
    """Test que hashdiff change quand données changent"""
    
    # Version 1
    df1 = pd.DataFrame({'col': ['A']})
    save_to_cache(df1, 'test_change', 'raw')
    
    config = Mock()
    config.TableName = 'test_change'
    config.HasTimestamps = False
    config.DateModifCol = None
    
    transform_from_parquet(config)
    result1 = load_from_cache('test_change', 'transformed')
    hash1 = result1['hashdiff'].iloc[0]
    
    # Version 2 (données différentes)
    df2 = pd.DataFrame({'col': ['B']})
    save_to_cache(df2, 'test_change', 'raw')
    
    transform_from_parquet(config)
    result2 = load_from_cache('test_change', 'transformed')
    hash2 = result2['hashdiff'].iloc[0]
    
    # Hashdiff DOIT être différent
    assert hash1 != hash2
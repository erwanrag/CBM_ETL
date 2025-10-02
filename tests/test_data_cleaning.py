# tests/test_data_cleaning.py
import pytest
import pandas as pd
from datetime import datetime
from src.utils.data_cleaning import (
    normalize_dataframe, 
    compute_hashdiff, 
    add_technical_columns
)

def test_normalize_trims_strings():
    """Test que normalize trim bien les strings"""
    df = pd.DataFrame({
        'text': ['  hello  ', 'world  ', '  test'],
        'number': [1, 2, 3]
    })
    
    result = normalize_dataframe(df)
    
    assert result['text'].tolist() == ['hello', 'world', 'test']
    assert result['number'].tolist() == [1, 2, 3]


def test_normalize_preserves_types():
    """Test préservation types numériques et dates"""
    df = pd.DataFrame({
        'string': ['  a  ', 'b'],
        'int': [10, 20],
        'float': [1.5, 2.7],
        'date': pd.to_datetime(['2025-01-01', '2025-01-02'])
    })
    
    result = normalize_dataframe(df)
    
    # Strings trimmed
    assert result['string'].iloc[0] == 'a'
    
    # Types préservés
    assert pd.api.types.is_integer_dtype(result['int'])
    assert pd.api.types.is_float_dtype(result['float'])
    assert pd.api.types.is_datetime64_any_dtype(result['date'])


def test_compute_hashdiff_basic():
    """Test calcul hashdiff basique"""
    df = pd.DataFrame({
        'a': [1, 2],
        'b': ['x', 'y']
    })
    
    result = compute_hashdiff(df)
    
    # Doit retourner Series de hashes
    assert isinstance(result, pd.Series)
    assert len(result) == 2
    assert result.str.len().eq(40).all()  # SHA1 = 40 chars hex


def test_add_technical_columns_complete():
    """Test ajout colonnes techniques complètes"""
    df = pd.DataFrame({
        'cod_pro': ['A001', 'A002'],
        'pri_ven': [10.5, 20.0]
    })
    
    from unittest.mock import Mock
    config = Mock()
    config.HasTimestamps = False
    config.DateModifCol = None
    
    result = add_technical_columns(df, config)
    
    # Vérifier colonnes ajoutées
    assert 'hashdiff' in result.columns
    assert 'ts_source' in result.columns
    assert 'load_ts' in result.columns
    
    # Colonnes originales préservées
    assert 'cod_pro' in result.columns
    assert 'pri_ven' in result.columns
    
    # Valeurs techniques
    assert result['hashdiff'].notna().all()
    assert result['load_ts'].notna().all()
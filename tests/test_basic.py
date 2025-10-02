import pytest
import pandas as pd
from src.utils.data_cleaning import normalize_dataframe, compute_hashdiff

def test_normalize_preserves_numbers():
    df = pd.DataFrame({
        'text': ['  hello  '],
        'number': [123]
    })
    result = normalize_dataframe(df)
    assert result['text'].iloc[0] == 'hello'
    assert result['number'].iloc[0] == 123

def test_hashdiff_deterministic():
    df = pd.DataFrame({'a': [1], 'b': [2]})
    h1 = compute_hashdiff(df).iloc[0]
    h2 = compute_hashdiff(df).iloc[0]
    assert h1 == h2

def test_hashdiff_detects_changes():
    df1 = pd.DataFrame({'a': [1]})
    df2 = pd.DataFrame({'a': [2]})
    assert compute_hashdiff(df1).iloc[0] != compute_hashdiff(df2).iloc[0]
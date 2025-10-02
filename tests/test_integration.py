import pytest
from src.tasks.config_tasks import get_table_config, get_included_columns

def test_config_load_produit():
    config = get_table_config('produit')
    assert config.TableName == 'produit'
    assert config.PrimaryKeyCols is not None

def test_columns_load_produit():
    cols = get_included_columns('produit')
    assert len(cols) > 0
    assert 'cod_pro' in cols
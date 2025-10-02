"""
Suite de tests data quality pour ETL
Requis : pytest, pytest-cov
"""
import pytest
import pandas as pd
from datetime import datetime
from sqlalchemy import text

from src.utils.data_cleaning import normalize_dataframe, compute_hashdiff
from src.utils.connections import get_sql_engine


# ========== TESTS DYNAMIQUES (Toutes les tables ODS) ==========

def get_ods_tables():
    """Charge dynamiquement toutes les tables ODS configurées"""
    try:
        engine = get_sql_engine()
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT DestinationTable, PrimaryKeyCols
                FROM config.ETL_Tables
                WHERE DestinationTable LIKE 'ods.%'
                  AND PrimaryKeyCols IS NOT NULL
                  AND PrimaryKeyCols != ''
            """, conn)
        
        return [(row['DestinationTable'], row['PrimaryKeyCols']) 
                for _, row in df.iterrows()]
    except Exception as e:
        # Si DB indisponible, retourner liste vide (tests seront skippés)
        return []


@pytest.mark.integration
@pytest.mark.parametrize("table_name,pk_cols", 
                         get_ods_tables(),
                         ids=lambda x: x[0] if isinstance(x, tuple) else x)
def test_pk_uniqueness(table_name, pk_cols):
    """Vérifie l'unicité des PK dans toutes les tables ODS"""
    engine = get_sql_engine()
    
    pk_list = [pk.strip() for pk in pk_cols.split(',')]
    pk_select = ', '.join([f'[{pk}]' for pk in pk_list])
    
    with engine.connect() as conn:
        duplicates = pd.read_sql(text(f"""
            SELECT {pk_select}, COUNT(*) as cnt
            FROM {table_name}
            GROUP BY {pk_select}
            HAVING COUNT(*) > 1
        """), conn)
    
    assert len(duplicates) == 0, \
        f"{len(duplicates)} doublons détectés dans {table_name}"


@pytest.mark.integration
def test_hashdiff_not_null():
    """Vérifie que hashdiff est toujours renseigné dans toutes les tables ODS"""
    engine = get_sql_engine()
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                t.TABLE_SCHEMA + '.' + t.TABLE_NAME as TableName,
                (SELECT COUNT(*) 
                 FROM sys.columns c
                 INNER JOIN sys.objects o ON o.object_id = c.object_id
                 INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
                 WHERE s.name = t.TABLE_SCHEMA 
                   AND o.name = t.TABLE_NAME
                   AND c.name = 'hashdiff'
                   AND c.is_nullable = 0) as HasHashdiff
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_SCHEMA IN ('ods', 'stg')
              AND t.TABLE_TYPE = 'BASE TABLE'
        """))
        
        for row in result:
            assert row[1] > 0, f"Colonne hashdiff manquante ou nullable dans {row[0]}"


@pytest.mark.integration
def test_timestamp_consistency():
    """Vérifie cohérence load_ts >= ts_source dans toutes les tables ODS"""
    engine = get_sql_engine()
    
    tables = pd.read_sql("""
        SELECT DestinationTable 
        FROM config.ETL_Tables 
        WHERE HasTimestamps = 1
    """, engine)
    
    for table in tables['DestinationTable']:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) as InvalidRows
                FROM {table}
                WHERE ts_source IS NOT NULL
                  AND load_ts < ts_source
            """))
            
            invalid = result.fetchone()[0]
            assert invalid == 0, \
                f"{invalid} lignes avec load_ts < ts_source dans {table}"


@pytest.mark.integration
def test_no_null_in_pk():
    """Vérifie absence de NULL dans colonnes PK de toutes les tables ODS"""
    engine = get_sql_engine()
    
    configs = pd.read_sql("""
        SELECT TableName, DestinationTable, PrimaryKeyCols
        FROM config.ETL_Tables
        WHERE DestinationTable LIKE 'ods.%'
    """, engine)
    
    for _, config in configs.iterrows():
        table = config['DestinationTable']
        pk_cols = [pk.strip() for pk in config['PrimaryKeyCols'].split(',')]
        
        for pk_col in pk_cols:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) as NullCount
                    FROM {table}
                    WHERE [{pk_col}] IS NULL
                """))
                
                null_count = result.fetchone()[0]
                assert null_count == 0, \
                    f"{null_count} valeurs NULL dans PK {pk_col} de {table}"


# ========== TESTS UNITAIRES (Transformations) ==========

class TestDataTransformations:
    """Tests des transformations de données"""
    
    def test_normalize_preserves_types(self):
        """Test que normalize_dataframe préserve les types numériques"""
        df = pd.DataFrame({
            'text': ['  hello  ', 'world  '],
            'number': [123, 456],
            'date': pd.to_datetime(['2025-01-01', '2025-01-02'])
        })
        
        df_norm = normalize_dataframe(df)
        
        assert df_norm['text'].iloc[0] == 'hello'  # Trimmed
        assert df_norm['number'].dtype == df['number'].dtype  # Type preserved
        assert pd.api.types.is_datetime64_any_dtype(df_norm['date'])  # Date preserved
    
    def test_hashdiff_deterministic(self):
        """Test que compute_hashdiff est déterministe"""
        df = pd.DataFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2]
        })
        
        hash1 = compute_hashdiff(df)
        hash2 = compute_hashdiff(df)
        
        assert hash1.equals(hash2), "Hashdiff doit être déterministe"
    
    def test_hashdiff_detects_changes(self):
        """Test que hashdiff détecte les changements"""
        df1 = pd.DataFrame({'col1': ['a'], 'col2': [1]})
        df2 = pd.DataFrame({'col1': ['a'], 'col2': [2]})
        
        hash1 = compute_hashdiff(df1).iloc[0]
        hash2 = compute_hashdiff(df2).iloc[0]
        
        assert hash1 != hash2, "Hashdiff doit changer quand données changent"


# ========== TESTS CONFIGURATION ==========

class TestConfiguration:
    """Tests de configuration"""
    
    @pytest.mark.integration
    def test_all_tables_have_pk(self):
        """Vérifie que toutes les tables ont une PK définie"""
        engine = get_sql_engine()
        
        missing_pk = pd.read_sql("""
            SELECT TableName
            FROM config.ETL_Tables
            WHERE PrimaryKeyCols IS NULL 
               OR PrimaryKeyCols = ''
        """, engine)
        
        assert len(missing_pk) == 0, \
            f"Tables sans PK: {', '.join(missing_pk['TableName'].tolist())}"
    
    @pytest.mark.integration
    def test_timestamp_config_consistency(self):
        """Vérifie cohérence config timestamps"""
        engine = get_sql_engine()
        
        invalid = pd.read_sql("""
            SELECT TableName
            FROM config.ETL_Tables
            WHERE HasTimestamps = 1
              AND (DateModifCol IS NULL OR DateModifCol = '')
        """, engine)
        
        assert len(invalid) == 0, \
            f"Tables avec HasTimestamps=1 mais DateModifCol vide: {invalid['TableName'].tolist()}"
    
    @pytest.mark.integration
    def test_destination_schema_exists(self):
        """Vérifie que les schémas de destination existent"""
        engine = get_sql_engine()
        
        with engine.connect() as conn:
            schemas = conn.execute(text("""
                SELECT DISTINCT 
                    SUBSTRING(DestinationTable, 1, CHARINDEX('.', DestinationTable) - 1) as SchemaName
                FROM config.ETL_Tables
            """))
            
            for schema in schemas:
                schema_name = schema[0]
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.SCHEMATA 
                    WHERE SCHEMA_NAME = '{schema_name}'
                """))
                
                exists = result.fetchone()[0]
                assert exists > 0, f"Schéma {schema_name} n'existe pas"


@pytest.fixture
def sample_config():
    """Fixture avec config de test"""
    return {
        'TableName': 'test_table',
        'DestinationTable': 'ods.test_table',
        'PrimaryKeyCols': 'id',
        'HasTimestamps': True,
        'DateModifCol': 'date_modif',
        'LastSuccessTs': datetime(2025, 1, 1)
    }
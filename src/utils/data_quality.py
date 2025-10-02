"""
Framework Data Quality pour ETL
Inspiré de Great Expectations
"""
import pandas as pd
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json


@dataclass
class QualityCheckResult:
    """Résultat d'un check qualité"""
    check_name: str
    passed: bool
    message: str
    severity: str  # 'critical', 'warning', 'info'
    failed_rows: int = 0
    total_rows: int = 0
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            'check_name': self.check_name,
            'passed': self.passed,
            'message': self.message,
            'severity': self.severity,
            'failed_rows': self.failed_rows,
            'total_rows': self.total_rows,
            'failure_rate': self.failed_rows / self.total_rows if self.total_rows > 0 else 0,
            'details': self.details,
            'timestamp': self.timestamp.isoformat()
        }


class DataQualityValidator:
    """Validateur de qualité des données"""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.results: List[QualityCheckResult] = []

    # ================= CHECKS =================

    def check_not_null(self, df: pd.DataFrame, columns: List[str], severity: str = 'critical') -> QualityCheckResult:
        total_rows = len(df)
        failed_rows = df[columns].isnull().any(axis=1).sum()
        passed = failed_rows == 0

        message = f"✅ Pas de NULL dans {columns}" if passed else f"❌ {failed_rows} lignes avec NULL dans {columns}"

        result = QualityCheckResult(
            check_name='not_null',
            passed=passed,
            message=message,
            severity=severity,
            failed_rows=failed_rows,
            total_rows=total_rows,
            details={'columns': columns}
        )
        self.results.append(result)
        return result

    def check_uniqueness(self, df: pd.DataFrame, columns: List[str], severity: str = 'critical') -> QualityCheckResult:
        total_rows = len(df)
        duplicates = df.duplicated(subset=columns, keep=False)
        failed_rows = duplicates.sum()
        passed = failed_rows == 0

        message = f"✅ Pas de doublons sur {columns}" if passed else f"❌ {failed_rows} doublons détectés sur {columns}"

        details = {'columns': columns}
        if not passed:
            dup_values = df[duplicates][columns].drop_duplicates().head(10).to_dict('records')
            details['sample_duplicates'] = dup_values

        result = QualityCheckResult(
            check_name='uniqueness',
            passed=passed,
            message=message,
            severity=severity,
            failed_rows=failed_rows,
            total_rows=total_rows,
            details=details
        )
        self.results.append(result)
        return result

    def check_value_range(self, df: pd.DataFrame, column: str,
                          min_value: Optional[float] = None, max_value: Optional[float] = None,
                          severity: str = 'warning') -> QualityCheckResult:
        total_rows = len(df)
        mask = pd.Series([True] * total_rows)
        if min_value is not None:
            mask &= df[column] >= min_value
        if max_value is not None:
            mask &= df[column] <= max_value

        failed_rows = (~mask).sum()
        passed = failed_rows == 0

        range_str = f"[{min_value}, {max_value}]"
        message = f"✅ {column} dans {range_str}" if passed else f"❌ {failed_rows} valeurs hors {range_str} dans {column}"

        result = QualityCheckResult(
            check_name='value_range',
            passed=passed,
            message=message,
            severity=severity,
            failed_rows=failed_rows,
            total_rows=total_rows,
            details={
                'column': column,
                'min_expected': min_value,
                'max_expected': max_value,
                'actual_min': df[column].min(),
                'actual_max': df[column].max()
            }
        )
        self.results.append(result)
        return result

    def check_pattern(self, df: pd.DataFrame, column: str, pattern: str,
                      severity: str = 'warning') -> QualityCheckResult:
        total_rows = len(df)
        matches = df[column].astype(str).str.match(pattern, na=False)
        failed_rows = (~matches).sum()
        passed = failed_rows == 0

        message = f"✅ {column} match regex" if passed else f"❌ {failed_rows} valeurs invalides pour {column}"

        details = {'column': column, 'pattern': pattern}
        if not passed:
            details['invalid_samples'] = df[~matches][column].head(5).tolist()

        result = QualityCheckResult(
            check_name='pattern',
            passed=passed,
            message=message,
            severity=severity,
            failed_rows=failed_rows,
            total_rows=total_rows,
            details=details
        )
        self.results.append(result)
        return result

    def check_completeness(self, df: pd.DataFrame, min_fill_rate: float = 0.95,
                           severity: str = 'warning') -> QualityCheckResult:
        total_cells = df.shape[0] * df.shape[1]
        null_cells = df.isnull().sum().sum()
        fill_rate = 1 - (null_cells / total_cells)
        passed = fill_rate >= min_fill_rate

        message = f"✅ Taux de remplissage: {fill_rate:.1%}" if passed else f"❌ Remplissage {fill_rate:.1%} (min: {min_fill_rate:.1%})"

        null_rates = (df.isnull().sum() / len(df)).sort_values(ascending=False).head(5).to_dict()

        result = QualityCheckResult(
            check_name='completeness',
            passed=passed,
            message=message,
            severity=severity,
            total_rows=len(df),
            details={'fill_rate': fill_rate, 'min_expected': min_fill_rate, 'worst_columns': null_rates}
        )
        self.results.append(result)
        return result

    # ================= RUNNER =================

    def run_all_checks(self, df: pd.DataFrame, config: dict) -> Dict[str, QualityCheckResult]:
        checks = {}
        if 'not_null' in config:
            for col in config['not_null']:
                checks[f'not_null_{col}'] = self.check_not_null(df, [col])
        if 'unique' in config:
            for cols in config['unique']:
                checks[f'unique_{"_".join(cols)}'] = self.check_uniqueness(df, cols)
        if 'ranges' in config:
            for col, bounds in config['ranges'].items():
                checks[f'range_{col}'] = self.check_value_range(df, col,
                                                                bounds.get('min'), bounds.get('max'))
        if 'patterns' in config:
            for col, pattern in config['patterns'].items():
                checks[f'pattern_{col}'] = self.check_pattern(df, col, pattern)
        if 'min_fill_rate' in config:
            checks['completeness'] = self.check_completeness(df, config['min_fill_rate'])
        return checks

    def print_report(self):
        summary = self.get_summary()
        print("="*80)
        print(f"📊 DATA QUALITY - {self.table_name}")
        print(f"✅ Succès: {summary['passed']} / {summary['total_checks']}")
        print(f"❌ Échecs: {summary['failed']} / {summary['total_checks']}")
        print(f"📈 Taux réussite: {summary['success_rate']:.1%}")
        print("="*80)
        for r in self.results:
            icon = "✅" if r.passed else "❌"
            sev = {"critical": "🔴", "warning": "⚠️", "info": "ℹ️"}[r.severity]
            print(f"{icon} {sev} {r.check_name} -> {r.message}")

    def get_summary(self):
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        critical_failures = [r for r in self.results if not r.passed and r.severity == 'critical']
        return {
            'table': self.table_name,
            'total_checks': total,
            'passed': passed,
            'failed': total - passed,
            'success_rate': passed / total if total else 0,
            'critical_failures': len(critical_failures),
            'has_critical_failure': bool(critical_failures),
            'timestamp': datetime.now().isoformat()
        }


class DataQualityMonitor:
    """Moniteur centralisé : log en base SQL"""

    def __init__(self, conn_string: str):
        self.conn_string = conn_string

    def log_quality_check(self, result: QualityCheckResult, table_name: str):
        import pyodbc
        conn = pyodbc.connect(self.conn_string)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                              WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'DataQuality_Log')
                BEGIN
                    CREATE TABLE etl.DataQuality_Log (
                        LogId INT IDENTITY(1,1) PRIMARY KEY,
                        TableName NVARCHAR(100),
                        CheckName NVARCHAR(100),
                        Passed BIT,
                        Severity NVARCHAR(20),
                        FailedRows INT,
                        TotalRows INT,
                        Message NVARCHAR(500),
                        Details NVARCHAR(MAX),
                        CheckTs DATETIME2 DEFAULT GETDATE()
                    )
                END
            """)
            cursor.execute("""
                INSERT INTO etl.DataQuality_Log 
                (TableName, CheckName, Passed, Severity, FailedRows, TotalRows, Message, Details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(table_name),
                str(result.check_name),
                bool(result.passed),  
                str(result.severity),
                int(result.failed_rows),
                int(result.total_rows),
                str(result.message),
                json.dumps(result.details, ensure_ascii=False)
            ))
            conn.commit()
        finally:
            cursor.close()
            conn.close()


# ========== Exemple ==========

def example_usage():
    df = pd.DataFrame({
        'id': [1, 2, 2, 3, None],
        'email': ['a@test.com', 'b@test.com', 'invalid', None, 'c@test.com'],
        'price': [10, 200, -5, 50, 99999]
    })
    config = {
        'not_null': ['id', 'email'],
        'unique': [['id']],
        'ranges': {'price': {'min': 0, 'max': 1000}},
        'patterns': {'email': r'^[\w\.-]+@[\w\.-]+\.\w+$'},
        'min_fill_rate': 0.9
    }
    validator = DataQualityValidator("test_table")
    validator.run_all_checks(df, config)
    validator.print_report()

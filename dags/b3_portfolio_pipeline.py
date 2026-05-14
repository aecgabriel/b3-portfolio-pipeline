import os
from pathlib import Path

from airflow.sdk import dag, task, DAG
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import TestBehavior
from pendulum import datetime

DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt"
DBT_PROFILES_PATH = "/usr/local/airflow/include/dbt"
DUCKDB_PATH = "/usr/local/airflow/dbt_data/b3_portfolio.duckdb"
FILES_PATH = "/usr/local/airflow/files"
REPORTS_PATH = "/usr/local/airflow/include/reports"

MONTH_ORDER = {
    "janeiro": "01", "fevereiro": "02", "marco": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12",
}

SHEET_TABLE_MAP = {
    "Posição - Ações": "raw_stock_positions",
    "Posição - ETF": "raw_etf_positions",
    "Posição - Renda Fixa": "raw_fixed_income",
    "Posição - Tesouro Direto": "raw_treasury_bonds",
    "Proventos Recebidos": "raw_dividends",
    "Negociações": "raw_trades",
    "Posição - Empréstimos": "raw_stock_lending",
}


def sanitize_column(col: str) -> str:
    replacements = {
        " ": "_", "/": "_", "(": "", ")": "",
        "ã": "a", "ç": "c", "é": "e", "í": "i", "ó": "o", "ú": "u",
        "Ã": "A", "Ç": "C", "É": "E", "Í": "I", "Ó": "O", "Ú": "U",
        "â": "a", "ê": "e", "ô": "o", "á": "a",
    }
    for old, new in replacements.items():
        col = col.replace(old, new)
    return col


DUCKDB_POOL = "duckdb_single_writer"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["b3", "portfolio"],
    doc_md="B3 Portfolio Analytics Pipeline - Ingests xlsx reports, transforms with dbt, outputs to DuckDB.",
)
def b3_portfolio_pipeline():

    @task
    def ingest_xlsx_to_duckdb():
        import duckdb
        import pandas as pd
        import glob
        import re

        con = duckdb.connect(DUCKDB_PATH)

        xlsx_files = sorted(glob.glob(os.path.join(FILES_PATH, "*.xlsx")))
        if not xlsx_files:
            raise FileNotFoundError(f"No xlsx files found in {FILES_PATH}")

        for table_name in SHEET_TABLE_MAP.values():
            con.execute(f"DROP TABLE IF EXISTS {table_name}")

        first_insert = {table: True for table in SHEET_TABLE_MAP.values()}

        for filepath in xlsx_files:
            filename = os.path.basename(filepath)
            match = re.search(r"(\d{4})-(\w+)\.xlsx$", filename)
            if match:
                year = match.group(1)
                month_name = match.group(2)
                month_num = MONTH_ORDER.get(month_name, "00")
                reference_period = f"{year}-{month_num}"
            else:
                reference_period = filename

            xls = pd.ExcelFile(filepath)
            available_sheets = xls.sheet_names

            for sheet_name, table_name in SHEET_TABLE_MAP.items():
                matched_sheet = None
                for s in available_sheets:
                    if sheet_name.lower() in s.lower() or s.lower() in sheet_name.lower():
                        matched_sheet = s
                        break

                if matched_sheet is None:
                    continue

                df = pd.read_excel(filepath, sheet_name=matched_sheet)
                if df.empty:
                    continue

                df["_source_file"] = filename
                df["_reference_period"] = reference_period

                unnamed_cols = [c for c in df.columns if str(c).startswith("Unnamed")]
                df = df.drop(columns=unnamed_cols)

                for col in df.columns:
                    if col in ("_source_file", "_reference_period"):
                        continue
                    if df[col].dtype == object:
                        cleaned = df[col].map(
                            lambda x: str(x).replace(",", ".") if pd.notna(x) else x
                        )
                        numeric = pd.to_numeric(cleaned, errors="coerce")
                        non_null_original = df[col].dropna()
                        non_null_numeric = numeric.dropna()
                        if len(non_null_original) > 0 and len(non_null_numeric) == len(non_null_original):
                            df[col] = numeric

                original_cols = [c for c in df.columns if c not in ("_source_file", "_reference_period")]
                threshold = max(len(original_cols) // 2, 1)
                df = df.dropna(subset=original_cols, thresh=threshold)

                df.columns = [sanitize_column(str(c)) for c in df.columns]

                if first_insert[table_name]:
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                    first_insert[table_name] = False
                else:
                    existing_cols = [row[0] for row in con.execute(f"DESCRIBE {table_name}").fetchall()]
                    for col in df.columns:
                        if col not in existing_cols:
                            con.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{col}\" VARCHAR")
                    select_cols = ", ".join(
                        f"\"{c}\"" if c in df.columns else f"NULL as \"{c}\""
                        for c in existing_cols
                    )
                    new_cols = [c for c in df.columns if c not in existing_cols]
                    if new_cols:
                        select_cols += ", " + ", ".join(f"\"{c}\"" for c in new_cols)
                    con.execute(f"INSERT INTO {table_name} SELECT {select_cols} FROM df")

        for table_name, is_first in first_insert.items():
            if is_first:
                con.execute(f"CREATE TABLE {table_name} (placeholder VARCHAR, _source_file VARCHAR, _reference_period VARCHAR)")

        row_counts = {}
        for table_name in SHEET_TABLE_MAP.values():
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            row_counts[table_name] = count

        con.close()
        print(f"Ingestion complete. Row counts: {row_counts}")
        return row_counts

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt deps --profiles-dir {DBT_PROFILES_PATH}",
    )

    profile_config = ProfileConfig(
        profile_name="b3_portfolio",
        target_name="dev",
        profiles_yml_filepath=Path(DBT_PROFILES_PATH) / "profiles.yml",
    )

    # Staging models run sequentially (pool=1 slot) to avoid DuckDB lock conflicts
    # since they all read from the same raw source tables concurrently
    dbt_staging = DbtTaskGroup(
        group_id="dbt_staging",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(
            test_behavior=TestBehavior.NONE,
            select=["path:models/staging"],
        ),
        operator_args={"pool": DUCKDB_POOL},
    )

    # Intermediate + Marts run in parallel where possible
    # (dbt dependency graph naturally serializes what needs it)
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(
            test_behavior=TestBehavior.NONE,
            select=["path:models/intermediate", "path:models/marts"],
        ),
        operator_args={"pool": DUCKDB_POOL},
    )

    @task
    def generate_excel_report():
        import sys
        sys.path.insert(0, "/usr/local/airflow/include")
        from report_generator import generate_report

        output_file = os.path.join(REPORTS_PATH, "carteira_report.xlsx")
        result = generate_report(DUCKDB_PATH, output_file)
        print(f"Report generated: {result}")
        return result

    ingest_xlsx_to_duckdb() >> dbt_deps >> dbt_staging >> dbt_transform >> generate_excel_report()


b3_portfolio_pipeline()

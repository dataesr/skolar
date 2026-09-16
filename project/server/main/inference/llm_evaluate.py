import os
import json
import argparse
import duckdb
import pandas as pd
from project.server.main.paragraphs.acknowledgement.extract import extract_results
from project.server.main.utils import string_to_id, read_jsonl
from project.server.main.logger import get_logger

# from project.server.main.paragraphs.software import extract_software
# from project.server.main.paragraphs.dataset import extract_dataset

logger = get_logger(__name__)

BASE_DIR = "/data"
OUTPUT_DIR = "/data/evaluation"

EXTRACTORS = {
    "acknowledgement": extract_results,
}


def parse_files(base_dir: str, model_name: str, use_case: str) -> tuple:
    """Extract metrics from JSONL files for a specific model and use case"""
    publications = []  # publications results
    paragraphs = []  # paragraphs results

    model_dir = os.path.join(base_dir, f"llm_{model_name}")
    if not os.path.isdir(model_dir):
        logger.error(f"Model folder not found: {model_dir}")
        return publications, paragraphs

    paragraph_dir = os.path.join(model_dir, use_case)
    if not os.path.isdir(paragraph_dir):
        logger.error(f"Paragraph folder not found: {paragraph_dir}")
        return publications, paragraphs

    extractor = EXTRACTORS[use_case]

    # Walk all subdirectories
    logger.info(f"Start walking directory {paragraph_dir}...")
    for root, dirs, files in os.walk(paragraph_dir):
        for file_name in files:
            logger.debug(f"-> {file_name=} ({root=})")
            if not file_name.endswith(".jsonl"):
                continue

            if file_name.endswith("_failed.jsonl"):
                continue

            file_path = os.path.join(root, file_name)

            # Extract publication_id from filename
            # Filename format: [file_id].[paragraph_type].jsonl
            file_stem = file_name[: -len(".jsonl")]  # Remove .jsonl
            file_id = file_stem.rsplit(".", 1)[0] if "." in file_stem else file_stem
            publication_id = string_to_id(file_id)

            # Default publication data
            publi_data = {
                "model": model_name,
                "use_case": use_case,
                "publication_id": publication_id,
                "paragraphs_processed": 0,
                "paragraphs_ok": 0,
                "paragraphs_with_results": 0,
                "num_results": 0,
                "has_result": False,
                "status": "success",
                "error": "",
            }

            if os.path.getsize(file_path) < 2:  # 2bytes
                publi_data["status"] = "error"
                publi_data["error"] = "empty_file"
                publications.append(publi_data)
                continue

            current_paragraphs = []

            try:
                rows = read_jsonl(file_path)

            except Exception as error:
                logger.warning(f"Error while parsing {file_path}: {error}")
                publi_data["status"] = "error"
                publi_data["error"] = str(error)
                publications.append(publi_data)
                continue

            for index, row in enumerate(rows):
                para_data = {
                    "model": model_name,
                    "use_case": use_case,
                    "publication_id": publication_id,
                    "paragraph_id": index,  # TODO read from file ?
                    "status": "success",
                    "has_result": False,
                    "num_results": 0,
                    "error": "",
                }

                try:
                    num_results = extractor(row, model_name)
                    para_data["num_results"] = num_results
                    para_data["has_result"] = num_results > 0

                except Exception as error:
                    logger.warning(f"Error while parsing {file_path} - paragraph {index}: {error}")
                    para_data["status"] = "error"
                    para_data["error"] = str(error)

                current_paragraphs.append(para_data)

            publi_data["paragraphs_processed"] = len(current_paragraphs)
            publi_data["paragraphs_ok"] = len([p for p in current_paragraphs if p["status"] == "success"])
            publi_data["paragraphs_with_results"] = len([p for p in current_paragraphs if p["has_result"]])
            publi_data["num_results"] = sum(int(p["num_results"]) for p in current_paragraphs)

            paragraphs.extend(current_paragraphs)
            publications.append(publi_data)

    return publications, paragraphs


def build_database(publications: list, paragraphs: list, output_dir: str) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB database from parsed results."""
    os.makedirs(output_dir, exist_ok=True)

    db_path = os.path.join(output_dir, "evaluation.duckdb")
    con = duckdb.connect(db_path)

    publications_df = pd.DataFrame(publications)
    paragraphs_df = pd.DataFrame(paragraphs)

    con.register("publications_df", publications_df)
    con.register("paragraphs_df", paragraphs_df)

    con.execute("""
        CREATE TABLE publications AS
        SELECT *
        FROM publications_df
    """)

    con.execute("""
        CREATE TABLE paragraphs AS
        SELECT *
        FROM paragraphs_df
    """)

    return con


def get_model_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return high-level statistics for every model/use case."""

    return con.execute("""
        SELECT
            model,
            use_case,

            COUNT(*) AS publications,

            COUNT(*) FILTER (
                WHERE status != 'error'
            ) AS publications_ok,

            COUNT(*) FILTER (
                WHERE status = 'error'
            ) AS publications_errors,

            COUNT(*) FILTER (
                WHERE has_result
            ) AS publications_with_results,

            ROUND(
                100.0 * COUNT(*) FILTER (WHERE has_result)
                / NULLIF(
                    COUNT(*) FILTER (WHERE status != 'error'),
                    0
                ),
                2
            ) AS publication_result_rate,

            SUM(paragraphs_processed)
                AS paragraphs_processed,

            SUM(paragraphs_ok)
                AS paragraphs_ok,

            SUM(paragraphs_with_results)
                AS paragraphs_with_results,

            ROUND(
                100.0 * SUM(paragraphs_with_results)
                / NULLIF(SUM(paragraphs_ok), 0),
                2
            ) AS paragraph_result_rate,

            SUM(num_results)
                AS total_results,

            ROUND(
                AVG(num_results),
                2
            ) AS avg_results_per_publication

        FROM publications

        GROUP BY
            model,
            use_case

        ORDER BY
            use_case,
            model
    """).df()


def get_paragraph_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return paragraph-level statistics for every model/use case."""

    return con.execute("""
        SELECT
            model,
            use_case,

            COUNT(*) AS paragraphs_processed,

            COUNT(*) FILTER (
                WHERE status = 'success'
            ) AS paragraphs_ok,

            COUNT(*) FILTER (
                WHERE status = 'error'
            ) AS paragraphs_errors,

            COUNT(*) FILTER (
                WHERE has_result
            ) AS paragraphs_with_results,

            ROUND(
                100.0 * COUNT(*) FILTER (WHERE has_result)
                / NULLIF(
                    COUNT(*) FILTER (WHERE status = 'success'),
                    0
                ),
                2
            ) AS result_rate,

            SUM(num_results) AS total_results,

            ROUND(
                AVG(num_results) FILTER (
                    WHERE status = 'success'
                ),
                2
            ) AS avg_results_per_paragraph,

            ROUND(
                AVG(num_results) FILTER (
                    WHERE has_result
                ),
                2
            ) AS avg_results_per_positive_paragraph

        FROM paragraphs

        GROUP BY
            model,
            use_case

        ORDER BY
            use_case,
            model
    """).df()


def get_publication_agreement(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Compare models at publication level.
    Only publications successfully processed by both models are compared.
    """

    models = [row[0] for row in con.execute("""
            SELECT DISTINCT model
            FROM publications
            ORDER BY model
        """).fetchall()]

    results = []

    for i, model_a in enumerate(models):
        for model_b in models[i + 1 :]:
            df = con.execute(
                """
                SELECT
                    a.use_case,

                    ? AS model_a,
                    ? AS model_b,

                    COUNT(*) AS common_publications,

                    COUNT(*) FILTER (
                        WHERE a.has_result
                          AND b.has_result
                    ) AS both_positive,

                    COUNT(*) FILTER (
                        WHERE a.has_result
                          AND NOT b.has_result
                    ) AS only_a_positive,

                    COUNT(*) FILTER (
                        WHERE NOT a.has_result
                          AND b.has_result
                    ) AS only_b_positive,

                    COUNT(*) FILTER (
                        WHERE NOT a.has_result
                          AND NOT b.has_result
                    ) AS both_negative

                FROM publications a

                INNER JOIN publications b
                    ON a.publication_id = b.publication_id
                    AND a.use_case = b.use_case

                WHERE
                    a.model = ?
                    AND b.model = ?
                    AND a.status != 'error'
                    AND b.status != 'error'

                GROUP BY
                    a.use_case
                """,
                [model_a, model_b, model_a, model_b],
            ).df()

            if df.empty:
                continue

            df["agreement_rate"] = (df["both_positive"] + df["both_negative"]) / df["common_publications"]

            df["positive_jaccard"] = df["both_positive"] / (
                df["both_positive"] + df["only_a_positive"] + df["only_b_positive"]
            )

            results.append(df)

    if not results:
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)


def get_paragraph_agreement(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Compare models at paragraph level.
    Paragraphs are matched using:
        use_case
        publication_id
        paragraph_id
    """

    models = [row[0] for row in con.execute("""
            SELECT DISTINCT model
            FROM paragraphs
            ORDER BY model
        """).fetchall()]

    results = []

    for i, model_a in enumerate(models):
        for model_b in models[i + 1 :]:
            df = con.execute(
                """
                SELECT
                    a.use_case,

                    ? AS model_a,
                    ? AS model_b,

                    COUNT(*) AS common_paragraphs,

                    COUNT(*) FILTER (
                        WHERE a.has_result
                          AND b.has_result
                    ) AS both_positive,

                    COUNT(*) FILTER (
                        WHERE a.has_result
                          AND NOT b.has_result
                    ) AS only_a_positive,

                    COUNT(*) FILTER (
                        WHERE NOT a.has_result
                          AND b.has_result
                    ) AS only_b_positive,

                    COUNT(*) FILTER (
                        WHERE NOT a.has_result
                          AND NOT b.has_result
                    ) AS both_negative

                FROM paragraphs a

                INNER JOIN paragraphs b
                    ON a.publication_id = b.publication_id
                    AND a.paragraph_id = b.paragraph_id
                    AND a.use_case = b.use_case

                WHERE
                    a.model = ?
                    AND b.model = ?
                    AND a.status = 'success'
                    AND b.status = 'success'

                GROUP BY
                    a.use_case
                """,
                [model_a, model_b, model_a, model_b],
            ).df()

            if df.empty:
                continue

            df["agreement_rate"] = (df["both_positive"] + df["both_negative"]) / df["common_paragraphs"]

            df["positive_jaccard"] = df["both_positive"] / (
                df["both_positive"] + df["only_a_positive"] + df["only_b_positive"]
            )

            results.append(df)

    if not results:
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)


def get_publication_matrix(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Return one row per publication/model.
    Useful later for visualisation and disagreement exploration.
    """

    return con.execute("""
        SELECT
            publication_id,
            use_case,
            model,
            status,
            has_result,
            paragraphs_processed,
            paragraphs_with_results,
            num_results

        FROM publications

        ORDER BY
            use_case,
            publication_id,
            model
    """).df()


def save_results(results: dict, output_dir: str) -> None:
    """Save evaluation tables as Parquet files."""

    os.makedirs(output_dir, exist_ok=True)

    for name, df in results.items():
        path = os.path.join(output_dir, f"{name}.parquet")
        df.to_parquet(path, index=False)
        logger.info(f"Saved {name}: {len(df):,} rows -> {path}")


def llm_evaluate(args: dict):
    use_case = args.get("use_case", "")
    models = args.get("models", [])
    base_dir = args.get("base_dir", BASE_DIR)
    output_dir = args.get("output_dir", OUTPUT_DIR)

    if not (use_case or models):
        raise ValueError(f"Missing arguments! {use_case=}, {models=}")

    logger.info(f"Starting LLM evaluation: models={models}, use_case={use_case}")
    publications = []
    paragraphs = []

    # Process each model
    for model in models:
        model_publications, model_paragraphs = parse_files(base_dir=base_dir, model_name=model, use_case=use_case)

        logger.info(f"{model}: " f"{len(model_publications):,} publications, " f"{len(model_paragraphs):,} paragraphs")

        publications.extend(model_publications)
        paragraphs.extend(model_paragraphs)

    if not publications:
        logger.warning("No publication results found")
        return {}

    con = build_database(publications=publications, paragraphs=paragraphs, output_dir=output_dir)
    logger.info(f"DuckDB loaded: " f"{len(publications):,} publications, " f"{len(paragraphs):,} paragraphs")

    results = {
        "publications": con.execute("SELECT * FROM publications").df(),
        "paragraphs": con.execute("SELECT * FROM paragraphs").df(),
        "model_summary": get_model_summary(con),
        "paragraph_summary": get_paragraph_summary(con),
        "publication_agreement": get_publication_agreement(con),
        "paragraph_agreement": get_paragraph_agreement(con),
        "publication_matrix": get_publication_matrix(con),
    }

    save_results(
        results=results,
        output_dir=output_dir,
    )

    con.close()

    print("\n=== MODEL SUMMARY ===")
    print(results["model_summary"].to_string(index=False))

    print("\n=== PARAGRAPH SUMMARY ===")
    print(results["paragraph_summary"].to_string(index=False))

    print("\n=== PUBLICATION AGREEMENT ===")
    print(results["publication_agreement"].to_string(index=False))

    print("\n=== PARAGRAPH AGREEMENT ===")
    print(results["paragraph_agreement"].to_string(index=False))

    print("\nDone.")
    logger.info("LLM evaluation completed")

    return results

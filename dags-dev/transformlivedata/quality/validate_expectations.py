import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
import json
import warnings
import logging
from datetime import datetime, timezone


""
# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)

lat_long_limits = {
    "lat_min_value": -23.916334,
    "lat_max_value": -23.189743,
    "long_min_value": -46.792037,  # wrong
    # "long_min_value": -46.984037,  # corect
    # "long_max_value": -46.184930,  # correct
    "long_max_value": -46.184930,
}


def validate_expectations(df_to_be_validated, transformed_expectations_config):
    def clear_internal_gx_warnings():
        warnings.filterwarnings(
            "ignore", category=UserWarning, module="great_expectations"
        )
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module=".*pyparsing.*"
        )
        logging.getLogger(
            "great_expectations.data_context.data_context.context_factory"
        ).setLevel(logging.WARNING)
        logging.getLogger("great_expectations.data_context.types.base").setLevel(
            logging.WARNING
        )
        logging.getLogger("great_expectations.datasource.fluent.config").setLevel(
            logging.WARNING
        )

    def extract_unmatched_expectations_details(checkpoint_result):
        bad_indices = set()
        reasons_by_index = {}
        total_checks = 0
        checks_failed = 0
        for run_result in checkpoint_result.run_results.values():
            validation_results = run_result["validation_result"]["results"]
            total_checks += len(validation_results)
            for expectation_result in validation_results:
                if not expectation_result.get("success", False):
                    checks_failed += 1
                result = expectation_result.get("result", {})
                unexpected_indices = result.get("unexpected_index_list") or []
                if len(unexpected_indices) > 0:
                    expectation_config = expectation_result.get(
                        "expectation_config", {}
                    )
                    expectation_type = expectation_config.get("expectation_type")
                    column = expectation_config.get("kwargs", {}).get("column")
                    violation_count = len(unexpected_indices)
                    logger.info(
                        f"{expectation_type}"
                        + (f" (column: {column})" if column else "")
                        + f" -> violations: {violation_count}"
                    )
                    reason = f"expectation_type:{expectation_type}" + (
                        f"|column:{column}" if column else ""
                    )
                    for idx in unexpected_indices:
                        bad_indices.add(idx)
                        reasons_by_index[idx] = reasons_by_index.get(idx, []) + [reason]
        return bad_indices, reasons_by_index, total_checks, checks_failed

    # clear_internal_gx_warnings()
    gx_context = gx.get_context(mode="ephemeral")
    with open(transformed_expectations_config, "r") as f:
        suite_dict = json.load(f)
    suite = ExpectationSuite(**suite_dict)
    gx_context.add_or_update_expectation_suite(expectation_suite=suite)
    datasource_name = "pandas_datasource"
    datasource = gx_context.sources.add_pandas(datasource_name)
    data_asset = datasource.add_dataframe_asset(name="positions_asset")
    batch_request = data_asset.build_batch_request(dataframe=df_to_be_validated)
    checkpoint = gx_context.add_or_update_checkpoint(
        name="prod_checkpoint_with_alerts",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite.expectation_suite_name,
            }
        ],
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
    )
    checkpoint_result = checkpoint.run(
        result_format="COMPLETE", evaluation_parameters=lat_long_limits
    )
    if checkpoint_result.success:
        logger.info("Validation successful!")
        valid_df = df_to_be_validated
        invalid_df = None
        expectations_summary = {
            "total_checks": 0,
            "checks_failed": 0,
            "rows_failed": 0,
            "top_failure_reasons": [],
        }
    else:
        logger.warning("Validation failures detected!")
        logger.info("Checking for unmatched expectations...")
        (
            bad_indices,
            reasons_by_index,
            total_checks,
            checks_failed,
        ) = extract_unmatched_expectations_details(checkpoint_result)
        bad_indices_list = list(bad_indices)
        valid_df = df_to_be_validated.drop(index=bad_indices_list)
        invalid_df = df_to_be_validated.loc[bad_indices_list]
        invalid_df = invalid_df.assign(
            invalid_reason=invalid_df.index.map(
                lambda idx: "; ".join(reasons_by_index.get(idx, []))
            )
        )
        invalid_df = invalid_df.assign(validation_failed_at=datetime.now(timezone.utc))
        logger.info(f"Amount of valid records: {valid_df.shape[0]}")
        logger.debug(f"Content of valid records:\n {valid_df.head()}")
        logger.warning(f"Amount of invalid records: {invalid_df.shape[0]}")
        logger.warning(f"Content of invalid records:\n {invalid_df.head()}")
        reasons_list = [r for reasons in reasons_by_index.values() for r in reasons]
        failure_counts = {}
        for reason in reasons_list:
            failure_counts[reason] = failure_counts.get(reason, 0) + 1
        failure_reasons = [
            {"rule": rule, "count": count} for rule, count in failure_counts.items()
        ]
        expectations_summary = {
            "total_checks": total_checks,
            "checks_failed": checks_failed,
            "rows_failed": len(bad_indices),
            "failure_reasons": failure_reasons,
        }
    return valid_df, invalid_df, expectations_summary
    # --- DATA DOCS GENERATION ---
    gx_context.build_data_docs()
    # This finds the local path to the 'index.html' of your documentation
    docs_url = gx_context.get_docs_sites_urls()[0]["site_url"]
    # # Optional: Automatically open the docs in your default browser
    import webbrowser

    webbrowser.open(docs_url)
    logger.info(f"Data Docs generated at: {docs_url}")

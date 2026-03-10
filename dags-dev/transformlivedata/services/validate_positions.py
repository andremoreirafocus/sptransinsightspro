import pandas as pd
import json
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
import logging

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


def validate_transformed_positions(positions_table, transformed_expectations_config):
    def get_df_from_tuples_list(tuples_table):
        column_names = [
            "extracao_ts",
            "veiculo_id",
            "linha_lt",
            "linha_code",
            "linha_sentido",
            "lt_destino",
            "lt_origem",
            "veiculo_prefixo",
            "veiculo_acessivel",
            "veiculo_ts",
            "veiculo_lat",
            "veiculo_long",
            "is_circular",
            "first_stop_id",
            "first_stop_lat",
            "first_stop_lon",
            "last_stop_id",
            "last_stop_lat",
            "last_stop_lon",
            "distance_to_first_stop",
            "distance_to_last_stop",
        ]
        if column_names:
            df = pd.DataFrame(tuples_table, columns=column_names)
        else:
            df = pd.DataFrame(tuples_table)
        return df

    def extract_unmatched_expectations_details(checkpoint_result):
        bad_indices = set()
        for run_result in checkpoint_result.run_results.values():
            validation_results = run_result["validation_result"]["results"]
            for expectation_result in validation_results:
                result = expectation_result.get("result", {})
                unexpected_indices = result.get("unexpected_index_list") or []
                if len(unexpected_indices) > 0:
                    expectation_config = expectation_result.get(
                        "expectation_config", {}
                    )
                    expectation_type = expectation_config.get("expectation_type")
                    column = expectation_config.get("kwargs", {}).get("column")
                    unexpected_indices = result["unexpected_index_list"]
                    violation_count = len(unexpected_indices)
                    logger.info(
                        f"{expectation_type}"
                        + (f" (column: {column})" if column else "")
                        + f" -> violations: {violation_count}"
                    )
                    bad_indices.update(result["unexpected_index_list"])
        logger.info(f"Amount of invalid records: {len(bad_indices)}")
        return bad_indices

    positions_df = get_df_from_tuples_list(positions_table)
    gx_context = gx.get_context()
    with open(transformed_expectations_config, "r") as f:
        suite_dict = json.load(f)
    suite = ExpectationSuite(**suite_dict)
    gx_context.add_or_update_expectation_suite(expectation_suite=suite)
    datasource_name = "pandas_datasource"
    datasource = gx_context.sources.add_pandas(datasource_name)
    data_asset = datasource.add_dataframe_asset(name="positions_asset")
    batch_request = data_asset.build_batch_request(dataframe=positions_df)
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
    # checkpoint_result = checkpoint.run(result_format="BOOLEAN_ONLY")
    # checkpoint_result = checkpoint.run(result_format="BASIC")
    # checkpoint_result = checkpoint.run(result_format="SUMMARY")
    # checkpoint_result = checkpoint.run(result_format="COMPLETE")
    if checkpoint_result.success:
        logger.info("Validation successful!")
        valid_df = positions_df
        invalid_df = None
    else:
        logger.info("Validation failures detected!")
        logger.info("Checking for unmatched expectations...")
        bad_indices = extract_unmatched_expectations_details(checkpoint_result)
        valid_df = positions_df.drop(index=list(bad_indices))
        invalid_df = positions_df.loc[list(bad_indices)]
        logger.info(f"Amount of valid records: {valid_df.shape[0]}")
        logger.info(f"Content of valid records:\n {valid_df.head()}")
        logger.info(f"Amount of invalid records: {invalid_df.shape[0]}")
        logger.info(f"Content of invalid records:\n {invalid_df.head()}")
    return
    # --- DATA DOCS GENERATION ---
    gx_context.build_data_docs()
    # This finds the local path to the 'index.html' of your documentation
    docs_url = gx_context.get_docs_sites_urls()[0]["site_url"]
    # # Optional: Automatically open the docs in your default browser
    import webbrowser

    webbrowser.open(docs_url)
    logger.info(f"Data Docs generated at: {docs_url}")

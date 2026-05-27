import great_expectations as gx  # type: ignore[import-untyped]
from great_expectations.core.expectation_suite import ExpectationSuite  # type: ignore[import-untyped]
from datetime import datetime, timezone


def validate_expectations(df_to_be_validated, expectations_suite):
    def extract_unmatched_expectations_details(checkpoint_result):
        bad_indices = set()
        reasons_by_index = {}
        total_checks = 0
        expectations_with_violations = 0
        expectations_failed_due_to_exceptions = 0
        exception_reasons = []
        for run_result in checkpoint_result.run_results.values():
            validation_results = run_result["validation_result"]["results"]
            total_checks += len(validation_results)
            for expectation_result in validation_results:
                try:
                    exception_info = expectation_result["exception_info"]
                    expectation_config = expectation_result["expectation_config"]
                    expectation_type = expectation_config["expectation_type"]
                    kwargs = expectation_config["kwargs"]
                    column = kwargs.get("column")
                except Exception as e:
                    raise ValueError(f"Error while parsing GX result: {e}")
                raised_exception = False
                exception_message = None
                exception_traceback = None
                if "raised_exception" in exception_info:
                    raised_exception = exception_info["raised_exception"]
                    exception_message = exception_info["exception_message"]
                    exception_traceback = exception_info["exception_traceback"]
                else:
                    # Nested metric-id keyed exceptions
                    for _, info in exception_info.items():
                        if "raised_exception" in info and info["raised_exception"]:
                            raised_exception = True
                            exception_message = info["exception_message"]
                            exception_traceback = info["exception_traceback"]
                            break
                if raised_exception:
                    expectations_failed_due_to_exceptions += 1
                    exception_reasons.append(
                        {
                            "rule": expectation_type,
                            "column": column,
                            "exception_message": exception_message,
                            "exception_traceback": exception_traceback,
                        }
                    )
                    continue
                result = expectation_result["result"]
                unexpected_indices = (
                    result["unexpected_index_list"]
                    if "unexpected_index_list" in result
                    else []
                )
                if len(unexpected_indices) > 0:
                    expectations_with_violations += 1
                    reason = f"expectation_type:{expectation_type}" + (
                        f"|column:{column}" if column else ""
                    )
                    for idx in unexpected_indices:
                        bad_indices.add(idx)
                        reasons_by_index[idx] = reasons_by_index.get(idx, []) + [reason]
        return (
            bad_indices,
            reasons_by_index,
            total_checks,
            expectations_with_violations,
            expectations_failed_due_to_exceptions,
            exception_reasons,
        )

    gx_context = gx.get_context(mode="ephemeral")
    gx_context.variables.progress_bars = {"globally": False, "metric_calculations": False}
    suite_dict = expectations_suite
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
    checkpoint_result = checkpoint.run(result_format="COMPLETE")
    (
        bad_indices,
        reasons_by_index,
        total_checks,
        expectations_with_violations,
        expectations_failed_due_to_exceptions,
        exception_reasons,
    ) = extract_unmatched_expectations_details(checkpoint_result)
    expectations_successful = (
        total_checks
        - expectations_with_violations
        - expectations_failed_due_to_exceptions
    )
    if checkpoint_result.success:
        valid_df = df_to_be_validated
        invalid_df = None
        expectations_summary = {
            "total_checks": total_checks,
            "expectations_successful": expectations_successful,
            "expectations_with_violations": expectations_with_violations,
            "expectations_failed_due_to_exceptions": expectations_failed_due_to_exceptions,
            "rows_failed": 0,
            "violation_reasons": [],
            "exception_reasons": exception_reasons,
        }
    else:
        bad_indices_list = list(bad_indices)
        valid_df = df_to_be_validated.drop(index=bad_indices_list)
        invalid_df = df_to_be_validated.loc[bad_indices_list]
        invalid_df = invalid_df.assign(
            invalid_reason=invalid_df.index.map(
                lambda idx: "; ".join(reasons_by_index.get(idx, []))
            )
        )
        invalid_df = invalid_df.assign(validation_failed_at=datetime.now(timezone.utc))
        reasons_list = [r for reasons in reasons_by_index.values() for r in reasons]
        failure_counts = {}
        for reason in reasons_list:
            failure_counts[reason] = failure_counts.get(reason, 0) + 1
        failure_reasons = [
            {"rule": rule, "count": count} for rule, count in failure_counts.items()
        ]
        expectations_summary = {
            "total_checks": total_checks,
            "expectations_successful": expectations_successful,
            "expectations_with_violations": expectations_with_violations,
            "expectations_failed_due_to_exceptions": expectations_failed_due_to_exceptions,
            "rows_failed": len(bad_indices),
            "violation_reasons": failure_reasons,
            "exception_reasons": exception_reasons,
        }
    return {
        "valid_df": valid_df,
        "invalid_df": invalid_df,
        "expectations_summary": expectations_summary,
    }

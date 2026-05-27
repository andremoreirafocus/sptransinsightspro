#!/usr/bin/env bash
set -u

EXAMPLE_DAG_IDS=(
  "example_bash_operator"
  "example_branch_datetime_operator"
  "example_branch_datetime_operator_2"
  "example_branch_datetime_operator_3"
  "example_branch_dop_operator_v3"
  "example_branch_labels"
  "example_branch_operator"
  "example_branch_python_operator_decorator"
  "example_complex"
  "example_dag_decorator"
  "example_dynamic_task_mapping"
  "example_dynamic_task_mapping_with_no_taskflow_operators"
  "example_external_task_marker_child"
  "example_external_task_marker_parent"
  "example_kubernetes_executor"
  "example_local_kubernetes_executor"
  "example_nested_branch_dag"
  "example_params_trigger_ui"
  "example_params_ui_tutorial"
  "example_passing_params_via_test_command"
  "example_python_decorator"
  "example_python_operator"
  "example_sensor_decorator"
  "example_sensors"
  "example_setup_teardown"
  "example_setup_teardown_taskflow"
  "example_short_circuit_decorator"
  "example_short_circuit_operator"
  "example_skip_dag"
  "example_sla_dag"
  "example_subdag_operator"
  "example_subdag_operator.section-1"
  "example_subdag_operator.section-2"
  "example_task_group"
  "example_task_group_decorator"
  "example_time_delta_sensor_async"
  "example_trigger_controller_dag"
  "example_trigger_target_dag"
  "example_weekday_branch_operator"
  "example_xcom"
  "example_xcom_args"
  "example_xcom_args_with_operators"
)

if ! command -v airflow >/dev/null 2>&1; then
  echo "Error: airflow CLI not found in PATH." >&2
  exit 1
fi

success_count=0
fail_count=0

echo "Deleting ${#EXAMPLE_DAG_IDS[@]} example DAG(s)..."

for dag_id in "${EXAMPLE_DAG_IDS[@]}"; do
  echo "[DELETE] ${dag_id}"
  if airflow dags delete "${dag_id}" --yes; then
    success_count=$((success_count + 1))
  else
    fail_count=$((fail_count + 1))
    echo "[WARN] Failed to delete: ${dag_id}" >&2
  fi
  echo
 done

echo "Finished. Success: ${success_count} | Failed: ${fail_count}"

if [ "${fail_count}" -gt 0 ]; then
  exit 2
fi

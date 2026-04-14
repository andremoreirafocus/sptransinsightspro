import sys
import os
import argparse

from os_command_helper import run_command
from deploy_helpers import run_code_validations


def promote_pipeline(pipeline_name):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dev_dir = os.path.join(project_root, "dags-dev")
    prod_dir = os.path.join(project_root, "airflow", "dags")
    pipeline_dev_path = os.path.join(dev_dir, pipeline_name)
    pipeline_prod_path = os.path.join(prod_dir, pipeline_name)
    infra_dev_path = os.path.join(dev_dir, "infra")
    infra_prod_path = os.path.join(prod_dir, "infra")
    pipeline_configurator_dev_path = os.path.join(dev_dir, "pipeline_configurator")
    pipeline_configurator_prod_path = os.path.join(prod_dir, "pipeline_configurator")
    # 1. Check if pipeline exists
    if not os.path.isdir(pipeline_dev_path):
        print(f"❌ Pipeline folder '{pipeline_name}' not found in dags-dev/")
        sys.exit(1)
    print(f"🚀 Promoting pipeline: {pipeline_name}")
    # 2. Validation (Linting + Tests)
    test_dir = os.path.join(pipeline_dev_path, "tests")
    has_tests = os.path.isdir(test_dir)
    total_steps = (2 if has_tests else 1) + 2
    steps_consumed = run_code_validations(pipeline_dev_path, pipeline_name, step_offset=1)
    # 3. Atomic Sync Folder
    sync_step = steps_consumed + 1
    print(
        f"Step {sync_step}/{total_steps}: Syncing folder '{pipeline_name}' to production..."
    )
    os.makedirs(pipeline_prod_path, exist_ok=True)
    run_command(
        [
            "rsync",
            "-av",
            "--delete",
            "--exclude",
            "__pycache__",
            "--exclude",
            ".pytest_cache",
            f"{pipeline_dev_path}/",
            f"{pipeline_prod_path}/",
        ],
        "Folder sync failed!",
    )
    print(
        f"Step {sync_step + 1}/{total_steps}: Syncing shared infra and pipeline_configurator files..."
    )
    os.makedirs(infra_prod_path, exist_ok=True)
    run_command(
        [
            "rsync",
            "-av",
            "--delete",
            "--exclude",
            "__pycache__",
            f"{infra_dev_path}/",
            f"{infra_prod_path}/",
        ],
        "Infra sync failed!",
    )
    os.makedirs(pipeline_configurator_prod_path, exist_ok=True)
    run_command(
        [
            "rsync",
            "-av",
            "--delete",
            "--exclude",
            "__pycache__",
            f"{pipeline_configurator_dev_path}/",
            f"{pipeline_configurator_prod_path}/",
        ],
        "pipeline_configurator sync failed!",
    )
    print(f"\n✅ Pipeline '{pipeline_name}' promoted successfully to production!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Promote a single pipeline from dev to prod."
    )
    parser.add_argument(
        "pipeline_name", help="The name of the pipeline folder (e.g. transformlivedata)"
    )
    args = parser.parse_args()

    promote_pipeline(args.pipeline_name)

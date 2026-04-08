import subprocess
import sys
import os
import argparse
import glob


def run_command(command, error_msg):
    """Utility to run shell commands and handle failures."""
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError:
        print(f"\n❌ {error_msg}")
        sys.exit(1)


def promote_pipeline(pipeline_name):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dev_dir = os.path.join(project_root, "dags-dev")
    prod_dir = os.path.join(project_root, "airflow", "dags")

    pipeline_dev_path = os.path.join(dev_dir, pipeline_name)
    pipeline_prod_path = os.path.join(prod_dir, pipeline_name)
    infra_dev_path = os.path.join(dev_dir, "infra")
    infra_prod_path = os.path.join(prod_dir, "infra")

    # 1. Check if pipeline exists
    if not os.path.isdir(pipeline_dev_path):
        print(f"❌ Pipeline folder '{pipeline_name}' not found in dags-dev/")
        sys.exit(1)

    print(f"🚀 Promoting pipeline: {pipeline_name}")

    # 2. Validation (Linting)
    print(f"Step 1/4: Linting {pipeline_name}...")
    run_command(
        f"ruff check {pipeline_dev_path}", f"Linting failed for {pipeline_name}!"
    )
    print("✅ Linting Passed.")

    # 3. Validation (Unit Tests)
    test_dir = os.path.join(pipeline_dev_path, "tests")
    if os.path.isdir(test_dir):
        print(f"test_dir: {test_dir}")
        print(f"Step 2/4: Running tests for {pipeline_name}...")
        run_command(
            f"python3 -m pytest {test_dir}", f"Tests failed for {pipeline_name}!"
        )
        print("✅ Unit Tests Passed.")
    else:
        print(f"Step 2/4: No tests found in {test_dir}, skipping.")

    # 4. Atomic Sync Folder
    print(f"Step 3/4: Syncing folder '{pipeline_name}' to production...")
    os.makedirs(pipeline_prod_path, exist_ok=True)
    sync_folder_cmd = (
        f"rsync -av --delete "
        f"--exclude '__pycache__' --exclude '.pytest_cache' "
        f"{pipeline_dev_path}/ {pipeline_prod_path}/"
    )
    run_command(sync_folder_cmd, "Folder sync failed!")

    # 5. Atomic Sync Matching DAG Files
    # print("Step 4/4: Syncing DAG files and shared infra...")
    # dag_pattern = os.path.join(dev_dir, f"{pipeline_name}*.py")
    # dag_files = glob.glob(dag_pattern)
    # for dag_file in dag_files:
    #     filename = os.path.basename(dag_file)
    #     print(f"   - Syncing {filename}")
    #     run_command(
    #         f"cp {dag_file} {prod_dir}/", f"Failed to copy DAG file: {filename}"
    #     )

    # 6. Atomic Sync Infra (Required Dependency)
    print("Step 4/4: Syncing shared infra files...")
    os.makedirs(infra_prod_path, exist_ok=True)
    sync_infra_cmd = (
        f"rsync -av --delete "
        f"--exclude '__pycache__' "
        f"{infra_dev_path}/ {infra_prod_path}/"
    )
    run_command(sync_infra_cmd, "Infra sync failed!")

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

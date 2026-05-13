import sys
import os
import argparse

from os_command_helper import run_command
from deploy_helpers import run_code_validations


def deploy_service(service_name, service_folder):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    service_path = os.path.join(project_root, service_folder)

    # 1. Check if service folder exists
    if not os.path.isdir(service_path):
        print(f"❌ Service folder '{service_folder}' not found in {project_root}/")
        sys.exit(1)

    print(f"🚀 Deploying microservice: {service_name} (folder: {service_folder})")

    # Validation (Linting + Tests)
    steps_consumed = run_code_validations(service_path, service_name, step_offset=1)
    mypy_step = steps_consumed + 1
    print(f"Step {mypy_step}/?: Running mypy for {service_name}...")
    run_command(
        [
            sys.executable,
            "-m",
            "mypy",
            "--explicit-package-bases",
            os.path.join(service_path, "src"),
        ],
        f"Type checking (mypy) failed for {service_name}!",
    )
    print("✅ Type checking Passed.")

    compose_file = os.path.join(project_root, "docker-compose.yml")
    build_step = steps_consumed + 2
    total_steps = steps_consumed + 3
    print(f"Step {build_step}/{total_steps}: Building {service_name}...")
    run_command(
        ["docker", "compose", "-f", compose_file, "build", service_name],
        "Build failed!",
    )
    print("✅ Build Successful.")

    print(f"Step {build_step + 1}/{total_steps}: Restarting {service_name}...")
    run_command(
        ["docker", "compose", "-f", compose_file, "up", "-d", service_name],
        "Deployment failed!",
    )
    print(f"✅ Service '{service_name}' is up and running.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build and deploy a microservice.")
    parser.add_argument(
        "service_name", help="Name of the service as defined in docker-compose.yml"
    )
    parser.add_argument(
        "service_folder",
        help="The subfolder containing the service code (e.g. extractloadlivedata)",
    )
    args = parser.parse_args()

    deploy_service(args.service_name, args.service_folder)

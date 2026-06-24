import sys
import os
import argparse

from os_command_helper import run_command
from deploy_helpers import run_code_validations


def deploy_service(service_name, service_folder, run_deploy):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    service_path = os.path.join(project_root, service_folder)

    # 1. Check if service folder exists
    if not os.path.isdir(service_path):
        print(f"❌ Service folder '{service_folder}' not found in {project_root}/")
        sys.exit(1)

    if run_deploy:
        print(f"🚀 Deploying microservice: {service_name} (folder: {service_folder})")
    else:
        print(f"🔍 Validating microservice: {service_name} (folder: {service_folder})")

    # Validation (Linting + SAST + Tests + Type checking)
    test_dir = os.path.join(service_path, "tests")
    has_tests = os.path.isdir(test_dir)
    steps_consumed = 4 if has_tests else 3
    total_steps = steps_consumed + (2 if run_deploy else 0)
    steps_consumed = run_code_validations(
        service_path,
        service_name,
        step_offset=1,
        total_steps=total_steps,
        mypy_extra_args=["--explicit-package-bases"],
        mypy_target=os.path.join(service_path, "src"),
    )

    if run_deploy:
        compose_file = os.path.join(project_root, "docker-compose.yml")
        build_step = steps_consumed + 1
        print(f"Step {build_step}/{total_steps}: Building {service_name}...")

        prefix = os.environ.get("DOCKER_CMD_PREFIX", "").split()
        docker_compose_cmd = prefix + ["docker", "compose"]

        run_command(
            docker_compose_cmd + ["-f", compose_file, "build", service_name],
            "Build failed!",
        )
        print("✅ Build Successful.")

        print(f"Step {build_step + 1}/{total_steps}: Restarting {service_name}...")
        run_command(
            docker_compose_cmd + ["-f", compose_file, "up", "-d", service_name],
            "Deployment failed!",
        )
        print(f"✅ Service '{service_name}' is up and running.")
    else:
        print(f"\n✅ Service '{service_name}' passed all validations.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build and deploy a microservice.")
    parser.add_argument(
        "service_name", help="Name of the service as defined in docker-compose.yml"
    )
    parser.add_argument(
        "service_folder",
        help="The subfolder containing the service code (e.g. extractloadlivedata)",
    )
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--check",
        action="store_true",
        help="Run validations only (lint, SAST, tests, type-check). No build or deploy.",
    )
    mode_group.add_argument(
        "--prod",
        action="store_true",
        help="Run validations, build, and deploy.",
    )
    args = parser.parse_args()

    deploy_service(args.service_name, args.service_folder, run_deploy=args.prod)

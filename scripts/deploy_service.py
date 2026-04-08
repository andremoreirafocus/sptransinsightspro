import subprocess
import sys
import os
import argparse

def run_command(command, error_msg):
    """Utility to run shell commands and handle failures."""
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError:
        print(f"\n❌ {error_msg}")
        sys.exit(1)

def deploy_service(service_name, service_folder):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    service_path = os.path.join(project_root, service_folder)
    
    # 1. Check if service folder exists
    if not os.path.isdir(service_path):
        print(f"❌ Service folder '{service_folder}' not found in {project_root}/")
        sys.exit(1)

    print(f"🚀 Deploying microservice: {service_name} (folder: {service_folder})")

    # Assuming docker-compose.yml is in the project root
    compose_file = os.path.join(project_root, "docker-compose.yml")
    
    # 2. Build Docker Image
    print(f"Step 1/2: Building {service_name}...")
    run_command(f"docker compose -f {compose_file} build {service_name}", "Build failed!")
    print("✅ Build Successful.")

    # 3. Restart Container
    print(f"Step 2/2: Restarting {service_name}...")
    run_command(f"docker compose -f {compose_file} up -d {service_name}", "Deployment failed!")
    print(f"✅ Service '{service_name}' is up and running.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build and deploy a microservice.")
    parser.add_argument("service_name", help="Name of the service as defined in docker-compose.yml")
    parser.add_argument("service_folder", help="The subfolder containing the service code (e.g. extractloadlivedata)")
    args = parser.parse_args()
    
    deploy_service(args.service_name, args.service_folder)

import subprocess
import sys


def run_command(command: list, error_msg: str) -> None:
    """Run a subprocess command and exit with an error message on failure."""
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {error_msg} (exit code {e.returncode})")
        sys.exit(1)

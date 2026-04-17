import sys
import os

from os_command_helper import run_command


def _resolve_python_executable(folder: str) -> str:
    local_venv_python = os.path.join(folder, ".venv", "bin", "python")
    if os.path.isfile(local_venv_python) and os.access(local_venv_python, os.X_OK):
        return local_venv_python
    return sys.executable


def run_code_validations(folder: str, label: str, step_offset: int = 1) -> int:
    """Run lint, SAST, and pytest (if tests exist) against a folder.

    Prints step labels starting from step_offset.
    Returns the number of steps consumed (2 if no tests, 3 if tests found).
    """
    test_dir = os.path.join(folder, "tests")
    has_tests = os.path.isdir(test_dir)
    python_executable = _resolve_python_executable(folder)

    print(f"Step {step_offset}/?: Linting {label}...")
    run_command(
        [python_executable, "-m", "ruff", "check", folder],
        f"Linting failed for {label}!",
    )
    print("✅ Linting Passed.")

    print(
        f"Step {step_offset + 1}/?: Running SAST (bandit, high severity) for {label}..."
    )
    bandit_cmd = [
        python_executable,
        "-m",
        "bandit",
        "-q",
        "-r",
        folder,
        "-lll",
    ]
    excluded_paths = []
    if has_tests:
        excluded_paths.append(test_dir)
    for local_exclude in [".venv", "venv", "__pycache__", ".pytest_cache"]:
        candidate = os.path.join(folder, local_exclude)
        if os.path.exists(candidate):
            excluded_paths.append(candidate)
    if excluded_paths:
        bandit_cmd.extend(["--exclude", ",".join(excluded_paths)])
    run_command(bandit_cmd, f"SAST (bandit) failed for {label}!")
    print("✅ SAST Passed.")

    if has_tests:
        print(f"Step {step_offset + 2}/?: Running tests for {label}...")
        run_command(
            [python_executable, "-m", "pytest", test_dir],
            f"Tests failed for {label}!",
        )
        print("✅ Unit Tests Passed.")

    return 3 if has_tests else 2

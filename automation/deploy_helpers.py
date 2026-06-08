import sys
import os

from os_command_helper import run_command


def resolve_python_executable(folder: str) -> str:
    for base in (folder, os.path.dirname(folder)):
        candidate = os.path.join(base, ".venv", "bin", "python")
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return sys.executable


def run_code_validations(
    folder: str,
    label: str,
    step_offset: int = 1,
    total_steps: int = 0,
    mypy_extra_args: list | None = None,
    mypy_target: str | None = None,
) -> int:
    """Run lint, SAST, pytest (if tests exist), and mypy against a folder.

    Prints step labels starting from step_offset.
    Returns the number of steps consumed (3 if no tests, 4 if tests found).
    mypy_extra_args defaults to ["--exclude", "tests"].
    mypy_target defaults to folder.
    """
    test_dir = os.path.join(folder, "tests")
    has_tests = os.path.isdir(test_dir)
    python_executable = resolve_python_executable(folder)

    if mypy_extra_args is None:
        mypy_extra_args = ["--exclude", "tests"]
    if mypy_target is None:
        mypy_target = folder

    if total_steps <= 0:
        total_steps = step_offset + (4 if has_tests else 3) - 1

    print(f"Step {step_offset}/{total_steps}: Linting {label}...")
    run_command(
        [python_executable, "-m", "ruff", "check", folder],
        f"Linting failed for {label}!",
    )
    print("✅ Linting Passed.")

    print(
        f"Step {step_offset + 1}/{total_steps}: Running SAST (bandit, high severity) for {label}..."
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
        print(f"Step {step_offset + 2}/{total_steps}: Running tests for {label}...")
        run_command(
            [python_executable, "-m", "pytest", test_dir],
            f"Tests failed for {label}!",
        )
        print("✅ Unit Tests Passed.")

    mypy_step_offset = step_offset + (3 if has_tests else 2)
    print(f"Step {mypy_step_offset}/{total_steps}: Running mypy for {label}...")
    run_command(
        [python_executable, "-m", "mypy", *mypy_extra_args, mypy_target],
        f"Type checking (mypy) failed for {label}!",
    )
    print("✅ Type checking Passed.")

    return 4 if has_tests else 3

import sys
import os

from os_command_helper import run_command


def run_code_validations(folder: str, label: str, step_offset: int = 1) -> int:
    """Run ruff lint and pytest (if tests exist) against a folder.

    Prints step labels starting from step_offset.
    Returns the number of steps consumed (1 if no tests, 2 if tests found).
    """
    test_dir = os.path.join(folder, "tests")
    has_tests = os.path.isdir(test_dir)

    print(f"Step {step_offset}/?: Linting {label}...")
    run_command(["ruff", "check", folder], f"Linting failed for {label}!")
    print("✅ Linting Passed.")

    if has_tests:
        print(f"Step {step_offset + 1}/?: Running tests for {label}...")
        run_command(
            [sys.executable, "-m", "pytest", test_dir],
            f"Tests failed for {label}!",
        )
        print("✅ Unit Tests Passed.")

    return 2 if has_tests else 1

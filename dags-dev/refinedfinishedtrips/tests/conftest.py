import os
import sys


def pytest_configure():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    tests_dir = os.path.abspath(os.path.dirname(__file__))
    for path in [project_root, tests_dir]:
        if path not in sys.path:
            sys.path.insert(0, path)

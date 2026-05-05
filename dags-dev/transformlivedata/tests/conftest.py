import os
import sys
import pytest

from transformlivedata.tests.fakes import FakeTransformPositionsDependencies


def pytest_configure(config):
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    shared_fakes = os.path.join(project_root, "tests")
    for path in [project_root, shared_fakes]:
        if path not in sys.path:
            sys.path.insert(0, path)


@pytest.fixture
def make_transform_positions_deps():
    return FakeTransformPositionsDependencies.create

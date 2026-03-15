"""
Root-level conftest.

Pipeline-specific fixtures live in tests/pipeline/conftest.py.
"""

import pytest


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"

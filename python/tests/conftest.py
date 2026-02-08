"""Shared fixtures for kanoniv-sdk tests."""

from __future__ import annotations

import pytest
import respx

import kanoniv


BASE_URL = "http://test-api.kanoniv.local"


@pytest.fixture()
def mock_api():
    """Activate a respx mock router scoped to the test API base URL."""
    with respx.mock(base_url=BASE_URL) as router:
        yield router


@pytest.fixture()
def client():
    """Create a sync client pointed at the test base URL."""
    c = kanoniv.Client(api_key="kn_test_key", base_url=BASE_URL, max_retries=0)
    yield c
    c.close()


@pytest.fixture()
def async_client():
    """Create an async client pointed at the test base URL."""
    return kanoniv.AsyncClient(api_key="kn_test_key", base_url=BASE_URL, max_retries=0)

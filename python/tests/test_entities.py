"""Tests for the entities resource."""

from __future__ import annotations

import httpx
import pytest

BASE_URL = "http://test-api.kanoniv.local"


class TestEntitiesSearch:
    def test_search_with_query(self, mock_api, client):
        mock_api.get("/v1/entities").mock(
            return_value=httpx.Response(
                200,
                json={"data": [{"id": "e-1", "name": "John"}], "total": 1},
            )
        )
        result = client.entities.search(q="john")
        assert len(result["data"]) == 1
        assert result["total"] == 1

    def test_search_with_filters(self, mock_api, client):
        route = mock_api.get("/v1/entities").mock(
            return_value=httpx.Response(200, json={"data": [], "total": 0})
        )
        client.entities.search(q="test", entity_type="contact", limit=10, offset=5)
        url = route.calls[0].request.url
        assert "entity_type=contact" in str(url)
        assert "limit=10" in str(url)
        assert "offset=5" in str(url)

    def test_search_omits_none_params(self, mock_api, client):
        route = mock_api.get("/v1/entities").mock(
            return_value=httpx.Response(200, json={"data": []})
        )
        client.entities.search(q="test")
        url = str(route.calls[0].request.url)
        assert "entity_type" not in url
        assert "limit" not in url


class TestEntitiesGet:
    def test_get_canonical(self, mock_api, client):
        mock_api.get("/v1/canonical/c-123").mock(
            return_value=httpx.Response(
                200,
                json={"id": "c-123", "canonical_data": {"email": "j@acme.com"}},
            )
        )
        result = client.entities.get("c-123")
        assert result["id"] == "c-123"

    def test_get_linked(self, mock_api, client):
        mock_api.get("/v1/canonical/c-123/linked").mock(
            return_value=httpx.Response(
                200,
                json={"linked": [{"source_id": "s-1", "external_id": "ext-1"}]},
            )
        )
        result = client.entities.get_linked("c-123")
        assert len(result["linked"]) == 1

    def test_history(self, mock_api, client):
        mock_api.get("/v1/entities/e-1/history").mock(
            return_value=httpx.Response(
                200,
                json={"events": [{"event_type": "merge", "at": "2024-01-01T00:00:00Z"}]},
            )
        )
        result = client.entities.history("e-1")
        assert result["events"][0]["event_type"] == "merge"


class TestEntitiesAsync:
    @pytest.mark.asyncio
    async def test_async_search(self, mock_api, async_client):
        mock_api.get("/v1/entities").mock(
            return_value=httpx.Response(200, json={"data": [{"id": "e-1"}], "total": 1})
        )
        result = await async_client.entities.search(q="test")
        assert result["total"] == 1
        await async_client.close()

    @pytest.mark.asyncio
    async def test_async_get(self, mock_api, async_client):
        mock_api.get("/v1/canonical/c-1").mock(
            return_value=httpx.Response(200, json={"id": "c-1"})
        )
        result = await async_client.entities.get("c-1")
        assert result["id"] == "c-1"
        await async_client.close()

"""Tests for the top-level client methods: resolve, ingest, stats."""

from __future__ import annotations

import httpx
import pytest
import respx

import kanoniv
from kanoniv.exceptions import (
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    ServerError,
    ValidationError,
)

BASE_URL = "http://test-api.kanoniv.local"


class TestResolve:
    def test_resolve_by_system_and_id(self, mock_api, client):
        mock_api.get("/v1/resolve").mock(
            return_value=httpx.Response(
                200,
                json={
                    "canonical_id": "c-123",
                    "canonical_data": {"name": "John"},
                },
            )
        )
        result = client.resolve(system="crm", external_id="sf_123")
        assert result["canonical_id"] == "c-123"
        assert result["canonical_data"]["name"] == "John"

    def test_resolve_by_query(self, mock_api, client):
        mock_api.get("/v1/resolve").mock(
            return_value=httpx.Response(200, json={"results": []})
        )
        result = client.resolve(query="john@acme.com")
        assert result["results"] == []

    def test_api_key_header_sent(self, mock_api, client):
        route = mock_api.get("/v1/resolve").mock(
            return_value=httpx.Response(200, json={})
        )
        client.resolve(system="x", external_id="1")
        assert route.calls[0].request.headers["x-api-key"] == "kn_test_key"

    def test_bearer_token_auth(self, mock_api):
        route = mock_api.get("/v1/resolve").mock(
            return_value=httpx.Response(200, json={})
        )
        c = kanoniv.Client(access_token="eyJ_test", base_url=BASE_URL, max_retries=0)
        c.resolve(system="x", external_id="1")
        assert route.calls[0].request.headers["authorization"] == "Bearer eyJ_test"
        c.close()


class TestIngest:
    def test_ingest_records(self, mock_api, client):
        mock_api.post("/v1/ingest/webhook/src-1").mock(
            return_value=httpx.Response(200, json={"ingested": 2})
        )
        result = client.ingest("src-1", records=[{"id": "a"}, {"id": "b"}])
        assert result["ingested"] == 2


class TestStats:
    def test_stats(self, mock_api, client):
        mock_api.get("/v1/stats").mock(
            return_value=httpx.Response(
                200,
                json={"total_canonical_entities": 42, "total_sources": 3},
            )
        )
        result = client.stats()
        assert result["total_canonical_entities"] == 42


class TestErrorMapping:
    def test_401_raises_authentication_error(self, mock_api, client):
        mock_api.get("/v1/stats").mock(
            return_value=httpx.Response(401, json={"error": "Unauthorized"})
        )
        with pytest.raises(AuthenticationError):
            client.stats()

    def test_400_raises_validation_error(self, mock_api, client):
        mock_api.get("/v1/resolve").mock(
            return_value=httpx.Response(400, json={"error": "missing param"})
        )
        with pytest.raises(ValidationError):
            client.resolve()

    def test_404_raises_not_found(self, mock_api, client):
        mock_api.get("/v1/canonical/nonexistent").mock(
            return_value=httpx.Response(404, json={"error": "not found"})
        )
        with pytest.raises(NotFoundError):
            client.entities.get("nonexistent")

    def test_429_raises_rate_limit_error(self, mock_api, client):
        mock_api.get("/v1/stats").mock(
            return_value=httpx.Response(
                429,
                json={"error": "rate limited"},
                headers={"Retry-After": "5"},
            )
        )
        with pytest.raises(RateLimitError) as exc_info:
            client.stats()
        assert exc_info.value.retry_after == 5.0

    def test_500_raises_server_error(self, mock_api, client):
        mock_api.get("/v1/stats").mock(
            return_value=httpx.Response(500, json={"error": "internal"})
        )
        with pytest.raises(ServerError):
            client.stats()


class TestContextManager:
    def test_sync_context_manager(self, mock_api):
        mock_api.get("/v1/stats").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        with kanoniv.Client(api_key="kn_test", base_url=BASE_URL, max_retries=0) as c:
            result = c.stats()
        assert result["ok"] is True

    @pytest.mark.asyncio
    async def test_async_context_manager(self, mock_api):
        mock_api.get("/v1/stats").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        async with kanoniv.AsyncClient(
            api_key="kn_test", base_url=BASE_URL, max_retries=0
        ) as c:
            result = await c.stats()
        assert result["ok"] is True


class TestAsyncClient:
    @pytest.mark.asyncio
    async def test_async_resolve(self, mock_api, async_client):
        mock_api.get("/v1/resolve").mock(
            return_value=httpx.Response(200, json={"canonical_id": "c-1"})
        )
        result = await async_client.resolve(system="crm", external_id="1")
        assert result["canonical_id"] == "c-1"
        await async_client.close()

    @pytest.mark.asyncio
    async def test_async_ingest(self, mock_api, async_client):
        mock_api.post("/v1/ingest/webhook/src-1").mock(
            return_value=httpx.Response(200, json={"ingested": 1})
        )
        result = await async_client.ingest("src-1", records=[{"id": "a"}])
        assert result["ingested"] == 1
        await async_client.close()

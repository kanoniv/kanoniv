"""Tests for sources, rules, jobs, reviews, overrides, audit, and specs resources."""

from __future__ import annotations

import httpx
import pytest

BASE_URL = "http://test-api.kanoniv.local"


# -- Sources -------------------------------------------------------------------

class TestSources:
    def test_list(self, mock_api, client):
        mock_api.get("/v1/sources").mock(
            return_value=httpx.Response(200, json=[{"id": "s-1", "name": "CRM"}])
        )
        result = client.sources.list()
        assert len(result) == 1

    def test_get(self, mock_api, client):
        mock_api.get("/v1/sources/s-1").mock(
            return_value=httpx.Response(200, json={"id": "s-1", "name": "CRM"})
        )
        result = client.sources.get("s-1")
        assert result["name"] == "CRM"

    def test_create(self, mock_api, client):
        route = mock_api.post("/v1/sources").mock(
            return_value=httpx.Response(201, json={"id": "s-new"})
        )
        result = client.sources.create(name="New", source_type="webhook")
        assert result["id"] == "s-new"

    def test_update(self, mock_api, client):
        mock_api.put("/v1/sources/s-1").mock(
            return_value=httpx.Response(200, json={"id": "s-1", "name": "Updated"})
        )
        result = client.sources.update("s-1", name="Updated")
        assert result["name"] == "Updated"

    def test_delete(self, mock_api, client):
        mock_api.delete("/v1/sources/s-1").mock(
            return_value=httpx.Response(204)
        )
        result = client.sources.delete("s-1")
        assert result is None

    def test_sync(self, mock_api, client):
        mock_api.post("/v1/sources/s-1/sync").mock(
            return_value=httpx.Response(200, json={"status": "syncing"})
        )
        result = client.sources.sync("s-1")
        assert result["status"] == "syncing"

    def test_preview(self, mock_api, client):
        mock_api.get("/v1/ingest/sources/s-1/preview").mock(
            return_value=httpx.Response(200, json={"rows": [{"a": 1}]})
        )
        result = client.sources.preview("s-1")
        assert len(result["rows"]) == 1


# -- Rules ---------------------------------------------------------------------

class TestRules:
    def test_list(self, mock_api, client):
        mock_api.get("/v1/rules").mock(
            return_value=httpx.Response(
                200, json=[{"name": "email_exact", "rule_type": "exact"}]
            )
        )
        result = client.rules.list()
        assert result[0]["name"] == "email_exact"

    def test_create(self, mock_api, client):
        mock_api.post("/v1/rules").mock(
            return_value=httpx.Response(201, json={"id": "r-1", "version": 1})
        )
        result = client.rules.create(
            name="email_exact",
            rule_type="exact",
            config={"field": "email"},
            weight=1.0,
        )
        assert result["version"] == 1

    def test_history(self, mock_api, client):
        mock_api.get("/v1/rules/email_exact/history").mock(
            return_value=httpx.Response(200, json=[{"version": 1}, {"version": 2}])
        )
        result = client.rules.history("email_exact")
        assert len(result) == 2


# -- Jobs ----------------------------------------------------------------------

class TestJobs:
    def test_list(self, mock_api, client):
        mock_api.get("/v1/jobs").mock(
            return_value=httpx.Response(200, json=[{"job_id": "j-1"}])
        )
        result = client.jobs.list()
        assert result[0]["job_id"] == "j-1"

    def test_list_with_filters(self, mock_api, client):
        route = mock_api.get("/v1/jobs").mock(
            return_value=httpx.Response(200, json=[])
        )
        client.jobs.list(limit=5, job_type="reconciliation")
        url = str(route.calls[0].request.url)
        assert "limit=5" in url
        assert "job_type=reconciliation" in url

    def test_get(self, mock_api, client):
        mock_api.get("/v1/jobs/j-1").mock(
            return_value=httpx.Response(
                200, json={"job_id": "j-1", "status": "completed"}
            )
        )
        result = client.jobs.get("j-1")
        assert result["status"] == "completed"

    def test_run(self, mock_api, client):
        mock_api.post("/v1/jobs/run").mock(
            return_value=httpx.Response(200, json={"job_id": "j-2", "status": "queued"})
        )
        result = client.jobs.run("reconciliation")
        assert result["status"] == "queued"

    def test_cancel(self, mock_api, client):
        mock_api.post("/v1/jobs/j-1/cancel").mock(
            return_value=httpx.Response(200, json={"status": "cancelled"})
        )
        result = client.jobs.cancel("j-1")
        assert result["status"] == "cancelled"


# -- Reviews -------------------------------------------------------------------

class TestReviews:
    def test_list_pending(self, mock_api, client):
        mock_api.get("/v1/resolve/pending").mock(
            return_value=httpx.Response(200, json=[{"entity_a": {}, "entity_b": {}}])
        )
        result = client.reviews.list()
        assert len(result) == 1

    def test_decide(self, mock_api, client):
        mock_api.post("/v1/resolve/quick").mock(
            return_value=httpx.Response(200, json={"status": "merged"})
        )
        result = client.reviews.decide(
            entity_a_id="e-1",
            entity_b_id="e-2",
            decision="merge",
            reason="same person",
        )
        assert result["status"] == "merged"


# -- Overrides -----------------------------------------------------------------

class TestOverrides:
    def test_list(self, mock_api, client):
        mock_api.get("/v1/overrides").mock(
            return_value=httpx.Response(200, json=[{"id": "o-1"}])
        )
        result = client.overrides.list()
        assert len(result) == 1

    def test_create(self, mock_api, client):
        mock_api.post("/v1/overrides").mock(
            return_value=httpx.Response(201, json={"id": "o-2"})
        )
        result = client.overrides.create(
            override_type="force_merge",
            entity_a_id="e-1",
            entity_b_id="e-2",
        )
        assert result["id"] == "o-2"

    def test_delete(self, mock_api, client):
        mock_api.delete("/v1/overrides/o-1").mock(
            return_value=httpx.Response(204)
        )
        result = client.overrides.delete("o-1")
        assert result is None


# -- Audit ---------------------------------------------------------------------

class TestAudit:
    def test_list(self, mock_api, client):
        mock_api.get("/v1/audit").mock(
            return_value=httpx.Response(
                200, json=[{"event_type": "merge", "entity_id": "e-1"}]
            )
        )
        result = client.audit.list()
        assert result[0]["event_type"] == "merge"

    def test_entity_trail(self, mock_api, client):
        mock_api.get("/v1/audit/entity/e-1").mock(
            return_value=httpx.Response(
                200, json=[{"event_type": "create"}, {"event_type": "merge"}]
            )
        )
        result = client.audit.entity_trail("e-1")
        assert len(result) == 2


# -- Specs ---------------------------------------------------------------------

class TestSpecs:
    def test_list(self, mock_api, client):
        mock_api.get("/v1/identity/specs").mock(
            return_value=httpx.Response(200, json=[{"version": 1}])
        )
        result = client.specs.list()
        assert result[0]["version"] == 1

    def test_get(self, mock_api, client):
        mock_api.get("/v1/identity/specs/1").mock(
            return_value=httpx.Response(
                200, json={"version": 1, "spec_yaml": "entity: contact"}
            )
        )
        result = client.specs.get(1)
        assert result["spec_yaml"] == "entity: contact"

    def test_ingest(self, mock_api, client):
        mock_api.post("/v1/identity/specs").mock(
            return_value=httpx.Response(
                200, json={"valid": True, "plan_hash": "abc123"}
            )
        )
        result = client.specs.ingest("entity: contact\n", compile=True)
        assert result["valid"] is True
        assert result["plan_hash"] == "abc123"

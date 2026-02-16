"""Pydantic v2 response models for Kanoniv API.

These mirror the server-side API response models.
All models use ``extra="allow"`` so new server-side fields don't break deserialization.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class _Base(BaseModel):
    model_config = ConfigDict(extra="allow")


# -- Canonical Entity (returned by resolve, get, search) ----------------------

class CanonicalEntity(_Base):
    id: UUID
    tenant_id: UUID | None = None
    entity_type: str
    canonical_data: dict[str, Any]
    field_provenance: dict[str, Any] | None = None
    confidence_score: float | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    is_locked: bool | None = None


class LinkedEntityRef(_Base):
    """Lightweight reference to a linked external entity (no raw source data)."""
    id: UUID | None = None
    data_source_id: UUID | None = None
    source_name: str | None = None
    external_id: str | None = None
    entity_type: str | None = None
    ingested_at: datetime | None = None


class IdentityLink(_Base):
    canonical_entity_id: UUID | None = None
    external_entity_id: UUID | None = None
    confidence: float | None = None
    link_type: str | None = None


class CanonicalDetailResponse(_Base):
    """Response from GET /v1/canonical/:id/linked."""
    canonical: CanonicalEntity | None = None
    linked_entities: list[LinkedEntityRef] = []
    links: list[IdentityLink] = []


class EntitySearchResponse(_Base):
    """Response from GET /v1/entities."""
    data: list[dict[str, Any]] = []
    total: int = 0


# -- Sources -------------------------------------------------------------------

class DataSource(_Base):
    id: UUID
    tenant_id: UUID | None = None
    name: str
    source_type: str
    config: dict[str, Any] | None = None
    created_at: datetime | None = None


# -- Rules ---------------------------------------------------------------------

class MatchRule(_Base):
    id: UUID
    tenant_id: UUID | None = None
    version: int | None = None
    name: str
    rule_type: str
    config: dict[str, Any] | None = None
    weight: float | None = None
    is_active: bool | None = None
    created_at: datetime | None = None
    created_by: UUID | None = None


# -- Jobs ----------------------------------------------------------------------

class BatchRun(_Base):
    id: UUID
    tenant_id: UUID | None = None
    status: str
    job_type: str
    stats: dict[str, Any] | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    triggered_by: UUID | None = None
    created_at: datetime | None = None


class RunJobResponse(_Base):
    job_id: UUID
    status: str


# -- Overrides -----------------------------------------------------------------

class ManualOverride(_Base):
    id: UUID
    tenant_id: UUID | None = None
    canonical_entity_id: UUID | None = None
    override_type: str
    override_data: dict[str, Any] | None = None
    reason: str | None = None
    created_by: UUID | None = None
    created_at: datetime | None = None
    superseded_at: datetime | None = None


# -- Audit ---------------------------------------------------------------------

class AuditEvent(_Base):
    id: UUID
    tenant_id: UUID | None = None
    actor_id: UUID | None = None
    actor_type: str | None = None
    action: str | None = None
    resource_type: str | None = None
    resource_id: UUID | None = None
    before_state: dict[str, Any] | None = None
    after_state: dict[str, Any] | None = None
    reason: str | None = None
    timestamp: datetime | None = None


# -- Stats ---------------------------------------------------------------------

class DashboardStats(_Base):
    total_canonical_entities: int = 0
    total_external_entities: int = 0
    pending_reviews: int = 0
    merge_rate: float = 0.0


# -- Identity Specs ------------------------------------------------------------

class IngestSpecResponse(_Base):
    """Response from POST /v1/identity/specs."""
    valid: bool
    warnings: list[str] = []
    errors: list[str] = []
    plan_hash: str | None = None


class SpecSummary(_Base):
    """Response item from GET /v1/identity/specs."""
    identity_version: str
    plan_hash: str
    created_at: str | None = None


class SpecDetail(_Base):
    """Response from GET /v1/identity/specs/:version."""
    identity_version: str
    plan_hash: str
    raw_yaml: str | None = None
    compiled_plan: dict[str, Any] | None = None
    created_at: str | None = None


# -- Resolve -------------------------------------------------------------------

class RealtimeResolveResponse(_Base):
    """Response from POST /v1/resolve/realtime."""
    entity_id: UUID
    canonical_data: dict[str, Any]
    is_new: bool
    matched_source: str | None = None
    confidence: float


class BulkResolveResult(_Base):
    """Single entry in a bulk resolve response."""
    source: str
    id: str
    entity_id: UUID | None = None
    canonical_data: dict[str, Any] | None = None
    found: bool


class BulkResolveResponse(_Base):
    """Response from POST /v1/resolve/bulk."""
    results: list[BulkResolveResult] = []
    resolved: int = 0
    not_found: int = 0


# -- Review Queue --------------------------------------------------------------

class PendingReview(_Base):
    entity_a_id: UUID | None = None
    entity_b_id: UUID | None = None
    entity_a_name: str | None = None
    entity_b_name: str | None = None
    entity_a_email: str | None = None
    entity_b_email: str | None = None
    entity_a_source: str | None = None
    entity_b_source: str | None = None
    confidence: float | None = None
    rule_results: dict[str, Any] | None = None
    created_at: datetime | None = None

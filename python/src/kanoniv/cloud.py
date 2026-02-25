"""Cloud reconciliation -- same inputs as local reconcile(), runs on the Kanoniv API."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .source import Source
from .spec import Spec


_BATCH_SIZE = 500


@dataclass
class CloudReconcileResult:
    """Result of a cloud reconciliation run.

    Contains aggregate summaries from the API.  Raw clusters and golden records
    live in the database and can be fetched lazily via :meth:`to_pandas`.
    """

    job_id: str
    status: str
    canonicals_created: int
    links_created: int
    duration_ms: int
    identity_summary: dict[str, Any]
    run_health: dict[str, Any]
    _client: Any = field(repr=False)
    _owns_client: bool = field(default=False, repr=False)

    # -- Computed properties from identity_summary ----------------------------

    @property
    def cluster_count(self) -> int:
        """Number of canonical identities produced."""
        output = self.identity_summary.get("output", {})
        return int(output.get("canonical_identities", 0))

    @property
    def merge_rate(self) -> float:
        """Fraction of input entities that were merged (0.0 – 1.0)."""
        output = self.identity_summary.get("output", {})
        return float(output.get("merge_rate", 0.0))

    @property
    def match_quality(self) -> dict[str, Any]:
        """Match quality breakdown (accepted, rejected, etc.)."""
        return self.identity_summary.get("match_quality", {})

    @property
    def health_status(self) -> str:
        """Overall run health: ``healthy``, ``degraded``, or ``unhealthy``."""
        return self.run_health.get("status", "unknown")

    @property
    def health_flags(self) -> list[str]:
        """Health flag labels from identity_summary."""
        return self.identity_summary.get("health_flags", [])

    # -- Lifecycle ------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying client if this result owns it."""
        if self._owns_client and self._client is not None:
            self._client.close()

    def __enter__(self) -> CloudReconcileResult:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # -- Methods --------------------------------------------------------------

    def to_pandas(self) -> Any:
        """Fetch canonical entities from the API and return a pandas DataFrame.

        Requires *pandas* to be installed (``pip install pandas``).
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "CloudReconcileResult.to_pandas() requires pandas: pip install pandas"
            ) from None

        entities: list[dict[str, Any]] = []
        offset = 0
        limit = 100
        while True:
            page = self._client.entities.search(limit=limit, offset=offset)
            items = page.get("items", page.get("data", []))
            if not items:
                break
            entities.extend(items)
            if len(items) < limit:
                break
            offset += limit
        return pd.DataFrame(entities)

    def summary(self) -> str:
        """Human-readable summary of the cloud reconciliation run."""
        inp = self.identity_summary.get("input", {})
        output = self.identity_summary.get("output", {})
        mq = self.match_quality
        lines = [
            f"Cloud Reconciliation - job {self.job_id}",
            f"  Status:          {self.status}",
            f"  Duration:        {self.duration_ms}ms",
            f"  Health:          {self.health_status}",
            f"  Input entities:  {inp.get('total_entities', '?')}",
            f"  Canonicals:      {self.canonicals_created}",
            f"  Links:           {self.links_created}",
            f"  Merge rate:      {self.merge_rate:.1%}",
        ]
        if mq:
            lines.append(f"  Match quality:   {mq}")
        flags = self.health_flags
        if flags:
            lines.append(f"  Health flags:    {', '.join(flags)}")
        return "\n".join(lines)


def _build_client(
    client: Any | None,
    api_key: str | None,
    base_url: str,
) -> tuple[Any, bool]:
    """Return (client, owns_client) -- creates one if needed."""
    if client is not None:
        return client, False
    if api_key is None:
        raise ValueError(
            "Provide a `client` or `api_key` to use cloud reconciliation."
        )
    from .client import Client

    return Client(api_key=api_key, base_url=base_url), True


def _job_to_result(
    job: dict[str, Any],
    job_id: str,
    client: Any,
    owns_client: bool,
) -> CloudReconcileResult:
    """Build a CloudReconcileResult from a completed job response."""
    result_data = job.get("result", job)
    stats = result_data.get("stats", result_data)
    return CloudReconcileResult(
        job_id=job_id,
        status="completed",
        canonicals_created=int(stats.get("canonicals_created", 0)),
        links_created=int(stats.get("links_created", 0)),
        duration_ms=int(stats.get("duration_ms", 0)),
        identity_summary=stats.get("identity_summary", {}),
        run_health=stats.get("run_health", {}),
        _client=client,
        _owns_client=owns_client,
    )


def _poll_job(
    client: Any,
    job_id: str,
    poll_interval: float,
    timeout: float,
) -> dict[str, Any]:
    """Poll a job until it reaches a terminal state or times out."""
    deadline = time.monotonic() + timeout
    _terminal = ("completed", "failed", "cancelled")
    job: dict[str, Any] = {}
    status = "pending"

    while status not in _terminal:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Cloud reconciliation job {job_id} did not complete within "
                f"{timeout}s.  Check status with client.jobs.get('{job_id}')."
            )
        time.sleep(poll_interval)
        job = client.jobs.get(job_id)
        status = job.get("status", status)

    if status == "failed":
        error = job.get("error", "unknown error")
        raise RuntimeError(
            f"Cloud reconciliation job {job_id} failed: {error}"
        )
    if status == "cancelled":
        raise RuntimeError(
            f"Cloud reconciliation job {job_id} was cancelled."
        )
    return job


def fetch_result(
    job_id: str,
    *,
    client: Any | None = None,
    api_key: str | None = None,
    base_url: str = "https://api.kanoniv.com",
    poll_interval: float = 2.0,
    timeout: float = 300.0,
) -> CloudReconcileResult:
    """Fetch the result of an existing reconciliation job.

    Use this to resume a timed-out job or retrieve results from a previous run
    without re-ingesting data.

    Args:
        job_id: The job ID returned by a previous reconcile() call.
        client: An existing :class:`kanoniv.Client` instance.
        api_key: API key (used only when *client* is not provided).
        base_url: API base URL (used only when *client* is not provided).
        poll_interval: Seconds between job-status polls (if still running).
        timeout: Maximum seconds to wait for the job to complete.

    Returns:
        A :class:`CloudReconcileResult` with aggregate summaries.
    """
    client, owns_client = _build_client(client, api_key, base_url)

    # Check current status -- may already be done
    job = client.jobs.get(job_id)
    status = job.get("status", "pending")

    if status in ("completed",):
        return _job_to_result(job, job_id, client, owns_client)

    if status in ("failed", "cancelled"):
        msg = job.get("error", status)
        raise RuntimeError(f"Job {job_id} {status}: {msg}")

    # Still running -- poll
    job = _poll_job(client, job_id, poll_interval, timeout)
    return _job_to_result(job, job_id, client, owns_client)


def reconcile(
    sources: list[Source],
    spec: Spec,
    *,
    client: Any | None = None,
    api_key: str | None = None,
    base_url: str = "https://api.kanoniv.com",
    poll_interval: float = 2.0,
    timeout: float = 300.0,
    resume_job_id: str | None = None,
) -> CloudReconcileResult:
    """Run identity resolution on the Kanoniv cloud API.

    Accepts the same ``sources`` and ``spec`` as :func:`kanoniv.reconcile`,
    but executes remotely.  The function blocks until the job finishes or
    *timeout* seconds elapse.

    **Idempotent ingest**: The API computes a content hash of each record's
    normalized fields. Records whose hash matches what is already stored are
    skipped (zero DB writes). The ingest response reports new/updated/unchanged
    counts.

    Args:
        sources: Data sources to reconcile.
        spec: Identity spec defining rules and thresholds.
        client: An existing :class:`kanoniv.Client` instance.  If *None*,
            one is created from *api_key* / *base_url*.
        api_key: API key (used only when *client* is not provided).
        base_url: API base URL (used only when *client* is not provided).
        poll_interval: Seconds between job-status polls.
        timeout: Maximum seconds to wait for the job to complete.
        resume_job_id: If provided, skip ingest and poll this job for results.

    Returns:
        A :class:`CloudReconcileResult` with aggregate summaries and lazy
        access to canonical entities.

    Raises:
        ValueError: If neither *client* nor *api_key* is provided.
        TimeoutError: If the job does not complete within *timeout* seconds.
        RuntimeError: If the job fails on the server.
    """
    # -- 1. Build / reuse client ----------------------------------------------
    client, owns_client = _build_client(client, api_key, base_url)

    # -- 1.5. Resume a specific job (skip ingest entirely) --------------------
    if resume_job_id is not None:
        job = client.jobs.get(resume_job_id)
        status = job.get("status", "pending")
        if status == "completed":
            return _job_to_result(job, resume_job_id, client, owns_client)
        if status in ("failed", "cancelled"):
            msg = job.get("error", status)
            raise RuntimeError(f"Job {resume_job_id} {status}: {msg}")
        # Still running -- poll it
        job = _poll_job(client, resume_job_id, poll_interval, timeout)
        return _job_to_result(job, resume_job_id, client, owns_client)

    # -- 1.6. Fast path: Arrow + DuckDB + Parquet (if dataplane deps available)
    try:
        from .cloud_io import _extract_connection_string

        conn_str = _extract_connection_string(sources)
        if conn_str:
            return _reconcile_arrow(
                sources, spec, client, conn_str, owns_client,
                poll_interval=poll_interval, timeout=timeout,
            )
    except ImportError:
        pass  # dataplane extras not installed -- fall through to JSON path

    # -- 2. Upload spec -------------------------------------------------------
    client.specs.ingest(spec.raw, compile=True)

    # -- 3. Derive entity_type and attribute mappings (same as local) ----------
    entity = spec.entity
    if isinstance(entity, dict):
        entity_type = entity.get("name", "entity")
    else:
        entity_type = entity or "entity"

    attr_maps: dict[str, dict[str, str]] = {}
    for spec_src in spec.sources:
        src_name = spec_src.get("name", "")
        attrs = spec_src.get("attributes", {})
        if attrs and src_name:
            attr_maps[src_name] = {v: k for k, v in attrs.items()}

    # -- 3.5. Ensure data sources exist on the API ----------------------------
    for source in sources:
        try:
            client.sources.create(name=source.name, source_type="sdk", config={})
        except Exception:
            pass  # source may already exist (409 Conflict)

    # -- 4. Ingest entities per source ----------------------------------------
    # Idempotency is now handled at the record level by the API (content hashing).
    # The ingest response includes new/updated/unchanged counts.
    for source in sources:
        entities = source.to_entities(entity_type)
        reverse_map = attr_maps.get(source.name, {})
        if reverse_map:
            for ent in entities:
                raw_data = ent["data"]
                mapped: dict[str, str] = {}
                for col, val in raw_data.items():
                    canonical = reverse_map.get(col, col)
                    mapped[canonical] = val
                ent["data"] = mapped

        # Batch upload via /v1/ingest/batch
        for i in range(0, len(entities), _BATCH_SIZE):
            batch = [
                {"external_id": e["external_id"], "data": e["data"]}
                for e in entities[i : i + _BATCH_SIZE]
            ]
            client.ingest(source.name, batch, entity_type=entity_type)

    # -- 5. Submit reconciliation job -----------------------------------------
    job_resp = client.jobs.run("reconciliation")
    job_id = job_resp.get("id") or job_resp.get("job_id", "")

    # -- 6. Poll until completed / failed / timeout ---------------------------
    job = _poll_job(client, job_id, poll_interval, timeout)

    # -- 7. Extract stats from completed job ----------------------------------
    return _job_to_result(job, job_id, client, owns_client)


def _reconcile_arrow(
    sources: list[Source],
    spec: Spec,
    client: Any,
    conn_str: str,
    owns_client: bool,
    *,
    poll_interval: float = 2.0,
    timeout: float = 300.0,
) -> CloudReconcileResult:
    """Arrow fast path: DuckDB staging -> Parquet upload -> API reconcile.

    Used automatically when ``dataplane`` extras are installed and all
    sources are warehouse-backed.
    """
    import tempfile

    import pyarrow.compute as pc

    from .staging import export_parquet, stage_sources

    # 1. Stage all sources via DuckDB -> unified Arrow table
    staged = stage_sources(sources, spec, conn_str)

    # 2. Upload spec
    client.specs.ingest(spec.raw, compile=True)

    # 3. Derive entity_type
    entity = spec.entity
    if isinstance(entity, dict):
        entity_type = entity.get("name", "entity")
    else:
        entity_type = entity or "entity"

    # 4. Ensure data sources exist on the API
    for source in sources:
        try:
            client.sources.create(name=source.name, source_type="sdk", config={})
        except Exception:
            pass  # source may already exist

    # 5. Export per-source Parquet and upload
    for source in sources:
        mask = pc.equal(staged.column("source_name"), source.name)
        source_tbl = staged.filter(mask)
        if source_tbl.num_rows == 0:
            continue
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            export_parquet(source_tbl, tmp_path)
            client.ingest_parquet(source.name, tmp_path, entity_type=entity_type)
        finally:
            import os
            os.unlink(tmp_path)

    # 6. Submit + poll job
    job_resp = client.jobs.run("reconciliation")
    job_id = job_resp.get("id") or job_resp.get("job_id", "")
    job = _poll_job(client, job_id, poll_interval, timeout)

    # 7. Build result
    return _job_to_result(job, job_id, client, owns_client)

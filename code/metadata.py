@ -0,0 +1,446 @@
#!/usr/bin/env python
"""Emit AIND metadata sidecars for the derived V1DD connectivity data asset.

Writes three files to the output directory:

* ``subject.json``          -- inherited verbatim from the input data asset
* ``data_description.json`` -- derived from the input asset's, re-stamped as ``derived``
* ``processing.json``       -- built from this run, one DataProcess per ETL notebook

Run after the ETL notebooks::

    python -u /code/metadata.py --start-time "$RUN_START"

Written against aind-data-schema 2.8.1.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from aind_data_schema.base import GenericModel
from aind_data_schema.components.identifiers import Code, DataAsset, Software
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.processing import DataProcess, Processing, ProcessStage
from aind_data_schema.core.subject import Subject
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.process_names import ProcessName

# ---------------------------------------------------------------------------
# CONFIG -- review these before a capture run
# ---------------------------------------------------------------------------

#: Input data asset the derived product is built from. Supplies subject.json and the
#: institutional fields of data_description.json.
INPUT_ASSET = Path("/data/v1dd_1196")

#: CodeOcean captures /results as the data asset; metadata belongs at its root.
OUTPUT_DIR = Path("/results")

#: Used only if the CCM settings cannot be read. The real name is resolved at runtime
#: from the output_root the ETL notebooks actually wrote to, so that the metadata always
#: describes where the data went even if CCC_OUTPUT_ROOT repoints it.
FALLBACK_ASSET_NAME = "409828_V1DD_CCM_materialization_1196"

#: CAVE materialization this release is built from; recorded as the code version.
RELEASE = "1196"

#: Repository this pipeline runs from, recorded as provenance on every DataProcess.
CODE_URL = "https://github.com/reneyagmur/SWDB_2026_Connectomics_v1dd_ccm_reprun"

#: Core analysis package the ETL is written against. Its exact commit is resolved at
#: runtime from the installed distribution (PEP 610), because the environment pins a
#: *branch* -- and branches move, so the branch name alone is not reproducible.
CORE_PACKAGE = "connects-common-connectivity"

#: Written by etl_v1dd_01; supplies the CAVE materialization version and timestamp.
CAVE_PROVENANCE_FILE = "cave_provenance.json"

#: Who ran the pipeline. Empty list means "fall back to the inherited investigators".
EXPERIMENTERS: list[str] = []

#: Modalities of the derived asset.
MODALITIES = [Modality.EM]

#: ProcessName has no connectomics-specific term; "Analysis" is the closest fit.
#: Alternatives in the 2.8.1 vocabulary: "Other", "Pipeline".
PROCESS_TYPE = ProcessName.ANALYSIS
PROCESS_STAGE = ProcessStage.ANALYSIS

#: One DataProcess per ETL notebook, in execution order.
ETL_STEPS = [
    (
        "etl_v1dd_01_cave_dataset_celltypes",
        "Query CAVE for soma / cell-type / proofreading annotations and write the "
        "DataSet, DataItem, cell-feature and cluster tables.",
    ),
    (
        "etl_v1dd_02_synapses",
        "Build the synapse table, synapse feature matrix and connectivity matrix.",
    ),
    (
        "etl_v1dd_03_somafeatures",
        "Attach soma morphology features as an additional cell-feature set.",
    ),
]

log = logging.getLogger("metadata")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _read_json(path: Path) -> dict:
    """Load a JSON sidecar, raising a message that names the file on failure."""
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} not found. The input asset is expected to carry it; check that "
            f"{path.parent} is mounted (see .codeocean/datasets.json)."
        )
    try:
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}") from exc


def _git_commit(repo: Path) -> str | None:
    """Best-effort commit hash. Returns None outside a git checkout (e.g. a capsule)."""
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=10, check=True,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def resolve_asset_name(explicit: str | None = None) -> str:
    """Name of the derived asset, taken from where the ETL actually wrote.

    ``code/run`` exports ``CCC_OUTPUT_ROOT`` with a creation-datetime suffix, and
    ``ccc_config.yaml`` holds a fallback. Reading the *resolved* setting rather than the
    environment variable means the metadata follows the data even if the override is
    ignored or the config is edited by hand.
    """
    if explicit:
        return explicit
    try:
        from connects_common_connectivity.config import get_settings

        name = Path(str(get_settings().output_root)).name
        if name:
            return name
        log.warning("output_root resolved to a rootless path; using fallback name")
    except Exception as exc:
        log.warning("could not read output_root from the CCM settings (%s); "
                    "falling back to %s", exc, FALLBACK_ASSET_NAME)
    return FALLBACK_ASSET_NAME


def _package_provenance(name: str) -> dict:
    """Resolve an installed package to a reproducible identifier.

    For a VCS install, pip records PEP 610 ``direct_url.json`` in the dist-info with the
    repository URL, the requested revision (branch) and the **resolved commit**. The
    environment pins ``connects-common-connectivity`` to a branch, so the commit is the
    only identifier that stays meaningful over time -- record it, not the branch name.
    """
    info: dict = {"name": name}
    try:
        from importlib.metadata import distribution

        dist = distribution(name)
        info["version"] = dist.version
        raw = dist.read_text("direct_url.json")
        if raw:
            direct = json.loads(raw)
            info["url"] = direct.get("url")
            vcs = direct.get("vcs_info") or {}
            if vcs:
                info["vcs"] = vcs.get("vcs")
                info["requested_revision"] = vcs.get("requested_revision")
                info["commit_id"] = vcs.get("commit_id")
    except Exception as exc:
        log.warning("could not resolve provenance for %s: %s", name, exc)
    return info


def _cave_provenance(results_dir: Path) -> dict:
    """Read the CAVE sidecar written by etl_v1dd_01."""
    path = results_dir / CAVE_PROVENANCE_FILE
    if not path.is_file():
        log.warning(
            "%s not found; processing.json will not record the CAVE materialization. "
            "It is written by etl_v1dd_01 -- did that notebook run?", path
        )
        return {}
    try:
        return _read_json(path)
    except ValueError as exc:
        log.warning("%s unreadable: %s", path, exc)
        return {}


def _notebook_end_time(results_dir: Path, stem: str) -> datetime | None:
    """When nbconvert finished a notebook, from the executed copy's mtime."""
    path = results_dir / f"{stem}.ipynb"
    if not path.is_file():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


# ---------------------------------------------------------------------------
# builders
# ---------------------------------------------------------------------------


def build_subject(input_asset: Path, strict: bool) -> Subject | None:
    """Inherit subject.json from the input asset.

    Returns a validated model where possible -- writing it back normalises the file to
    the pinned schema version. If the upstream file predates this schema it will not
    validate; unless ``strict``, return None so the caller copies it verbatim, since
    preserving real provenance beats emitting nothing.
    """
    raw = _read_json(input_asset / "subject.json")
    try:
        return Subject.model_validate(raw)
    except Exception as exc:
        msg = (
            f"upstream subject.json does not validate against the pinned "
            f"aind-data-schema: {type(exc).__name__}: {exc}"
        )
        if strict:
            raise ValueError(msg) from exc
        log.warning("%s", msg)
        log.warning("copying subject.json verbatim instead (schema version may differ)")
        return None


def build_data_description(
    input_asset: Path, creation_time: datetime, strict: bool, asset_name: str
) -> DataDescription:
    """Derive the data description from the input asset's.

    Institutional fields are inherited; only the fields that genuinely change for a
    derived product are overridden. aind-data-schema 2.8.1 has no
    ``DerivedDataDescription`` class, so this is a plain DataDescription carrying
    ``data_level="derived"`` and a ``source_data`` back-reference.
    """
    raw = _read_json(input_asset / "data_description.json")

    inherited_keys = [
        "institution",
        "funding_source",
        "investigators",
        "project_name",
        "subject_id",
        "license",
        "group",
        "restrictions",
    ]
    missing = [
        k for k in ("institution", "funding_source", "investigators", "project_name")
        if raw.get(k) in (None, [], "")
    ]
    if missing:
        raise ValueError(
            f"upstream data_description.json is missing required field(s) {missing}. "
            "These describe the institution and people behind the data and are not "
            "safe to invent -- supply them explicitly or fix the input asset."
        )

    kwargs = {k: raw[k] for k in inherited_keys if raw.get(k) is not None}
    kwargs.update(
        name=asset_name,
        creation_time=creation_time,
        data_level=DataLevel.DERIVED,
        modalities=MODALITIES,
        source_data=[raw.get("name") or input_asset.name],
        data_summary=(
            "Common Connectivity Matrix representation of the V1DD EM connectome "
            "(materialization 1196): cell types, proofreading cohorts, synapses, "
            "connectivity and soma morphology features."
        ),
    )

    try:
        return DataDescription(**kwargs)
    except Exception as exc:
        raise ValueError(
            "could not build data_description.json from the inherited fields "
            f"({type(exc).__name__}: {exc}). The upstream file may use an older "
            "schema whose field shapes differ from the pinned version."
        ) from exc


def build_processing(
    start_time: datetime, results_dir: Path, experimenters: list[str], asset_name: str
) -> Processing:
    """One DataProcess per ETL notebook, chained end-to-start."""
    ccm = _package_provenance(CORE_PACKAGE)
    cave = _cave_provenance(results_dir)

    # Structured, queryable provenance. GenericModel allows extra keys.
    parameters = {"core_package": ccm}
    if cave:
        parameters["cave"] = cave

    # Semantic declaration of what the code read. CAVE is queried live, so it is an
    # input data asset even though it never lands on disk as one.
    input_data = [DataAsset(name=INPUT_ASSET.name, url=f"file://{INPUT_ASSET}")]
    if cave:
        input_data.append(
            DataAsset(
                name=(
                    f"{cave.get('datastack', 'v1dd')} CAVE materialization "
                    f"{cave.get('materialization_version')}"
                ),
                url=f"{cave.get('server', '')}/{cave.get('datastack', '')}".rstrip("/"),
            )
        )

    code = Code(
        url=CODE_URL,
        name="SWDB 2026 Connectomics V1DD CCM ETL",
        version=RELEASE,
        commit_hash=_git_commit(Path(__file__).resolve().parent),
        language="Python",
        language_version=f"{sys.version_info.major}.{sys.version_info.minor}",
        # Software carries only name/version -- the git URL and branch have no slot of
        # their own, so they travel in `parameters` alongside the resolved commit.
        core_dependency=Software(
            name=CORE_PACKAGE,
            version=ccm.get("commit_id") or ccm.get("version"),
        ),
        input_data=input_data,
        parameters=GenericModel.model_validate(parameters),
    )

    processes, cursor = [], start_time
    for stem, notes in ETL_STEPS:
        end = _notebook_end_time(results_dir, stem)
        if end is None or end < cursor:
            # nbconvert copy absent (or clock skew) -- leave end open rather than lie
            log.warning("no usable end time for %s; leaving end_date_time unset", stem)
            end = None
        processes.append(
            DataProcess(
                process_type=PROCESS_TYPE,
                name=stem,
                stage=PROCESS_STAGE,
                code=code,
                experimenters=experimenters,
                start_date_time=cursor,
                end_date_time=end,
                # AssetPath must be relative to the metadata directory, not absolute
                output_path=asset_name,
                notes=notes,
            )
        )
        if end is not None:
            cursor = end

    return Processing(
        data_processes=processes,
        notes=(
            "Derived from live CAVE queries (datastack v1dd, materialization 1196) "
            f"and the {INPUT_ASSET.name} data asset."
        ),
    )


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-asset", type=Path, default=INPUT_ASSET,
                        help="asset supplying subject / institutional metadata")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR,
                        help="where the three sidecars are written")
    parser.add_argument("--start-time", default=None,
                        help="ISO-8601 pipeline start; defaults to now (UTC)")
    parser.add_argument("--asset-name", default=None,
                        help="overrides the name resolved from the CCM output_root")
    parser.add_argument("--experimenters", nargs="*", default=None,
                        help="overrides EXPERIMENTERS; falls back to inherited investigators")
    parser.add_argument("--strict", action="store_true",
                        help="fail instead of copying a non-validating subject.json verbatim")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    if args.start_time:
        start_time = datetime.fromisoformat(args.start_time)
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
    else:
        start_time = datetime.now(timezone.utc)
    creation_time = datetime.now(timezone.utc)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    asset_name = resolve_asset_name(args.asset_name)
    log.info("asset name: %s", asset_name)

    # --- subject.json -------------------------------------------------------
    subject = build_subject(args.input_asset, args.strict)
    if subject is not None:
        subject.write_standard_file(output_directory=args.output_dir)
    else:
        shutil.copyfile(
            args.input_asset / "subject.json", args.output_dir / "subject.json"
        )
    log.info("wrote %s", args.output_dir / "subject.json")

    # --- data_description.json ---------------------------------------------
    data_description = build_data_description(
        args.input_asset, creation_time, args.strict, asset_name
    )
    data_description.write_standard_file(output_directory=args.output_dir)
    log.info("wrote %s", args.output_dir / "data_description.json")

    # --- processing.json ----------------------------------------------------
    experimenters = args.experimenters if args.experimenters is not None else EXPERIMENTERS
    if not experimenters:
        experimenters = [
            name
            for p in (data_description.investigators or [])
            if (name := getattr(p, "name", None))
        ]
        log.info("experimenters not set; inherited from investigators: %s", experimenters)
    if not experimenters:
        raise ValueError(
            "no experimenters resolved. Set EXPERIMENTERS in metadata.py or pass "
            "--experimenters."
        )

    processing = build_processing(start_time, args.output_dir, experimenters, asset_name)
    processing.write_standard_file(output_directory=args.output_dir)
    log.info("wrote %s", args.output_dir / "processing.json")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""
Single-op CWL runner with asset-aware metadata.

Requirements:
  pip install dagster dagster-webserver cwltool pydantic

Run:
  dagster dev
and load the repository in Dagit. Configure the run via the provided config example at the bottom.
"""

from __future__ import annotations
import hashlib
import json
import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union

import dagster as dg


# Utilities


def _sha256_file(path: Union[str, Path]) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Union[str, Path]) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _ensure_dir(p: Union[str, Path]) -> Path:
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


# Resource: CWL runner configuration


@dataclass
class CWLRunnerConfig:
    """Configuration for how we call the CWL runner."""

    executable: str = "cwltool"
    default_args: Tuple[str, ...] = (
        "--enable-dev",
        "--no-match-user",
    )
    env: Optional[Mapping[str, str]] = None
    # If set, we will append '--provenance <dir>' to collect CWLProv bundles
    provenance_dir: Optional[str] = None


class CWLRunnerResource(dg.ConfigurableResource):
    """Dagster resource that holds the CWL runner configuration."""

    executable: str = "cwltool"
    default_args: Optional[Iterable[str]] = None
    env: Optional[Mapping[str, str]] = None
    provenance_dir: Optional[str] = None

    def to_runtime(self) -> CWLRunnerConfig:
        return CWLRunnerConfig(
            executable=self.executable,
            default_args=(
                tuple(self.default_args)
                if self.default_args
                else ("--enable-dev", "--no-match-user")
            ),
            env=self.env,
            provenance_dir=self.provenance_dir,
        )


# Optional IOManager: map named CWL outputs to stable locations


class CWLPathIOManager(dg.IOManager):
    """
    A minimal IOManager that writes a text file containing the resolved path of
    each CWL 'File' output. Downstream ops can read the path without guessing.

    This is just proof of concept; Replace with an S3 or object storage manager if needed.
    """

    def __init__(self, base_dir: str = "outputs_registry"):
        self.base_dir = base_dir

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        # obj is expected to be a filesystem path (str or Path) or a simple value
        root = _ensure_dir(self.base_dir)
        ns = "_".join(context.get_identifier())
        out_path = root / f"{ns}.txt"
        out_path.write_text(str(obj))
        context.log.info(f"[IOManager] Wrote path to {out_path}")

    def load_input(self, context: dg.InputContext) -> Any:
        root = Path(self.base_dir)
        ns = "_".join(context.get_identifier())
        in_path = root / f"{ns}.txt"
        if not in_path.exists():
            raise FileNotFoundError(f"Expected registry file not found: {in_path}")
        resolved = in_path.read_text().strip()
        context.log.info(f"[IOManager] Loaded path {resolved} from {in_path}")
        return resolved


@dg.io_manager
def cwl_path_io_manager(_) -> CWLPathIOManager:
    return CWLPathIOManager()


# Schema for op config


RunCWLConfig = dg.define_structured_config_cls(
    "RunCWLConfig",
    {
        "cwl_path": (str, ...),  # Path or URL to the CWL workflow
        "job_path": (str, ...),  # Path to the CWL job object (YAML or JSON)
        "outdir": (str, "outputs"),  # Directory for workflow outputs
        "cachedir": (
            str,
            ".cwl-cache",
        ),  # Cache directory to enable content-addressed reuse
        "extra_args": (
            Iterable[str],
            (),
        ),  # Extra CLI flags for cwltool or chosen runner
    },
)


# The op: run the whole CWL and emit asset-aware metadata


@dg.op(
    config_schema=RunCWLConfig,
    required_resource_keys={"cwl_runner"},
    out=dg.Out(dict, description="Resolved CWL outputs object (parsed JSON)."),
)
def run_cwl_op(context: dg.OpExecutionContext) -> dict:
    cfg = RunCWLConfig.from_config(context.op_config)
    runner = context.resources.cwl_runner.to_runtime()

    outdir = _ensure_dir(cfg.outdir).resolve()
    cachedir = _ensure_dir(cfg.cachedir).resolve()

    # Basic provenance on inputs
    cwl_hash = _sha256_file(cfg.cwl_path) if os.path.exists(cfg.cwl_path) else ""
    job_hash = _sha256_file(cfg.job_path) if os.path.exists(cfg.job_path) else ""

    cmd = [
        runner.executable,
        "--outdir",
        str(outdir),
        "--cachedir",
        str(cachedir),
        *runner.default_args,
        *cfg.extra_args,
    ]

    # Collect a CWLProv bundle if requested
    prov_dir = None
    if runner.provenance_dir:
        prov_dir = _ensure_dir(runner.provenance_dir).resolve()
        cmd.extend(["--provenance", str(prov_dir)])

    cmd.extend([cfg.cwl_path, cfg.job_path])

    # Log the exact command for reproducibility
    cmd_str = " ".join(shlex.quote(x) for x in cmd)
    context.log.info(f"Running CWL: {cmd_str}")

    # Invoke the runner
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, **(runner.env or {})},
    )

    # Attach raw logs for debugging even on success
    context.log.debug(proc.stdout[:10000])
    if proc.returncode != 0:
        context.log.error(proc.stderr[:10000])
        raise RuntimeError(f"cwltool failed with code {proc.returncode}")

    # cwltool prints a JSON object with final resolved outputs to stdout
    try:
        outputs = json.loads(proc.stdout)
    except json.JSONDecodeError:
        # Some runners print logs to stdout and write outputs to a file named 'cwl.output.json' in outdir
        alt = outdir / "cwl.output.json"
        if alt.exists():
            outputs = _read_json(alt)
        else:
            raise

    # Emit an overall materialization for the run itself
    run_meta = {
        "cwl_path": cfg.cwl_path,
        "cwl_sha256": cwl_hash,
        "job_path": cfg.job_path,
        "job_sha256": job_hash,
        "outdir": str(outdir),
        "cachedir": str(cachedir),
        "runner_executable": runner.executable,
        "runner_args": " ".join(runner.default_args),
        "invocation": cmd_str,
    }
    if prov_dir:
        run_meta["provenance_dir"] = str(prov_dir)

    context.log_event(
        dg.AssetMaterialization(
            asset_key=dg.AssetKey(["cwl_run", Path(cfg.cwl_path).stem]),
            description="CWL run materialization",
            metadata=run_meta,
        )
    )

    # Emit materializations for each output and build a simple dict of resolved paths or values
    resolved: Dict[str, Any] = {}
    for name, val in outputs.items():
        # CWL 'File' object
        if isinstance(val, dict) and val.get("class") == "File":
            path = val.get("path") or val.get("location") or val.get("basename")
            sha256 = None
            try:
                if path and os.path.exists(path):
                    sha256 = _sha256_file(path)
            except Exception:
                sha256 = None

            meta = {
                "class": "File",
                "path": path,
                "checksum": val.get("checksum"),
                "sha256": sha256,
                "size": val.get("size"),
                "format": val.get("format"),
                "secondaryFiles": json.dumps(val.get("secondaryFiles", [])),
            }
            context.log_event(
                dg.AssetMaterialization(
                    asset_key=dg.AssetKey(["cwl_output", name]),
                    description=f"CWL output file for '{name}'",
                    metadata={k: v for k, v in meta.items() if v is not None},
                )
            )
            resolved[name] = path

        # CWL 'Directory' object
        elif isinstance(val, dict) and val.get("class") == "Directory":
            path = val.get("path") or val.get("location") or val.get("basename")
            meta = {
                "class": "Directory",
                "path": path,
                "listing_len": (
                    len(val.get("listing", []))
                    if isinstance(val.get("listing"), list)
                    else None
                ),
            }
            context.log_event(
                dg.AssetMaterialization(
                    asset_key=dg.AssetKey(["cwl_output", name]),
                    description=f"CWL output directory for '{name}'",
                    metadata={k: v for k, v in meta.items() if v is not None},
                )
            )
            resolved[name] = path

        else:
            # Primitive values or arrays
            context.log_event(
                dg.AssetMaterialization(
                    asset_key=dg.AssetKey(["cwl_output", name]),
                    description=f"CWL output value for '{name}'",
                    metadata={"value_preview": str(val)[:500]},
                )
            )
            resolved[name] = val

    return resolved


# Job and repository


@dg.job(
    resource_defs={
        "cwl_runner": CWLRunnerResource(),  # default runner
        "io_manager": cwl_path_io_manager,  # optional output path registry
    }
)
def cwl_job():
    run_cwl_op()


# Expose a repository for dagster dev
defs = dg.Definitions(
    jobs=[cwl_job],
    resources={"cwl_runner": CWLRunnerResource(), "io_manager": cwl_path_io_manager},
)

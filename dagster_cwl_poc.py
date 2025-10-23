"""
Single-op CWL runner with asset-aware metadata (Pattern A).

Requirements:
  pip install dagster dagster-webserver cwltool

Run:
  dagster dev -m dagster_cwl_poc
  (or add [tool.dagster] module_name="dagster_cwl_poc" to pyproject.toml and run `dagster dev`)
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


# --------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------


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


# --------------------------------------------------------------------
# Resource: CWL runner configuration (classic @resource for compatibility)
# --------------------------------------------------------------------


@dataclass
class CWLRunnerConfig:
    executable: str = "cwltool"
    default_args: Tuple[str, ...] = ("--enable-dev", "--no-match-user")
    env: Optional[Mapping[str, str]] = None
    provenance_dir: Optional[str] = None  # if set, we pass --provenance <dir>


@dg.resource(
    config_schema={
        "executable": dg.Field(str, default_value="cwltool"),
        "default_args": dg.Field(
            [str], default_value=["--enable-dev", "--no-match-user"]
        ),
        "env": dg.Field(dict, default_value={}),
        "provenance_dir": dg.Field(str, is_required=False),
    }
)
def cwl_runner(init_context) -> CWLRunnerConfig:
    cfg = init_context.resource_config
    return CWLRunnerConfig(
        executable=cfg["executable"],
        default_args=tuple(cfg.get("default_args", [])),
        env=cfg.get("env", {}),
        provenance_dir=cfg.get("provenance_dir"),
    )


# --------------------------------------------------------------------
# IOManagers
# --------------------------------------------------------------------


class CWLPathIOManager(dg.IOManager):
    """
    Minimal IOManager that writes a text file containing the resolved path of
    each CWL 'File' output. Downstream ops can read the path deterministically.
    """

    def __init__(self, base_dir: str = "outputs_registry"):
        self.base_dir = base_dir

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
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
def cwl_path_io_manager(_):
    return CWLPathIOManager()


@dg.io_manager(name="path_registry")
def path_registry_io_manager(_):
    return CWLPathIOManager(base_dir="outputs_registry")


# --------------------------------------------------------------------
# The op: run the whole CWL and emit asset-aware metadata
# --------------------------------------------------------------------


@dg.op(
    config_schema={
        "cwl_path": dg.Field(str, description="Path or URL to the CWL workflow"),
        "job_path": dg.Field(
            str, description="Path to the CWL input object (YAML/JSON)"
        ),
        "outdir": dg.Field(
            str, default_value="outputs", description="Directory for workflow outputs"
        ),
        "cachedir": dg.Field(
            str, default_value=".cwl-cache", description="CWL content cache directory"
        ),
        "extra_args": dg.Field(
            [str], default_value=[], description="Extra args for the CWL runner"
        ),
    },
    required_resource_keys={"cwl_runner"},
    out=dg.Out(dict, description="Resolved CWL outputs object (parsed JSON)."),
)
def run_cwl_op(context) -> dict:
    cfg = context.op_config
    cwl_path: str = cfg["cwl_path"]
    job_path: str = cfg["job_path"]
    outdir: Path = _ensure_dir(cfg.get("outdir", "outputs")).resolve()
    cachedir: Path = _ensure_dir(cfg.get("cachedir", ".cwl-cache")).resolve()
    extra_args = list(cfg.get("extra_args", []))

    runner: CWLRunnerConfig = context.resources.cwl_runner

    # Basic provenance on inputs
    cwl_hash = _sha256_file(cwl_path) if os.path.exists(cwl_path) else ""
    job_hash = _sha256_file(job_path) if os.path.exists(job_path) else ""

    cmd = [
        runner.executable,
        "--outdir",
        str(outdir),
        "--cachedir",
        str(cachedir),
        *runner.default_args,
        *extra_args,
    ]
    prov_dir = None
    if runner.provenance_dir:
        prov_dir = _ensure_dir(runner.provenance_dir).resolve()
        cmd.extend(["--provenance", str(prov_dir)])
    cmd.extend([cwl_path, job_path])

    cmd_str = " ".join(shlex.quote(x) for x in cmd)
    context.log.info(f"Running CWL: {cmd_str}")

    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, **(runner.env or {})},
    )

    context.log.debug(proc.stdout[:10000])
    if proc.returncode != 0:
        context.log.error(proc.stderr[:10000])
        raise RuntimeError(f"cwltool failed with code {proc.returncode}")

    # Parse outputs JSON
    try:
        outputs = json.loads(proc.stdout)
    except json.JSONDecodeError:
        alt = outdir / "cwl.output.json"
        if alt.exists():
            outputs = _read_json(alt)
        else:
            raise

    # Overall materialization
    run_meta = {
        "cwl_path": cwl_path,
        "cwl_sha256": cwl_hash,
        "job_path": job_path,
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
            asset_key=dg.AssetKey(["cwl_run", Path(cwl_path).stem]),
            description="CWL run materialization",
            metadata=run_meta,
        )
    )

    # Per-output materializations
    resolved: Dict[str, Any] = {}
    for name, val in outputs.items():
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
            context.log_event(
                dg.AssetMaterialization(
                    asset_key=dg.AssetKey(["cwl_output", name]),
                    description=f"CWL output value for '{name}'",
                    metadata={"value_preview": str(val)[:500]},
                )
            )
            resolved[name] = val

    return resolved


# --------------------------------------------------------------------
# Downstream: select and consume one output (optional chain)
# --------------------------------------------------------------------


@dg.op(
    config_schema={
        "output_key": dg.Field(str, description="Name of the CWL output to select")
    },
    ins={"outputs_map": dg.In(dict)},
    out=dg.Out(
        str,
        io_manager_key="path_registry",
        description="Filesystem path of the selected CWL output",
    ),
)
def select_cwl_output(context, outputs_map: dict) -> str:
    key = context.op_config["output_key"]
    if key not in outputs_map:
        raise dg.Failure(
            f"Output key '{key}' not found. Available: {list(outputs_map.keys())}"
        )
    path = outputs_map[key]
    if not isinstance(path, str):
        raise dg.Failure(
            f"Selected output '{key}' is not a file path (got {type(path).__name__})."
        )
    context.log.info(f"Selected CWL output '{key}': {path}")
    return path


@dg.op(ins={"file_path": dg.In(str)}, out=dg.Out(dg.Nothing))
def consume_selected_file(context, file_path: str) -> None:
    p = Path(file_path)
    if not p.exists():
        raise dg.Failure(f"File does not exist: {p}")
    size = p.stat().st_size
    sha256 = _sha256_file(p)
    try:
        with open(p, "rb") as f:
            head = f.read(256)
    except Exception:
        head = b""

    context.log_event(
        dg.AssetMaterialization(
            asset_key=dg.AssetKey(["consumed_output", p.name]),
            description="Verified selected CWL file output",
            metadata={
                "path": str(p.resolve()),
                "size": size,
                "sha256": sha256,
                "preview_bytes_hex": head[:64].hex(),
            },
        )
    )


# --------------------------------------------------------------------
# Jobs and Definitions
# --------------------------------------------------------------------


@dg.job(resource_defs={"cwl_runner": cwl_runner, "io_manager": cwl_path_io_manager})
def cwl_job():
    run_cwl_op()


@dg.job(
    resource_defs={
        "cwl_runner": cwl_runner,
        "io_manager": cwl_path_io_manager,
        "path_registry": path_registry_io_manager,
    }
)
def cwl_job_with_consumer():
    outputs = run_cwl_op()
    selected = select_cwl_output(outputs)
    consume_selected_file(selected)


defs = dg.Definitions(
    jobs=[cwl_job, cwl_job_with_consumer],
    resources={
        "cwl_runner": cwl_runner,
        "io_manager": cwl_path_io_manager,
        "path_registry": path_registry_io_manager,
    },
)

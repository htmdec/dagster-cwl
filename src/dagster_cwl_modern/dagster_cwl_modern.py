"""
CWL + Dagster (Pattern A) — Dagster 1.7.x compatible (no typed Config on ops)
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple, Union

from dagster import (
    AssetKey,
    AssetMaterialization,
    Definitions,
    Field,
    IOManager,
    In,
    InputContext,
    Nothing,
    Out,
    OutputContext,
    io_manager,
    job,
    op,
    ConfigurableResource,
)

# -------------------- utils --------------------


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


# -------------------- resource --------------------


@dataclass
class CWLRunnerRuntime:
    executable: str
    default_args: Tuple[str, ...]
    provenance_dir: Optional[str]
    env: Optional[Mapping[str, str]]


class CWLRunnerResource(ConfigurableResource):
    """Modern resource is OK on 1.7.x, we just avoid typed Config on ops."""

    executable: str = "cwltool"
    default_args: list[str] = ["--enable-dev", "--no-match-user"]
    provenance_dir: Optional[str] = None
    env: Optional[Dict[str, str]] = None

    def to_runtime(self) -> CWLRunnerRuntime:
        return CWLRunnerRuntime(
            executable=self.executable,
            default_args=tuple(self.default_args),
            provenance_dir=self.provenance_dir,
            env=self.env,
        )


# -------------------- IO managers --------------------


class CWLPathIOManager(IOManager):
    def __init__(self, base_dir: str = "outputs_registry"):
        self.base_dir = base_dir

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        root = _ensure_dir(self.base_dir)
        ns = "_".join(context.get_identifier())
        out_path = root / f"{ns}.txt"
        out_path.write_text(str(obj))
        context.log.info(f"[IOManager] Wrote path to {out_path}")

    def load_input(self, context: InputContext) -> Any:
        root = Path(self.base_dir)
        ns = "_".join(context.get_identifier())
        in_path = root / f"{ns}.txt"
        if not in_path.exists():
            raise FileNotFoundError(f"Expected registry file not found: {in_path}")
        resolved = in_path.read_text().strip()
        context.log.info(f"[IOManager] Loaded path {resolved} from {in_path}")
        return resolved


@io_manager
def path_registry_io_manager(_):
    return CWLPathIOManager(base_dir="outputs_registry")


@io_manager
def cwl_path_io_manager(_):
    return CWLPathIOManager()


# -------------------- CWL runner op --------------------


@op(
    config_schema={
        "cwl_path": Field(str, description="Path or URL to the CWL workflow"),
        "job_path": Field(str, description="Path to the CWL input object (YAML/JSON)"),
        "outdir": Field(str, default_value="outputs"),
        "cachedir": Field(str, default_value=".cwl-cache"),
        "extra_args": Field([str], default_value=[]),
    },
    required_resource_keys={"cwl_runner"},
    out=Out(dict, description="Resolved CWL outputs object (parsed JSON)."),
)
def run_cwl_op(context) -> dict:
    cfg = context.op_config
    runner = context.resources.cwl_runner.to_runtime()

    cwl_path = cfg["cwl_path"]
    job_path = cfg["job_path"]
    outdir = _ensure_dir(cfg.get("outdir", "outputs")).resolve()
    cachedir = _ensure_dir(cfg.get("cachedir", ".cwl-cache")).resolve()
    extra_args = list(cfg.get("extra_args", []))

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
        cwl_path,
        job_path,
    ]
    prov_dir = None
    if runner.provenance_dir:
        prov_dir = _ensure_dir(runner.provenance_dir).resolve()
        cmd[0:0] = []  # no-op, we already appended above
        cmd[-2:-2] = ["--provenance", str(prov_dir)]

    cmd_str = " ".join(shlex.quote(x) for x in cmd)
    context.log.info(f"Running CWL: {cmd_str}")

    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, **(runner.env or {})} if runner.env is not None else None,
    )
    context.log.debug(proc.stdout[:10000])
    if proc.returncode != 0:
        context.log.error(proc.stderr[:10000])
        raise RuntimeError(f"cwltool failed with code {proc.returncode}")

    try:
        outputs = json.loads(proc.stdout)
    except json.JSONDecodeError:
        alt = outdir / "cwl.output.json"
        outputs = (
            _read_json(alt)
            if alt.exists()
            else (_ for _ in ()).throw(  # raises
                json.JSONDecodeError("Could not parse cwltool output JSON", "", 0)
            )
        )

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
        **({"provenance_dir": str(prov_dir)} if prov_dir else {}),
    }
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(["cwl_run", Path(cwl_path).stem]),
            description="CWL run materialization",
            metadata=run_meta,
        )
    )

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
                AssetMaterialization(
                    asset_key=AssetKey(["cwl_output", name]),
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
                AssetMaterialization(
                    asset_key=AssetKey(["cwl_output", name]),
                    description=f"CWL output directory for '{name}'",
                    metadata={k: v for k, v in meta.items() if v is not None},
                )
            )
            resolved[name] = path
        else:
            context.log_event(
                AssetMaterialization(
                    asset_key=AssetKey(["cwl_output", name]),
                    description=f"CWL output value for '{name}'",
                    metadata={"value_preview": str(val)[:500]},
                )
            )
            resolved[name] = val

    return resolved


# -------------------- downstream ops --------------------


@op(
    config_schema={
        "output_key": Field(str, description="Name of the CWL output to select")
    },
    ins={"outputs_map": In(dict)},
    out=Out(
        str,
        io_manager_key="path_registry",
        description="Filesystem path of the selected CWL output",
    ),
)
def select_cwl_output(context, outputs_map: dict) -> str:
    key = context.op_config["output_key"]
    if key not in outputs_map:
        raise Exception(
            f"Output key '{key}' not found. Available: {list(outputs_map.keys())}"
        )
    path = outputs_map[key]
    if not isinstance(path, str):
        raise Exception(
            f"Selected output '{key}' is not a file path (got {type(path).__name__})."
        )
    context.log.info(f"Selected CWL output '{key}': {path}")
    return path


@op(ins={"file_path": In(str)}, out=Out(Nothing))
def consume_selected_file(context, file_path: str) -> None:
    p = Path(file_path)
    if not p.exists():
        raise Exception(f"File does not exist: {p}")
    size = p.stat().st_size
    sha256 = _sha256_file(p)
    try:
        with open(p, "rb") as f:
            head = f.read(256)
    except Exception:
        head = b""

    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(["consumed_output", p.name]),
            description="Verified selected CWL file output",
            metadata={
                "path": str(p.resolve()),
                "size": size,
                "sha256": sha256,
                "preview_bytes_hex": head[:64].hex(),
            },
        )
    )


# -------------------- jobs + defs --------------------


@job(
    resource_defs={"cwl_runner": CWLRunnerResource(), "io_manager": cwl_path_io_manager}
)
def cwl_job():
    run_cwl_op()


@job(
    resource_defs={
        "cwl_runner": CWLRunnerResource(),
        "io_manager": cwl_path_io_manager,
        "path_registry": path_registry_io_manager,
    }
)
def cwl_job_with_consumer():
    outputs = run_cwl_op()
    selected = select_cwl_output(outputs)
    consume_selected_file(selected)


defs = Definitions(
    jobs=[cwl_job, cwl_job_with_consumer],
    resources={
        "cwl_runner": CWLRunnerResource(),
        "io_manager": cwl_path_io_manager,
        "path_registry": path_registry_io_manager,
    },
)

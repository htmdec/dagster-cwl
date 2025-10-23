
# CWL + Dagster (Pattern A) — Modern Repo

This repo upgrades your CWL-in-Dagster PoC to **modern Dagster (>= 1.7)** with typed config and resources.

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
dagster dev
```

Open http://localhost:3000, choose **cwl_job** or **cwl_job_with_consumer**, then in Launchpad load:

```
run_configs/cwl_job.yaml
```

This runs a tiny CWL tool that sums two integers and writes `sum.txt`. The downstream job selects
the `sum_file` output and verifies it.

## Files

- `dagster_cwl_modern.py` — modern Dagster code (Pattern A)
- `cwl/arith_workflow.cwl` — toy CWL `CommandLineTool`
- `cwl/inputs.yml` — inputs for the CWL tool (`a` and `b`)
- `run_configs/cwl_job.yaml` — ready-to-run Dagster config
- `pyproject.toml` — points Dagster at this module so `dagster dev` works without flags

## Notes

- To collect CWLProv bundles, keep `provenance_dir: "prov"` in the run config.
- Swap the runner by setting `executable: "toil-cwl-runner"` and keep the same op config.
- For larger workflows, this Pattern A keeps Dagster simple and lets CWL handle internal scatter/steps.
```

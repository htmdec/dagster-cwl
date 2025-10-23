
# CWL + Dagster

This repo uses **src/** layout so editable installs and conda builds are robust.

## Create env (Conda)

```bash
conda env create -f environment.yml
conda activate dagster-cwl
export DAGSTER_HOME="$(pwd)/.dagster_home"; mkdir -p "$DAGSTER_HOME"
dagster dev
```

Open http://localhost:3000 and load `run_configs/cwl_job.yaml` in Launchpad. Launch `cwl_job` or `cwl_job_with_consumer`.

## Notes
- Module path for Dagster discovery is set in `pyproject.toml` as:
  `dagster_cwl_modern.dagster_cwl_modern`
- The CWL toy tool writes `sum.txt`; the downstream op verifies it and emits metadata.
- Install validations without conda by: `pip install -e .[validation]`
```

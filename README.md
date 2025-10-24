
# Wrap CWL in Dagster

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
# CWL + Dagster Pattern A Proof of Concept

This repository demonstrates wrapping a CWL workflow inside Dagster as a single op that:

* Executes the CWL workflow via `cwltool` (or `toil-cwl-runner`)
* Parses its JSON output
* Emits Dagster **AssetMaterializations** for every CWL output
* Records file hashes, container provenance, and runtime arguments
* Optionally exposes one output to downstream ops through an **IOManager**




## Repository Layout

```
dagster_cwl_poc/
├── dagster_cwl_poc.py           # main Dagster definitions (Pattern A)
├── cwl_job.yaml                 # example run configuration
├── outputs/                     # workflow output directory (auto-created)
├── outputs_registry/            # IOManager registry for downstream paths
└── prov/                        # optional CWLProv provenance directory
```


## Run the Proof of Concept

### 1  Start the Dagster UI

```bash
dagster dev
```

This launches **Dagit** at [http://localhost:3000](http://localhost:3000).
Dagster automatically discovers the `defs` object in `dagster_cwl_poc.py` and exposes the `cwl_job` and `cwl_job_with_consumer` jobs.



### 2  Prepare a CWL Workflow

You’ll need a valid CWL v1.x workflow and an input job object, e.g.:

```bash
/path/to/workflow.cwl
/path/to/job.yml
```

Your job YAML might look like:

```yaml
reference: ref.fa
reads:
  - class: File
    path: reads.fastq
output_name: aligned_bam
```


### 3  Configure and Launch a Run

Edit the provided example config `cwl_job.yaml`:

```yaml
resources:
  cwl_runner:
    config:
      executable: "cwltool"
      default_args:
        - "--enable-dev"
        - "--no-match-user"
      provenance_dir: "prov"

ops:
  run_cwl_op:
    config:
      cwl_path: "/abs/path/to/workflow.cwl"
      job_path: "/abs/path/to/job.yml"
      outdir: "outputs"
      cachedir: ".cwl-cache"
      extra_args:
        - "--parallel"

  select_cwl_output:
    config:
      output_key: "aligned_bam"
```

Then, in **Dagit**, click **Launchpad → Load Config → YAML File**, choose this file, and run either:

* `cwl_job` — just runs the CWL and records assets
* `cwl_job_with_consumer` — runs the CWL, selects one output, verifies it, and emits a second materialization



### 4  View Results

* **Dagit > Assets > Materializations**
  Each CWL output appears as an asset with recorded path, checksum, and size.
* **Outputs directory** (`outputs/`)
  Contains files produced by the CWL workflow.
* **outputs_registry/**
  Stores text files mapping Dagster output identifiers to actual filesystem paths (via `CWLPathIOManager`).
* **prov/**
  (Optional) Holds CWLProv provenance bundles if `provenance_dir` was enabled.



### 5  Re-run or Chain

You can add downstream ops that consume registered file paths:

```python
from dagster import job

@job
def process_after_cwl():
    outputs = run_cwl_op()
    selected = select_cwl_output(outputs)
    consume_selected_file(selected)
```

Or schedule this job and integrate it with sensors, partitions, or your PCL event system.



## Customization

| Task                                  | Where to edit                                                           |
| ------------------------------------- | ----------------------------------------------------------------------- |
| Use a different CWL runner            | change `executable` in the `cwl_runner` resource to `"toil-cwl-runner"` |
| Add container credentials or env vars | `cwl_runner.env` in run config                                          |
| Send outputs to object storage        | replace `CWLPathIOManager` with a custom S3/MinIO IOManager             |
| Enable richer provenance              | set `provenance_dir` and parse generated CWLProv bundles                |



## Outcome

This proof of concept demonstrates that:

* A single Dagster op can fully orchestrate CWL workflows (if they are DAGs)
* Dagster can capture CWL-level provenance and expose results as assets

## Some Obvious Things
* Validate that the CWL is a DAG
* Provide more granularity in the called code (ops in Dagster parlance)
*  Provide more thoughtful materialization of results in the run metadata
*  Fix the idiocy (my hardcoded paths, etc.)
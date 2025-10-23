# dagster-cwl
A proof of concept for wrapping DAG compliant CWL with Dagster.  Created during the MADICES 2025 Workshop and Hackathon at PSI, Switzerland

* **Structured outputs**: returns the parsed CWL outputs dict and exposes per-output materializations in the Dagster UI.
* **Run provenance**: hashes the CWL and job files, records the exact invocation and optional CWLProv location.
* **Resource based config**: CWL runner path, default flags, environment, and optional provenance directory are centralized in `CWLRunnerResource`.
* **Optional IOManager**: writes a small registry file with each output path so downstream ops can consume paths deterministically without guessing.
* **Should works with other runners**: replace `executable: "cwltool"` with `"toil-cwl-runner"` and keep the same interface.
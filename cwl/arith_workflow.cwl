cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["bash", "-lc"]

inputs:
  a: int
  b: int

outputs:
  sum_file:
    type: File
    outputBinding:
      glob: sum.txt

# Build a tiny command that uses positional args $1 and $2
arguments:
  - valueFrom: |
      echo $(( $1 + $2 )) > sum.txt
    shellQuote: false
  - $(inputs.a)
  - $(inputs.b)

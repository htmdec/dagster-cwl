
cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["bash", "-lc"]
inputs:
  a:
    type: int
  b:
    type: int
outputs:
  sum_file:
    type: File
    outputBinding:
      glob: sum.txt
arguments:
  - |
    echo $(( $(inputs.a) + $(inputs.b) )) > sum.txt

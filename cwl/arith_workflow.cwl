cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["bash", "-lc"]

inputs:
  a:
    type: int
    inputBinding:
      position: 1
  b:
    type: int
    inputBinding:
      position: 2

outputs:
  sum_file:
    type: File
    outputBinding:
      glob: sum.txt

arguments:
  # <SCRIPT> — avoid $(( ... )) and $( ... ); use backticks to avoid CWL parsing '$('
  - valueFrom: |
      sum=`expr "$1" + "$2"`
      echo "$sum" > sum.txt
    shellQuote: false

  # <NAME> — required so that $1 and $2 are your two numbers inside the script
  - valueFrom: "cwltool-args"

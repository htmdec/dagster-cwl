{
    "class": "CommandLineTool",
    "baseCommand": [
        "bash",
        "-lc"
    ],
    "inputs": [
        {
            "type": "int",
            "inputBinding": {
                "position": 1
            },
            "id": "#main/a"
        },
        {
            "type": "int",
            "inputBinding": {
                "position": 2
            },
            "id": "#main/b"
        }
    ],
    "arguments": [
        {
            "valueFrom": "sum=`expr \"$1\" + \"$2\"`\necho \"$sum\" > sum.txt\n",
            "shellQuote": false
        },
        {
            "valueFrom": "cwltool-args"
        }
    ],
    "id": "#main",
    "outputs": [
        {
            "type": "File",
            "outputBinding": {
                "glob": "sum.txt"
            },
            "id": "#main/sum_file"
        }
    ],
    "cwlVersion": "v1.2"
}
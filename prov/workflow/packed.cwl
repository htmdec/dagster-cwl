{
    "class": "CommandLineTool",
    "baseCommand": [
        "bash",
        "-lc"
    ],
    "inputs": [
        {
            "type": "int",
            "id": "#main/a"
        },
        {
            "type": "int",
            "id": "#main/b"
        }
    ],
    "arguments": [
        "echo $(( $(inputs.a) + $(inputs.b) )) > sum.txt\n"
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
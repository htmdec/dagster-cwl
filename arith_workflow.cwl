cwlVersion: v1.2
class: Workflow
requirements:
  InlineJavascriptRequirement: {}

inputs:
  x: int
  y: int

outputs:
  result:
    type: int
    outputSource: square/result

steps:

  prod_div:
    run:
      class: ExpressionTool
      requirements:
        InlineJavascriptRequirement: {}
      inputs:
        x: int
        y: int
      outputs:
        prod: int
        div: float
      expression: |
        ${
          return {
            prod: inputs.x * inputs.y,
            div: inputs.x / inputs.y
          };
        }
    in:
      x: x
      y: y
    out: [prod, div]

  sum_step:
    run:
      class: ExpressionTool
      requirements:
        InlineJavascriptRequirement: {}
      inputs:
        prod: int
        div: float
      outputs:
        sum: float
      expression: |
        ${
          return { sum: inputs.prod + inputs.div };
        }
    in:
      prod: prod_div/prod
      div: prod_div/div
    out: [sum]

  square:
    run:
      class: ExpressionTool
      requirements:
        InlineJavascriptRequirement: {}
      inputs:
        x: float
      outputs:
        result: float
      expression: |
        ${
          return { result: Math.pow(inputs.x, 2) };
        }
    in:
      x: sum_step/sum
    out: [result]

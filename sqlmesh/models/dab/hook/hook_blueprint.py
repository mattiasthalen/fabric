import os
import yaml

from sqlglot import exp

from sqlmesh.core.model import model
from sqlmesh.core.macros import MacroEvaluator

script_dir = os.path.dirname(os.path.abspath(__file__))
frames_path = os.path.join(script_dir, "frames.yml")

with open(frames_path, 'r') as f:
    frames = yaml.safe_load(f)

frames_to_generate = [frame for frame in frames if not frame.get("skip_generation", False)]

@model(
    "dab.hook.frame__@{name}",
    is_sql=True,
    kind="VIEW",
    blueprints=frames_to_generate,
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    name = evaluator.blueprint_var("name")
    hooks = evaluator.blueprint_var("hooks")
    composite_hooks = evaluator.blueprint_var("composite_hooks")

    source_table = f"das.scd.scd__{name}"
    source_columns = evaluator.columns_to_types(source_table).keys()

    all_hooks = []
    primary_hook = None
    hook_expressions = []

    if isinstance(hooks, list):
        for hook in hooks:
            name = hook.get("name")
            keyset = hook.get("keyset")
            expression = hook.get("expression")

            all_hooks.append(name)

            if hook.get("primary", False):
                primary_hook = name

            hook_expression = exp.func(
                "CONCAT",
                exp.Literal.string(f"{keyset}|"),
                exp.cast(expression, exp.DataType.build("text"))
            ).as_(name)

            hook_expressions.append(hook_expression)

    composite_hook_expressions = []
    if isinstance(composite_hooks, list):
        for hook in composite_hooks:
            name = hook.get("name")
            child_hooks = hook.get("hooks")

            all_hooks.append(name)

            if hook.get("primary", False):
                primary_hook = name

            hook_expression = exp.func(
                "CONCAT_WS",
                exp.Literal.string("~"),
                *child_hooks
            ).as_(name)

            composite_hook_expressions.append(hook_expression)
    
    primary_hook_expression = exp.func(
        "CONCAT",
        exp.column(primary_hook),
        exp.Literal.string("~epoch__valid_from|"),
        exp.cast(exp.column("_record__valid_from"), exp.DataType.build("text"))
    ).as_(f"_pit_{primary_hook}")

    cte__source = exp.select(exp.Star()).from_(source_table)

    cte__hooks = (
        exp.select(
            *hook_expressions,
            exp.Star()
        ).from_("cte__source")
    )

    cte__composite_hooks = exp.select(
        *composite_hook_expressions,
        exp.Star()
    ).from_("cte__hooks")

    cte__primary_hook = exp.select(
        primary_hook_expression,
        exp.Star()
    ).from_("cte__composite_hooks")
    
    sql = (
        exp.select(
            f"_pit_{primary_hook}",
            *all_hooks,
            *source_columns
        )
        .from_("cte__primary_hooks")
        .with_("cte__source", as_=cte__source)
        .with_("cte__hooks", as_=cte__hooks)
        .with_("cte__composite_hooks", as_=cte__composite_hooks)
        .with_("cte__primary_hooks", as_=cte__primary_hook)
    )

    return sql
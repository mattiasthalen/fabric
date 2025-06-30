import os
import yaml
import subprocess
import typing as t

from sqlglot import exp
from sqlmesh.core.model import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName
from sqlmesh.core import macros

def generate_blueprints(schemas) -> list[dict[str, t.Any]]:
    """Generates a blueprint for the DAS raw models based on the dlt schema(s)."""
    script_dir = os.path.dirname(os.path.abspath(__file__))

    blueprints = []
    if isinstance(schemas, str):
        schemas = [schemas]

    for schema in schemas:
        schema_path = os.path.join(script_dir, f"../../../dlt/{schema}/schemas/export/{schema}.schema.yaml")
        schema_path = os.path.normpath(schema_path)

        with open(schema_path, 'r') as f:
            schema_dict = yaml.safe_load(f)

        schema_blueprints = [
            {
                "name": key,
                "schema": schema,
                "columns": value["columns"]
            }
            
            for key, value
            in schema_dict["tables"].items()
            if not key.startswith("_dlt")
        ]

        blueprints.extend(schema_blueprints)

    return blueprints

def hash_columns(
    *fields: t.List[exp.Expression]
) -> exp.Func:

    string_fields: t.List[exp.Expression] = []
    for i, field in enumerate(fields):
        if i > 0:
            string_fields.append(exp.Literal.string("|"))
        string_fields.append(
            exp.func(
                "COALESCE",
                exp.cast(field, exp.DataType.build("text")),
                exp.Literal.string("_sqlmesh_surrogate_key_null_"),
            )
        )

    func = exp.func(
        "SHA256",
        exp.func("CONCAT", *string_fields)
    )
    if isinstance(func, exp.MD5Digest):
        func = exp.MD5(this=func.this)

    return func

def to_timestamp(column: exp.Expression) -> exp.Expression:
    """
    Converts a unix timestamp (seconds_since_epoch) to a timestamp.

    Args:
        seconds_since_epoch: The column expression to convert.
    Returns:
        A sqlglot expression.
    """

    # Cast string to float
    seconds_since_epoch = exp.cast(
        column,
        exp.DataType.build("float")
    )

    # Convert unix timestamp (seconds) to microseconds and round to avoid decimals
    microseconds = exp.Cast(
        this=exp.Round(
            this=exp.Mul(this=seconds_since_epoch, expression=exp.Literal.number("1e6")),
            decimals=exp.Literal.number("0"),
        ),
        to=exp.DataType(this=exp.DataType.Type.BIGINT),
    )

    # Create the base datetime as '1970-01-01' cast to DATETIME2(6)
    epoch_start = exp.Cast(
        this=exp.Literal.string("1970-01-01"),
        to=exp.DataType(this=exp.DataType.Type.TIMESTAMP)
    )

    # Create DATEADD expression
    date_add = exp.func(
        "DATEADD",
        exp.Identifier(this="MICROSECOND"),
        microseconds,
        epoch_start,
    )

    # Cast the result to TIMESTAMP
    cast = exp.Cast(
        this=date_add,
        to=exp.DataType(this=exp.DataType.Type.TIMESTAMP)
    )

    return cast

@model(
    "das.raw.@{name}",
    is_sql=True,
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key="_record__hash",
        disable_restatement=True
    ),
    blueprints=generate_blueprints("northwind"),
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    name = evaluator.blueprint_var("name")
    schema = evaluator.blueprint_var("schema")
    columns = evaluator.blueprint_var("columns")

    source_table = f"landing_zone.{schema}.{name}"
    source_columns = [
        exp.cast(
            exp.column(key),
            exp.DataType.build(value["data_type"])
        ).as_(key)
        for key, value
        in columns.items()
    ]

    columns_to_hash = [column.alias for column in source_columns if not column.alias.startswith("_dlt")]
    hashed_columns = hash_columns(*columns_to_hash).as_("_record__hash")
    loaded_at = to_timestamp(exp.column("_dlt_load_id")).as_("_record__loaded_at")

    sql = (
        exp.select(
            *source_columns,
            hashed_columns,
            loaded_at
        )
        .from_(source_table)
    )

    return sql
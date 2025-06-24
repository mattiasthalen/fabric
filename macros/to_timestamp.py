from __future__ import annotations

from sqlglot import exp
from sqlmesh.core.macros import macro


@macro()
def to_timestamp(evaluator, seconds_since_epoch: exp.Expression) -> exp.Expression:
    """
    Converts a unix timestamp (seconds_since_epoch) to a timestamp.

    Args:
        seconds_since_epoch: The column expression to convert.
    Returns:
        A sqlglot expression.
    """

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
    return exp.func(
        "DATEADD",
        exp.Identifier(this="MICROSECOND"),
        microseconds,
        epoch_start,
    )

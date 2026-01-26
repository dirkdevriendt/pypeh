import polars as pl


def stat_count(
    value_col: str,
    *,
    result_aliases: list[str] = ["n", "missing_n", "missing_pct"],
) -> list[pl.Expr]:
    null_count = pl.col(value_col).null_count()
    return [
        pl.len().alias(result_aliases[0]),
        null_count.alias(result_aliases[1]),
        (null_count / pl.len()).alias(result_aliases[2]),
    ]


def stat_arithmetic(
    value_col: str,
    *,
    result_aliases: list[str] = ["mean", "st", "sem", "mean_95_ci_lower", "mean_95_ci_upper"],
) -> list[pl.Expr]:
    mean = pl.col(value_col).mean()
    sem = pl.col(value_col).std() / pl.len()
    return [
        mean.alias(result_aliases[0]),
        pl.col(value_col).std().alias(result_aliases[1]),
        sem.alias(result_aliases[2]),
        (mean - 1.96 * sem).alias(result_aliases[3]),
        (mean + 1.96 * sem).alias(result_aliases[4]),
    ]


def stat_geometric(
    value_col: str,
    *,
    result_aliases: list[str] = ["geom_mean", "geom_mean_95_ci_lower", "geom_mean_95_ci_upper"],
) -> list[pl.Expr]:
    log_mean = pl.col(value_col).log().mean()
    se = pl.col(value_col).log().std() / pl.len()
    return [
        log_mean.exp().alias(result_aliases[0]),
        (log_mean - 1.96 * se).exp().alias(result_aliases[1]),
        (log_mean + 1.96 * se).exp().alias(result_aliases[2]),
    ]


def _percentile_ci_lower(
    value_col: str,
    q: float,
    *,
    result_aliases: list[str] = ["p", "ci_lower"],
) -> tuple[pl.Expr, pl.Expr]:
    """Calculate the lower confidence interval for a given percentile.

    Formula (Conover, 1999):
        j = nq - 1.96 root(nq(1-q))

    for ci_lower / n in [0,1]
    """
    n = pl.col(value_col).count()
    se = (n * q * (1 - q)).sqrt()
    ci_lower = (n * q - 1.96 * se).ceil()

    return (
        pl.col(value_col)
        .quantile((ci_lower / n).clip(0, 1), interpolation="nearest")
        .alias(f"{result_aliases[0]}{int(q * 100)}_{result_aliases[1]}")
    )


def _percentile_ci_upper(
    value_col: str,
    q: float,
    *,
    result_aliases: list[str] = ["p", "ci_upper"],
) -> tuple[pl.Expr, pl.Expr]:
    """Calculate the upper confidence interval for a given percentile.

    Formula (Conover, 1999):
        k = nq + 1.96 root(nq(1-q))
    """
    n = pl.col(value_col).count()
    se = (n * q * (1 - q)).sqrt()
    ci_upper = (n * q + 1.96 * se).ceil()

    return (
        pl.col(value_col)
        .quantile((ci_upper / n).clip(0, 1), interpolation="nearest")
        .alias(f"{result_aliases[0]}{int(q * 100)}_{result_aliases[1]}")
    )


def stat_percentiles(
    value_col: str,
    quants: list[float] = [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95],
    *,
    result_aliases: list[str] = ["p", "ci_lower", "ci_upper"],
) -> list[pl.Expr]:
    quantile_exprs = [pl.col(value_col).quantile(q).alias(f"{result_aliases[0]}{int(q * 100)}") for q in quants]
    quantile_ci_lower_exprs = [
        _percentile_ci_lower(value_col, q, result_aliases=[result_aliases[0], result_aliases[1]]) for q in quants
    ]
    quantile_ci_upper_exprs = [
        _percentile_ci_upper(value_col, q, result_aliases=[result_aliases[0], result_aliases[2]]) for q in quants
    ]
    return quantile_exprs + quantile_ci_lower_exprs + quantile_ci_upper_exprs

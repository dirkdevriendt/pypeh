import datetime
import polars as pl


def age_in_months_from_birth_and_interview_dates(birth_date, interview_date):
    return interview_date - birth_date


def datetime_from_year_month_day(
    year: pl.Series, month: pl.Series, day: pl.Series
) -> pl.Series:
    return pl.Series(
        (
            datetime.date(int(y), int(m), int(d))
            for y, m, d in zip(year, month, day)
        ),
        dtype=pl.Date,
    )

"""Time-series forecasting utilities using linear regression."""
from __future__ import annotations
import math
from typing import NamedTuple


class ForecastPoint(NamedTuple):
    day_offset: int
    value: float
    lower: float
    upper: float


def linear_forecast(values: list[float], horizon: int = 30) -> list[ForecastPoint]:
    """Fit a simple OLS linear trend and extrapolate horizon steps.

    Returns ForecastPoint(day_offset, value, lower, upper) for each day.
    lower/upper are ±1 std dev of residuals from the trend line.
    """
    n = len(values)
    if n < 2:
        # Not enough data — flat line
        flat = values[0] if values else 0.0
        return [ForecastPoint(i + 1, flat, flat, flat) for i in range(horizon)]

    # Compute OLS slope and intercept
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    ss_xx = sum((x - x_mean) ** 2 for x in xs)
    ss_xy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    slope = ss_xy / ss_xx if ss_xx != 0 else 0.0
    intercept = y_mean - slope * x_mean

    # Residual std dev for confidence band
    residuals = [v - (intercept + slope * x) for x, v in zip(xs, values)]
    variance = sum(r ** 2 for r in residuals) / max(n - 2, 1)
    std = math.sqrt(variance)

    result = []
    for i in range(horizon):
        x_future = n + i
        pred = intercept + slope * x_future
        pred = max(pred, 0.0)  # clip to non-negative
        result.append(ForecastPoint(
            day_offset=i + 1,
            value=round(pred, 2),
            lower=round(max(pred - std, 0.0), 2),
            upper=round(pred + std, 2),
        ))
    return result


def exponential_smoothing(values: list[float], alpha: float = 0.3, horizon: int = 30) -> list[ForecastPoint]:
    """Simple exponential smoothing forecast."""
    if not values:
        return [ForecastPoint(i + 1, 0.0, 0.0, 0.0) for i in range(horizon)]

    smoothed = [values[0]]
    for v in values[1:]:
        smoothed.append(alpha * v + (1 - alpha) * smoothed[-1])

    last = smoothed[-1]
    # Compute error band from smoothing errors
    errors = [abs(s - v) for s, v in zip(smoothed[1:], values[1:])]
    avg_error = sum(errors) / len(errors) if errors else 0.0

    return [
        ForecastPoint(
            day_offset=i + 1,
            value=round(last, 2),
            lower=round(max(last - avg_error, 0.0), 2),
            upper=round(last + avg_error, 2),
        )
        for i in range(horizon)
    ]

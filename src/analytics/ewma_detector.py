"""
ewma_detector.py — Streaming Exponentially Weighted Moving Average with Dynamic Bounds.
Adapts to gradual baseline drift while detecting sharp spikes in O(1) time and memory.
"""
import math
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class EWMAState:
    mean: float = 0.0
    variance: float = 1.0
    initialized: bool = False
    update_count: int = 0


class EWMADetector:
    """
    Streaming EWMA anomaly detector.
    S_t = alpha * X_t + (1 - alpha) * S_{t-1}
    sigma_t^2 = beta * (X_t - S_{t-1})^2 + (1 - beta) * sigma_{t-1}^2
    Bounds: [S_t - k*sigma_t, S_t + k*sigma_t]
    """

    def __init__(self, alpha: float = 0.15, beta: float = 0.05, k: float = 3.0):
        self.alpha = alpha
        self.beta = beta
        self.k = k
        self.state = EWMAState()

    def update(self, value: float) -> Dict[str, Any]:
        if not self.state.initialized:
            self.state.mean = value
            self.state.variance = 1.0
            self.state.initialized = True
            self.state.update_count = 1
            return self._result(value, False, 0.0, 0.0)

        self.state.update_count += 1

        prior_mean = self.state.mean
        prior_std = math.sqrt(max(self.state.variance, 1e-4))
        
        # Test anomaly against prior adaptive state
        diff = value - prior_mean
        deviation = diff / prior_std if prior_std > 1e-4 else 0.0
        upper = prior_mean + self.k * prior_std
        lower = prior_mean - self.k * prior_std
        is_anomaly = (value > upper) or (value < lower)

        # Update variance with dampening on extreme anomalies to prevent masking
        var_weight = self.beta if not is_anomaly else (self.beta * 0.1)
        self.state.variance = (
            var_weight * (diff ** 2) + (1.0 - var_weight) * self.state.variance
        )

        # Update smoothed mean
        mean_weight = self.alpha if not is_anomaly else (self.alpha * 0.1)
        self.state.mean = mean_weight * value + (1.0 - mean_weight) * prior_mean

        # Normalized anomaly score [0, 1]
        score = min(1.0, abs(deviation) / (self.k * 2))

        return {
            "is_anomaly": is_anomaly,
            "value": round(value, 3),
            "expected": round(prior_mean, 3),
            "lower_bound": round(lower, 3),
            "upper_bound": round(upper, 3),
            "deviation_sigma": round(deviation, 3),
            "anomaly_score": round(score, 4),
        }

    def _result(self, value: float, is_anomaly: bool, score: float, deviation: float) -> Dict[str, Any]:
        std = math.sqrt(max(self.state.variance, 1e-6))
        return {
            "is_anomaly": is_anomaly,
            "value": round(value, 3),
            "expected": round(self.state.mean, 3),
            "lower_bound": round(self.state.mean - self.k * std, 3),
            "upper_bound": round(self.state.mean + self.k * std, 3),
            "deviation_sigma": round(deviation, 3),
            "anomaly_score": round(score, 4),
        }

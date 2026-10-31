"""
robust_zscore.py & cusum_detector.py — Robust Z-Score (MAD) and CUSUM Change-Point Detection.
"""
from collections import deque
import numpy as np
from typing import Any, Dict


class RobustZScoreDetector:
    """
    Sliding-window Robust Z-Score using Median Absolute Deviation (MAD).
    Resistant to masking effects and up to 50% outlier contamination.
    M_t = 0.6745 * (X_t - Median) / MAD
    """

    def __init__(self, window_size: int = 60, threshold: float = 3.5):
        self.window: deque[float] = deque(maxlen=window_size)
        self.threshold = threshold
        self.min_samples = max(8, window_size // 4)

    def update(self, value: float) -> Dict[str, Any]:
        self.window.append(value)

        if len(self.window) < self.min_samples:
            return {
                "is_anomaly": False,
                "robust_z_score": 0.0,
                "median": value,
                "mad": 0.0,
                "status": "WARMUP",
            }

        arr = np.array(self.window)
        median = float(np.median(arr))
        mad = float(np.median(np.abs(arr - median)))

        if mad < 1e-6:
            mad = float(np.mean(np.abs(arr - median)))
            if mad < 1e-6:
                return {
                    "is_anomaly": False,
                    "robust_z_score": 0.0,
                    "median": median,
                    "mad": 0.0,
                    "status": "CONSTANT",
                }

        robust_z = 0.6745 * (value - median) / mad
        is_anomaly = abs(robust_z) > self.threshold

        return {
            "is_anomaly": is_anomaly,
            "robust_z_score": round(robust_z, 3),
            "median": round(median, 3),
            "mad": round(mad, 3),
            "status": "ANOMALY" if is_anomaly else "NORMAL",
        }


class CUSUMDetector:
    """
    Two-Sided Cumulative Sum (CUSUM) for persistent baseline shift detection.
    C_t^+ = max(0, C_{t-1}^+ + X_t - (mu + k))
    C_t^- = max(0, C_{t-1}^- - X_t + (mu - k))
    """

    def __init__(self, target_mean: float, std_dev: float, slack_factor: float = 0.5, threshold_factor: float = 4.5):
        self.mu = target_mean
        self.sigma = max(std_dev, 0.01)
        self.k = slack_factor * self.sigma
        self.h = threshold_factor * self.sigma
        self.c_plus = 0.0
        self.c_minus = 0.0

    def update(self, value: float) -> Dict[str, Any]:
        self.c_plus = max(0.0, self.c_plus + value - (self.mu + self.k))
        self.c_minus = max(0.0, self.c_minus - value + (self.mu - self.k))

        is_shift = (self.c_plus > self.h) or (self.c_minus > self.h)
        direction = "UP" if self.c_plus > self.h else ("DOWN" if self.c_minus > self.h else "NONE")

        # Gradual decay after alarm to avoid permanent latch
        if is_shift:
            self.c_plus *= 0.5
            self.c_minus *= 0.5

        return {
            "is_shift": is_shift,
            "shift_direction": direction,
            "c_plus": round(self.c_plus, 3),
            "c_minus": round(self.c_minus, 3),
            "threshold": round(self.h, 3),
        }

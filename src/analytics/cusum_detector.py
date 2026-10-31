"""
cusum_detector.py — Two-Sided Cumulative Sum (CUSUM) for persistent level shifts.
"""
from typing import Any, Dict


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

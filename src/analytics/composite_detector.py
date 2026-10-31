"""
composite_detector.py — Ensemble Anomaly Detector combining EWMA, Robust Z, CUSUM, and ML.
"""
from typing import Any, Dict, List, Optional
import numpy as np

from .cusum_detector import CUSUMDetector
from .ewma_detector import EWMADetector
from .isolation_forest import MultiVariateAnomalyDetector
from .robust_zscore import RobustZScoreDetector


class CompositeAnomalyDetector:
    """
    Per-device, per-metric composite detector that fuses:
      1. EWMA (Adaptive dynamic thresholding)
      2. Robust Z-score (MAD outlier detection)
      3. CUSUM (Persistent level shifts)
    And links with the device-level Isolation Forest detector.
    """

    def __init__(self, device_id: str, metric_name: str):
        self.device_id = device_id
        self.metric_name = metric_name
        self.ewma = EWMADetector(alpha=0.15, beta=0.05, k=3.0)
        self.robust_z = RobustZScoreDetector(window_size=50, threshold=3.5)
        self.cusum: Optional[CUSUMDetector] = None
        self._warmup_buffer: List[float] = []
        self._warmup_threshold = 20

    def process(self, value: float, timestamp: str, ml_detector: Optional[MultiVariateAnomalyDetector] = None, current_metrics: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        # CUSUM initialization upon completing warmup
        if self.cusum is None:
            self._warmup_buffer.append(value)
            if len(self._warmup_buffer) >= self._warmup_threshold:
                mu = float(np.mean(self._warmup_buffer))
                sigma = float(np.std(self._warmup_buffer))
                self.cusum = CUSUMDetector(target_mean=mu, std_dev=max(sigma, 0.5))

        ewma_res = self.ewma.update(value)
        rz_res = self.robust_z.update(value)
        cusum_res = self.cusum.update(value) if self.cusum else {"is_shift": False, "shift_direction": "NONE"}

        # Optional ML evaluation
        ml_res = {"is_anomaly": False, "ml_anomaly_score": 0.0}
        if ml_detector and current_metrics:
            ml_res = ml_detector.predict(current_metrics)

        # Count signals
        stat_flags = sum([
            1 if ewma_res["is_anomaly"] else 0,
            1 if rz_res.get("is_anomaly") else 0,
            1 if cusum_res.get("is_shift") else 0,
        ])
        ml_flag = 1 if ml_res.get("is_anomaly") else 0

        # Composite score
        composite_score = (
            0.35 * ewma_res["anomaly_score"] +
            0.35 * min(1.0, abs(rz_res.get("robust_z_score", 0.0)) / 7.0) +
            0.15 * (1.0 if cusum_res.get("is_shift") else 0.0) +
            0.15 * ml_res.get("ml_anomaly_score", 0.0)
        )
        composite_score = round(min(1.0, max(0.0, composite_score)), 4)

        if stat_flags >= 2 or (stat_flags >= 1 and ml_flag):
            severity = "CRITICAL"
        elif stat_flags == 1 or ml_flag:
            severity = "WARNING"
        else:
            severity = "NORMAL"

        return {
            "device_id": self.device_id,
            "metric": self.metric_name,
            "timestamp": timestamp,
            "value": value,
            "severity": severity,
            "is_anomaly": severity != "NORMAL",
            "composite_score": composite_score,
            "evidence": {
                "ewma": {
                    "expected": ewma_res["expected"],
                    "lower_bound": ewma_res["lower_bound"],
                    "upper_bound": ewma_res["upper_bound"],
                    "deviation_sigma": ewma_res["deviation_sigma"],
                    "flagged": ewma_res["is_anomaly"],
                },
                "robust_z": {
                    "score": rz_res.get("robust_z_score", 0.0),
                    "median": rz_res.get("median", value),
                    "flagged": rz_res.get("is_anomaly", False),
                },
                "cusum": {
                    "shift_detected": cusum_res.get("is_shift", False),
                    "direction": cusum_res.get("shift_direction", "NONE"),
                    "c_plus": cusum_res.get("c_plus", 0.0),
                    "c_minus": cusum_res.get("c_minus", 0.0),
                },
                "ml_isolation_forest": {
                    "flagged": ml_res.get("is_anomaly", False),
                    "score": ml_res.get("ml_anomaly_score", 0.0),
                },
            },
        }

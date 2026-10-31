"""
isolation_forest.py — Multi-Variate Anomaly Detection using scikit-learn Isolation Forest.
"""
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from typing import Any, Dict, List, Optional


class MultiVariateAnomalyDetector:
    """
    Isolation Forest for correlated multi-metric anomaly detection across:
    [cpu_utilization, memory_utilization, throughput_bps, latency_ms, errors, discards]
    """

    FEATURE_NAMES = [
        "cpu_utilization",
        "memory_utilization",
        "throughput_bps",
        "rtt_latency_ms",
        "in_errors",
        "in_discards",
    ]

    def __init__(self, contamination: float = 0.03, n_estimators: int = 100):
        self.model = IsolationForest(
            n_estimators=n_estimators,
            contamination=contamination,
            random_state=42,
            n_jobs=1,
        )
        self.scaler = StandardScaler()
        self.is_fitted = False
        self.training_size = 0
        self._history_buffer: List[List[float]] = []

    def bootstrap_default_baseline(self):
        """
        Bootstrap with synthetic normal operating baseline data.
        """
        np.random.seed(42)
        n_samples = 400
        cpu = np.random.normal(40, 8, n_samples)
        mem = np.random.normal(55, 5, n_samples)
        tp = np.random.normal(500 * 1000000, 50 * 1000000, n_samples)
        lat = np.random.normal(12, 2, n_samples)
        err = np.maximum(0, np.random.normal(0, 1, n_samples))
        disc = np.maximum(0, np.random.normal(0, 1, n_samples))

        X = np.column_stack([cpu, mem, tp, lat, err, disc])
        self.train(X)

    def train(self, feature_matrix: np.ndarray):
        self.scaler.fit(feature_matrix)
        X_scaled = self.scaler.transform(feature_matrix)
        self.model.fit(X_scaled)
        self.is_fitted = True
        self.training_size = len(feature_matrix)

    def predict(self, feature_dict: Dict[str, float]) -> Dict[str, Any]:
        if not self.is_fitted:
            self.bootstrap_default_baseline()

        row = [
            feature_dict.get("cpu_utilization", 40.0),
            feature_dict.get("memory_utilization", 55.0),
            feature_dict.get("throughput_bps", 500000000.0),
            feature_dict.get("rtt_latency_ms", 12.0),
            feature_dict.get("in_errors", 0.0),
            feature_dict.get("in_discards", 0.0),
        ]

        X = np.array([row])
        X_scaled = self.scaler.transform(X)

        prediction = self.model.predict(X_scaled)  # -1 = anomaly, 1 = normal
        raw_score = self.model.score_samples(X_scaled)[0]

        threshold = self.model.offset_
        norm_score = np.clip(1.0 - (raw_score - threshold) / (abs(threshold) + 1e-6), 0.0, 1.0)
        is_anomaly = (prediction[0] == -1)

        # Approximate feature contributions
        contributions = {}
        for idx, feat in enumerate(self.FEATURE_NAMES):
            val = row[idx]
            mean_val = float(self.scaler.mean_[idx])
            scale_val = float(self.scaler.scale_[idx])
            z = abs(val - mean_val) / (scale_val + 1e-6)
            contributions[feat] = round(z, 2)

        return {
            "is_anomaly": bool(is_anomaly),
            "ml_anomaly_score": round(float(norm_score), 4),
            "raw_score": round(float(raw_score), 4),
            "feature_deviations": contributions,
        }

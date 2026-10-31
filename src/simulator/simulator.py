"""
simulator.py — RFC 8641 YANG-Push Telemetry Generator & Anomaly Injection Engine.
"""
import copy
from datetime import datetime, timezone
import json
import math
import random
from typing import Any, Dict, List, Optional, Tuple

from .device_profiles import AnomalyType, DEFAULT_DEVICE_FLEET, DeviceProfile


class TelemetrySimulator:
    """
    Generates realistic RFC 8641 YANG-Push notification telemetry streams
    with controllable anomaly and fault injection.
    """

    def __init__(self, devices: Optional[List[DeviceProfile]] = None, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
        fleet = devices if devices is not None else DEFAULT_DEVICE_FLEET
        self.devices: Dict[str, DeviceProfile] = {copy.deepcopy(d).device_id: copy.deepcopy(d) for d in fleet}
        self.tick_count = 0
        self.subscriptions: Dict[str, int] = {
            dev_id: 100 + idx for idx, dev_id in enumerate(self.devices.keys())
        }
        self.active_faults: Dict[str, Dict[str, Any]] = {}  # device_id -> fault info
        self.ground_truth_log: List[Dict[str, Any]] = []

    def inject_fault(self, device_id: str, anomaly_type: AnomalyType, duration_ticks: int = 20, intensity: float = 1.0):
        if device_id in self.devices:
            self.active_faults[device_id] = {
                "type": anomaly_type,
                "duration": duration_ticks,
                "remaining": duration_ticks,
                "intensity": intensity,
            }

    def clear_faults(self, device_id: Optional[str] = None):
        if device_id:
            self.active_faults.pop(device_id, None)
        else:
            self.active_faults.clear()

    def generate_next_tick(self) -> List[Tuple[str, Any]]:
        """
        Generates telemetry messages for all devices for the current tick.
        Returns a list of (device_id, message_payload) tuples.
        """
        self.tick_count += 1
        results: List[Tuple[str, Any]] = []

        for dev_id, device in self.devices.items():
            if not device.is_online:
                continue

            active_fault = self.active_faults.get(dev_id)
            anomaly_type = active_fault["type"] if active_fault else AnomalyType.NONE

            # 1. Fault: Corrupted JSON (Syntax Error)
            if anomaly_type == AnomalyType.MALFORMED_JSON:
                bad_json = f'{{"ietf-notification:notification": {{"eventTime": "{datetime.now(timezone.utc).isoformat()}", "ietf-yang-push:push-update": {{"id": {self.subscriptions[dev_id]}, "datastore-contents": {{"unclosed":'
                results.append((dev_id, bad_json))
                self._record_ground_truth(dev_id, anomaly_type)
                self._tick_fault(dev_id)
                continue

            # 2. Fault: Schema Violation (Type / Enum / Range mismatch)
            if anomaly_type == AnomalyType.SCHEMA_VIOLATION:
                bad_yang = {
                    "ietf-notification:notification": {
                        "eventTime": datetime.now(timezone.utc).isoformat(),
                        "ietf-yang-push:push-update": {
                            "id": self.subscriptions[dev_id],
                            "datastore-contents": {
                                "ietf-interfaces:interfaces": {
                                    "interface": [{
                                        "name": device.interfaces[0],
                                        "oper-status": "INVALID_STATUS_VALUE",  # Illegal enum
                                        "statistics": {
                                            "in-octets": -500,  # Negative uint64 violation
                                            "out-octets": 1000000,
                                            "in-errors": 0,
                                        },
                                    }]
                                },
                                "custom-telemetry:system": {
                                    "cpu-utilization": 250,  # Out of range 0..100
                                    "memory-utilization": 50,
                                },
                            },
                        },
                    },
                    "_simulator": {"device_id": dev_id, "tick": self.tick_count, "anomaly": "SCHEMA_VIOLATION"},
                }
                results.append((dev_id, bad_yang))
                self._record_ground_truth(dev_id, anomaly_type)
                self._tick_fault(dev_id)
                continue

            # 3. Fault: Missing Mandatory YANG Key
            if anomaly_type == AnomalyType.MISSING_MANDATORY:
                missing_key_yang = {
                    "ietf-notification:notification": {
                        "eventTime": datetime.now(timezone.utc).isoformat(),
                        "ietf-yang-push:push-update": {
                            "id": self.subscriptions[dev_id],
                            "datastore-contents": {
                                "ietf-interfaces:interfaces": {
                                    "interface": [{
                                        # Missing mandatory "name" key
                                        "oper-status": "up",
                                        "statistics": {"in-octets": 1000},
                                    }]
                                }
                            },
                        },
                    },
                    "_simulator": {"device_id": dev_id, "tick": self.tick_count, "anomaly": "MISSING_MANDATORY"},
                }
                results.append((dev_id, missing_key_yang))
                self._record_ground_truth(dev_id, anomaly_type)
                self._tick_fault(dev_id)
                continue

            # Generate valid telemetry metrics with noise + diurnal curves
            metrics = self._calculate_metrics(device, active_fault)

            # Build RFC 8641 message
            msg = self._build_rfc8641_message(device, metrics)
            results.append((dev_id, msg))

            # 4. Fault: Duplicate packet
            if anomaly_type == AnomalyType.DUPLICATE:
                results.append((dev_id, copy.deepcopy(msg)))

            self._record_ground_truth(dev_id, anomaly_type)
            self._tick_fault(dev_id)

        return results

    def _calculate_metrics(self, device: DeviceProfile, fault: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        cycle = (self.tick_count % 120) / 120.0
        diurnal = 0.8 + 0.3 * math.sin(2 * math.pi * cycle)

        cpu = device.cpu_baseline + device.cpu_std * random.gauss(0, 1) * diurnal
        mem = device.memory_baseline + device.memory_std * random.gauss(0, 0.4)
        throughput = device.throughput_baseline_mbps * diurnal + device.throughput_std_mbps * random.gauss(0, 1)
        latency = device.latency_baseline_ms + device.latency_std_ms * random.gauss(0, 1)
        errors = max(0, int(random.gauss(0, 1)))
        discards = max(0, int(random.gauss(0, 0.5)))
        oper_status = "up"

        if fault:
            f_type = fault["type"]
            intensity = fault["intensity"]

            if f_type == AnomalyType.CPU_SPIKE:
                cpu = 92.0 + 5.0 * random.random()
            elif f_type == AnomalyType.MEMORY_LEAK:
                prog = 1.0 - (fault["remaining"] / fault["duration"])
                mem = device.memory_baseline + (38.0 * prog * intensity)
            elif f_type == AnomalyType.PACKET_DROPS:
                errors = int(35 * intensity + random.randint(5, 15))
                discards = int(120 * intensity + random.randint(20, 50))
                latency += 25.0 * intensity
            elif f_type == AnomalyType.LATENCY_SPIKE:
                latency = device.latency_baseline_ms + 75.0 * intensity + random.uniform(5, 15)
            elif f_type == AnomalyType.INTERFACE_FLAP:
                oper_status = "down" if (self.tick_count % 2 == 0) else "up"
                if oper_status == "down":
                    throughput = 0.0
            elif f_type == AnomalyType.TRAFFIC_FLOOD:
                throughput = device.throughput_baseline_mbps * 2.8 * intensity
                cpu += 22.0 * intensity
                latency += 18.0 * intensity
            elif f_type == AnomalyType.BLACKOUT:
                cpu = 0.0
                mem = 0.0
                throughput = 0.0
                latency = 0.0
                oper_status = "down"

        # Safe clamping for valid schema compliance
        cpu = round(max(0.0, min(100.0, cpu)), 1)
        mem = round(max(0.0, min(100.0, mem)), 1)
        throughput_bps = int(max(0.0, throughput) * 1000000)
        latency_ms = round(max(0.0, latency), 2)

        # Monotonic counters
        bytes_added = int((throughput_bps / 8.0) * random.uniform(0.9, 1.1))
        device.in_octets += bytes_added
        device.out_octets += int(bytes_added * 0.85)
        device.in_errors += errors
        device.in_discards += discards

        return {
            "interface": device.interfaces[0],
            "oper_status": oper_status,
            "in_octets": device.in_octets,
            "out_octets": device.out_octets,
            "in_errors": device.in_errors,
            "in_discards": device.in_discards,
            "cpu_utilization": int(cpu),
            "memory_utilization": int(mem),
            "throughput_bps": throughput_bps,
            "rtt_latency_ms": latency_ms,
        }

    def _build_rfc8641_message(self, device: DeviceProfile, metrics: Dict[str, Any]) -> Dict[str, Any]:
        event_time = datetime.now(timezone.utc).isoformat()
        return {
            "ietf-notification:notification": {
                "eventTime": event_time,
                "ietf-yang-push:push-update": {
                    "id": self.subscriptions[device.device_id],
                    "datastore-contents": {
                        "ietf-interfaces:interfaces": {
                            "interface": [{
                                "name": metrics["interface"],
                                "oper-status": metrics["oper_status"],
                                "statistics": {
                                    "in-octets": str(metrics["in_octets"]),
                                    "out-octets": str(metrics["out_octets"]),
                                    "in-errors": metrics["in_errors"],
                                    "out-errors": 0,
                                    "in-discards": metrics["in_discards"],
                                },
                            }]
                        },
                        "custom-telemetry:system": {
                            "cpu-utilization": metrics["cpu_utilization"],
                            "memory-utilization": metrics["memory_utilization"],
                            "throughput-bps": str(metrics["throughput_bps"]),
                            "rtt-latency-ms": str(metrics["rtt_latency_ms"]),
                        },
                    },
                },
            },
            "_simulator": {
                "device_id": device.device_id,
                "tick": self.tick_count,
                "anomaly": self.active_faults.get(device.device_id, {}).get("type", AnomalyType.NONE).value,
            },
        }

    def _tick_fault(self, dev_id: str):
        if dev_id in self.active_faults:
            self.active_faults[dev_id]["remaining"] -= 1
            if self.active_faults[dev_id]["remaining"] <= 0:
                self.active_faults.pop(dev_id, None)

    def _record_ground_truth(self, dev_id: str, anomaly_type: AnomalyType):
        self.ground_truth_log.append({
            "tick": self.tick_count,
            "device_id": dev_id,
            "ground_truth_anomaly": anomaly_type.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        if len(self.ground_truth_log) > 2000:
            self.ground_truth_log.pop(0)

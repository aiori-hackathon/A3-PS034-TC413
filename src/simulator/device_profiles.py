"""
device_profiles.py — Pre-configured network fleet baseline definitions.
"""
from dataclasses import dataclass, field
from enum import Enum
import random
from typing import List


class AnomalyType(str, Enum):
    NONE = "NONE"
    CPU_SPIKE = "CPU_SPIKE"
    MEMORY_LEAK = "MEMORY_LEAK"
    PACKET_DROPS = "PACKET_DROPS"
    LATENCY_SPIKE = "LATENCY_SPIKE"
    INTERFACE_FLAP = "INTERFACE_FLAP"
    TRAFFIC_FLOOD = "TRAFFIC_FLOOD"
    GRADUAL_DEGRADATION = "GRADUAL_DEGRADATION"
    BLACKOUT = "BLACKOUT"
    MALFORMED_JSON = "MALFORMED_JSON"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    MISSING_MANDATORY = "MISSING_MANDATORY"
    DUPLICATE = "DUPLICATE"


@dataclass
class DeviceProfile:
    device_id: str
    device_type: str  # router, switch, firewall
    vendor: str       # cisco, arista, juniper, paloalto
    model: str
    interfaces: List[str]

    # Baseline operating metrics
    cpu_baseline: float = 40.0
    cpu_std: float = 6.0
    memory_baseline: float = 55.0
    memory_std: float = 4.0
    throughput_baseline_mbps: float = 500.0
    throughput_std_mbps: float = 40.0
    latency_baseline_ms: float = 12.0
    latency_std_ms: float = 1.5

    # Monotonic counters
    in_octets: int = field(default_factory=lambda: random.randint(10**9, 10**11))
    out_octets: int = field(default_factory=lambda: random.randint(10**9, 10**11))
    in_errors: int = 0
    in_discards: int = 0

    oper_status: str = "up"
    is_online: bool = True


DEFAULT_DEVICE_FLEET: List[DeviceProfile] = [
    DeviceProfile(
        device_id="core-router-01",
        device_type="router",
        vendor="cisco",
        model="ASR-9001",
        interfaces=["GigabitEthernet0/0/0", "GigabitEthernet0/0/1", "TenGigE0/0/0"],
        cpu_baseline=42.0,
        cpu_std=6.0,
        memory_baseline=60.0,
        memory_std=3.0,
        throughput_baseline_mbps=800.0,
        latency_baseline_ms=8.0,
        latency_std_ms=1.2,
    ),
    DeviceProfile(
        device_id="edge-switch-01",
        device_type="switch",
        vendor="arista",
        model="DCS-7050TX",
        interfaces=["Ethernet1", "Ethernet2", "Ethernet48"],
        cpu_baseline=18.0,
        cpu_std=3.0,
        memory_baseline=35.0,
        memory_std=2.0,
        throughput_baseline_mbps=250.0,
        latency_baseline_ms=2.5,
        latency_std_ms=0.4,
    ),
    DeviceProfile(
        device_id="dist-router-02",
        device_type="router",
        vendor="juniper",
        model="MX204",
        interfaces=["ge-0/0/0", "ge-0/0/1", "xe-0/0/0"],
        cpu_baseline=58.0,
        cpu_std=7.0,
        memory_baseline=68.0,
        memory_std=4.0,
        throughput_baseline_mbps=650.0,
        latency_baseline_ms=14.0,
        latency_std_ms=2.0,
    ),
    DeviceProfile(
        device_id="fw-01",
        device_type="firewall",
        vendor="paloalto",
        model="PA-3260",
        interfaces=["ethernet1/1", "ethernet1/2"],
        cpu_baseline=32.0,
        cpu_std=4.0,
        memory_baseline=48.0,
        memory_std=3.0,
        throughput_baseline_mbps=350.0,
        latency_baseline_ms=6.0,
        latency_std_ms=0.8,
    ),
]

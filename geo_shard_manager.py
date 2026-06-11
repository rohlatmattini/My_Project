import hashlib
import bisect
import math
import time
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)

# ============================================================
# CONSTANTS & MAPPINGS
# ============================================================
COUNTRY_TO_REGION: Dict[str, str] = {
    # Middle East
    "SA": "ME", "AE": "ME", "EG": "ME", "JO": "ME",
    "IQ": "ME", "SY": "ME", "LB": "ME", "KW": "ME",
    "QA": "ME", "BH": "ME", "OM": "ME", "YE": "ME",
    # Europe
    "DE": "EU", "FR": "EU", "GB": "EU", "IT": "EU",
    "ES": "EU", "NL": "EU", "PL": "EU", "SE": "EU",
    "NO": "EU", "FI": "EU", "CH": "EU", "AT": "EU",
    # North America
    "US": "NA", "CA": "NA", "MX": "NA",
    # Asia Pacific
    "CN": "AP", "JP": "AP", "KR": "AP", "IN": "AP",
    "AU": "AP", "SG": "AP", "TH": "AP", "ID": "AP",
    # Africa
    "NG": "AF", "ZA": "AF", "KE": "AF", "ET": "AF",
    # South America
    "BR": "SA_", "AR": "SA_", "CL": "SA_", "CO": "SA_",
}

REGION_NAMES = {
    "ME": "Middle East",
    "EU": "Europe",
    "NA": "North America",
    "AP": "Asia Pacific",
    "AF": "Africa",
    "SA_": "South America",
}

@dataclass
class GeoRecord:
    user_id:   str
    country:   str
    lat:       float
    lon:       float
    data:      Dict[str, Any] = field(default_factory=dict)
    timestamp: float          = field(default_factory=time.time)

    @property
    def region(self) -> str:
        return COUNTRY_TO_REGION.get(self.country, "XX")

@dataclass
class ShardInfo:
    shard_id:    str
    region:      str
    primary:     str            
    replicas:    List[str]     
    key_range:   Tuple[int, int] = (0, 0)  
    record_count: int = 0
    total_writes: int = 0
    hot_spot:    bool = False

    def load_factor(self) -> float:
        # فرضنا أن سعة الشارد 10,000 سجل للتبسيط
        return min(self.record_count / 10000, 1.0)

# ============================================================
# CONSISTENT HASHING
# ============================================================
class ConsistentHashRing:
    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self._ring:  Dict[int, str] = {}  
        self._sorted: List[int]     = []   

    def add_server(self, server_id: str):
        for i in range(self.virtual_nodes):
            key  = f"{server_id}:vnode:{i}"
            h    = self._hash(key)
            self._ring[h] = server_id
        self._sorted = sorted(self._ring.keys())

    def remove_server(self, server_id: str):
        for i in range(self.virtual_nodes):
            key = f"{server_id}:vnode:{i}"
            h   = self._hash(key)
            self._ring.pop(h, None)
        self._sorted = sorted(self._ring.keys())

    def get_server(self, key: str) -> Optional[str]:
        if not self._ring:
            return None
        h   = self._hash(key)
        pos = bisect.bisect_right(self._sorted, h) % len(self._sorted)
        return self._ring[self._sorted[pos]]

    def get_servers(self, key: str, n: int = 3) -> List[str]:
        if not self._ring:
            return []
        h     = self._hash(key)
        start = bisect.bisect_right(self._sorted, h) % len(self._sorted)
        seen  = set()
        result= []
        for i in range(len(self._sorted)):
            idx    = (start + i) % len(self._sorted)
            server = self._ring[self._sorted[idx]]
            if server not in seen:
                seen.add(server)
                result.append(server)
            if len(result) == n:
                break
        return result

    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

# ============================================================
# GEO SHARD MANAGER
# ============================================================
class GeoShardManager:
    HOT_SPOT_THRESHOLD = 0.8   

    def __init__(self):
        self.shards:   Dict[str, ShardInfo]    = {}
        self.rings:    Dict[str, ConsistentHashRing] = {}  
        self.routing:  Dict[str, str]          = {}  
        self.logger    = logging.getLogger("GeoShardManager")
        self._init_shards()

    def _init_shards(self):
        regions_config = {
            "ME":  ["me-shard-1", "me-shard-2"],
            "EU":  ["eu-shard-1", "eu-shard-2", "eu-shard-3"],
            "NA":  ["na-shard-1", "na-shard-2", "na-shard-3"],
            "AP":  ["ap-shard-1", "ap-shard-2", "ap-shard-3"],
            "AF":  ["af-shard-1"],
            "SA_": ["sa-shard-1", "sa-shard-2"],
            "XX":  ["default-shard-1"],
        }

        for region, shard_ids in regions_config.items():
            ring = ConsistentHashRing(virtual_nodes=100)
            for shard_id in shard_ids:
                shard = ShardInfo(
                    shard_id = shard_id,
                    region   = region,
                    primary  = f"server-{shard_id}-primary",
                    replicas = [
                        f"server-{shard_id}-replica-1",
                        f"server-{shard_id}-replica-2",
                    ],
                )
                self.shards[shard_id] = shard
                ring.add_server(shard_id)
            self.rings[region] = ring

        self.logger.info(f"Initialized {len(self.shards)} shards")

    def route(self, record: GeoRecord) -> ShardInfo:
        region = record.region
        ring   = self.rings.get(region, self.rings["XX"])
        key    = f"{record.country}:{record.user_id}"
        shard_id = ring.get_server(key) or "default-shard-1"

        shard = self.shards[shard_id]
        shard.record_count += 1
        shard.total_writes += 1

        if shard.load_factor() >= self.HOT_SPOT_THRESHOLD:
            shard.hot_spot = True
        
        return shard

    def route_many(self, records: List[GeoRecord]) -> Dict[str, List[GeoRecord]]:
        result: Dict[str, List[GeoRecord]] = defaultdict(list)
        for rec in records:
            shard = self.route(rec)
            result[shard.shard_id].append(rec)
        return dict(result)

    def split_hot_shard(self, shard_id: str) -> Tuple[str, str]:
        shard = self.shards.get(shard_id)
        if not shard: raise ValueError("Shard not found")

        new_id = f"{shard_id}-split-{int(time.time()) % 10000}"
        new_shard = ShardInfo(
            shard_id = new_id,
            region   = shard.region,
            primary  = f"server-{new_id}-primary",
            replicas = [f"server-{new_id}-replica-1"],
        )
        self.shards[new_id] = new_shard
        self.rings[shard.region].add_server(new_id)

        # تقسيم البيانات نظرياً
        shard.record_count //= 2
        new_shard.record_count = shard.record_count
        shard.hot_spot = False
        
        return shard_id, new_id

    def handle_hot_spots(self) -> List[Tuple[str, str]]:
        splits = []
        hot = [s for s in self.shards.values() if s.hot_spot]
        for shard in hot:
            splits.append(self.split_hot_shard(shard.shard_id))
        return splits

    # --------------------------------------------------------
    # FIXED BALANCE SCORE (Logical Accuracy)
    # --------------------------------------------------------
    def balance_score(self) -> float:
        """
        Calculates how well the data is distributed across ALL available shards.
        Uses Normalized Coefficient of Variation (CV).
        """
        # نأخذ كل الشاردات في الحسبان (حتى الفاضية)
        counts = [float(s.record_count) for s in self.shards.values()]
        n = len(counts)
        
        if n < 2: return 1.0
        
        total = sum(counts)
        if total == 0: return 1.0
        
        # المتوسط الحسابي
        mean = total / n
        
        # الانحراف المعياري (Standard Deviation)
        variance = sum((c - mean) ** 2 for c in counts) / n
        std = math.sqrt(variance)
        
        # معامل الاختلاف (CV)
        cv = std / mean
        
        # تحويل الـ CV إلى نسبة مئوية (0% توزيع سيء جداً، 100% توزيع مثالي)
        # أقصى قيمة ممكنة للـ CV هي sqrt(n - 1) عندما تكون كل البيانات في مكان واحد
        max_cv = math.sqrt(n - 1)
        
        score = 1.0 - (cv / max_cv)
        return max(0.0, min(1.0, score))
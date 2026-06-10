import hashlib
import bisect
import math
import time
import random
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)



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
        return min(self.record_count / 10_000, 1.0)



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
        """إرجاع أول n servers لنسخ البيانات"""
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

    def distribution(self) -> Dict[str, int]:
        counts: Dict[str, int] = defaultdict(int)
        for server in self._ring.values():
            counts[server] += 1
        return dict(counts)

    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)




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

        self.logger.info(
            f"Initialized {len(self.shards)} shards across "
            f"{len(self.rings)} regions"
        )


    def route(self, record: GeoRecord) -> ShardInfo:
       
        region = record.region
        ring   = self.rings.get(region, self.rings["XX"])
        key    = self._routing_key(record)
        shard_id = ring.get_server(key)

        if shard_id is None:
            shard_id = "default-shard-1"

        shard = self.shards[shard_id]
        shard.record_count += 1
        shard.total_writes += 1

        if shard.load_factor() >= self.HOT_SPOT_THRESHOLD:
            if not shard.hot_spot:
                shard.hot_spot = True
                self.logger.warning(
                    f"🔥 Hot-Spot detected: {shard_id} "
                    f"({shard.record_count:,} records)"
                )

        self.routing[key] = shard_id
        return shard

    def route_many(self, records: List[GeoRecord]) -> Dict[str, List[GeoRecord]]:
        result: Dict[str, List[GeoRecord]] = defaultdict(list)
        for rec in records:
            shard = self.route(rec)
            result[shard.shard_id].append(rec)
        return dict(result)

    def get_replicas(self, record: GeoRecord, n: int = 3) -> List[str]:
        region   = record.region
        ring     = self.rings.get(region, self.rings["XX"])
        key      = self._routing_key(record)
        return ring.get_servers(key, n)

    def _routing_key(self, record: GeoRecord) -> str:
        return f"{record.country}:{record.user_id}"


    def split_hot_shard(self, shard_id: str) -> Tuple[str, str]:
      
        shard = self.shards.get(shard_id)
        if shard is None:
            raise ValueError(f"Shard {shard_id} not found")

        new_id   = f"{shard_id}-split-{int(time.time()) % 10000}"
        new_shard = ShardInfo(
            shard_id = new_id,
            region   = shard.region,
            primary  = f"server-{new_id}-primary",
            replicas = [f"server-{new_id}-replica-1"],
        )
        self.shards[new_id] = new_shard

        ring = self.rings[shard.region]
        ring.add_server(new_id)

        shard.record_count //= 2
        new_shard.record_count = shard.record_count
        shard.hot_spot = False

        self.logger.info(
            f"✂️  Split {shard_id} → {shard_id} + {new_id}"
        )
        return shard_id, new_id

    def handle_hot_spots(self) -> List[Tuple[str, str]]:
        splits = []
        hot = [s for s in self.shards.values() if s.hot_spot]
        for shard in hot:
            pair = self.split_hot_shard(shard.shard_id)
            splits.append(pair)
        return splits


    def regional_stats(self) -> Dict[str, dict]:
        stats: Dict[str, dict] = {}
        for region in REGION_NAMES:
            region_shards = [s for s in self.shards.values() if s.region == region]
            total_records = sum(s.record_count for s in region_shards)
            hot_count     = sum(1 for s in region_shards if s.hot_spot)
            stats[region] = {
                "region_name":  REGION_NAMES.get(region, region),
                "shard_count":  len(region_shards),
                "total_records": total_records,
                "hot_shards":   hot_count,
                "shards": [
                    {
                        "id":      s.shard_id,
                        "records": s.record_count,
                        "load":    f"{s.load_factor():.1%}",
                        "hot":     s.hot_spot,
                        "primary": s.primary,
                    }
                    for s in region_shards
                ],
            }
        return stats

    def balance_score(self) -> float:

        counts = [s.record_count for s in self.shards.values() if s.record_count > 0]
        if len(counts) < 2:
            return 1.0
        mean   = sum(counts) / len(counts)
        std    = math.sqrt(sum((c - mean) ** 2 for c in counts) / len(counts))
        cv     = std / mean if mean else 0
        return max(0.0, 1.0 - cv)

    def print_summary(self):
        total_records = sum(s.record_count for s in self.shards.values())
        hot_shards    = [s for s in self.shards.values() if s.hot_spot]

        print("\n" + "═" * 65)
        print(f"  GEO SHARD MANAGER SUMMARY")
        print("═" * 65)
        print(f"  Total Shards : {len(self.shards)}")
        print(f"  Total Records: {total_records:,}")
        print(f"  Hot-Spots    : {len(hot_shards)}")
        print(f"  Balance Score: {self.balance_score():.2%}")
        print("─" * 65)
        print(f"  {'SHARD':<22} {'REGION':<10} {'RECORDS':>8} {'LOAD':>7}  STATUS")
        print("─" * 65)
        for shard in sorted(self.shards.values(), key=lambda s: s.region):
            status = "🔥 HOT" if shard.hot_spot else "✅ OK"
            print(
                f"  {shard.shard_id:<22} "
                f"{shard.region:<10} "
                f"{shard.record_count:>8,} "
                f"{shard.load_factor():>7.1%}  "
                f"{status}"
            )
        print("═" * 65)

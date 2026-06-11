import time
import asyncio
import random
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional



class RequestResult(Enum):
    ALLOWED   = "✅ ALLOWED"
    RATE_LIMITED = "🚫 RATE_LIMITED"
    BLOCKED_IP   = "⛔ BLOCKED_IP"
    WHITELISTED  = "⭐ WHITELISTED"


@dataclass
class RateLimitConfig:
    requests_per_second: float = 10.0 
    burst_size: int = 20                
    window_size: int = 60          
    max_per_window: int = 300          
    block_threshold: int = 500         
    block_duration: int = 300           


@dataclass
class TokenBucket:

    capacity: float
    rate: float          
    tokens: float = field(init=False)
    last_refill: float = field(init=False)

    def __post_init__(self):
        self.tokens = self.capacity
        self.last_refill = time.monotonic()

    def consume(self, tokens: int = 1) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_refill

        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    @property
    def fill_ratio(self) -> float:
        return self.tokens / self.capacity


@dataclass
class SlidingWindowLog:
    window_seconds: int
    max_requests: int
    timestamps: deque = field(default_factory=deque)

    def is_allowed(self) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_seconds

        while self.timestamps and self.timestamps[0] < cutoff:
            self.timestamps.popleft()

        if len(self.timestamps) < self.max_requests:
            self.timestamps.append(now)
            return True
        return False

    @property
    def current_count(self) -> int:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        return sum(1 for t in self.timestamps if t >= cutoff)



class NginxFirewall:
   

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.stats = defaultdict(lambda: {
            "allowed": 0, "blocked": 0, "rate_limited": 0
        })

        self.whitelist: set[str] = set()
        self.blacklist: set[str] = set()
        self.dynamic_blocks: dict[str, float] = {}  

        self._token_buckets: dict[str, TokenBucket] = {}
        self._sliding_windows: dict[str, SlidingWindowLog] = {}

        self._request_counts: dict[str, int] = defaultdict(int)

        print(f"🛡️  Nginx Firewall initialized")
        print(f"   Rate: {config.requests_per_second} req/s | Burst: {config.burst_size}")
        print(f"   Window: {config.window_size}s max {config.max_per_window} requests\n")

    def _get_bucket(self, ip: str) -> TokenBucket:
        if ip not in self._token_buckets:
            self._token_buckets[ip] = TokenBucket(
                capacity=self.config.burst_size,
                rate=self.config.requests_per_second
            )
        return self._token_buckets[ip]

    def _get_window(self, ip: str) -> SlidingWindowLog:
        if ip not in self._sliding_windows:
            self._sliding_windows[ip] = SlidingWindowLog(
                window_seconds=self.config.window_size,
                max_requests=self.config.max_per_window
            )
        return self._sliding_windows[ip]

    def add_to_whitelist(self, ip: str):
        self.whitelist.add(ip)
        print(f"⭐ Whitelist: {ip}")

    def add_to_blacklist(self, ip: str):
        self.blacklist.add(ip)
        print(f"⛔ Blacklist: {ip}")

    def _auto_block(self, ip: str):
        unblock_at = time.monotonic() + self.config.block_duration
        self.dynamic_blocks[ip] = unblock_at
        print(f"🔒 AUTO-BLOCKED: {ip} for {self.config.block_duration}s "
              f"(requests={self._request_counts[ip]})")

    def process_request(self, ip: str, path: str = "/") -> RequestResult:
       
        self._request_counts[ip] += 1

        if ip in self.whitelist:
            self.stats[ip]["allowed"] += 1
            return RequestResult.WHITELISTED

        if ip in self.blacklist:
            self.stats[ip]["blocked"] += 1
            return RequestResult.BLOCKED_IP

        if ip in self.dynamic_blocks:
            if time.monotonic() < self.dynamic_blocks[ip]:
                self.stats[ip]["blocked"] += 1
                return RequestResult.BLOCKED_IP
            else:
                del self.dynamic_blocks[ip]
                print(f"🔓 UNBLOCKED: {ip}")

        bucket = self._get_bucket(ip)
        if not bucket.consume():
            self.stats[ip]["rate_limited"] += 1

            if self._request_counts[ip] >= self.config.block_threshold:
                self._auto_block(ip)

            return RequestResult.RATE_LIMITED

        window = self._get_window(ip)
        if not window.is_allowed():
            self.stats[ip]["rate_limited"] += 1
            return RequestResult.RATE_LIMITED

        self.stats[ip]["allowed"] += 1
        return RequestResult.ALLOWED

    def get_ip_status(self, ip: str) -> dict:
        bucket = self._get_bucket(ip)
        window = self._get_window(ip)
        return {
            "ip": ip,
            "whitelisted": ip in self.whitelist,
            "blacklisted": ip in self.blacklist,
            "dynamically_blocked": ip in self.dynamic_blocks,
            "bucket_fill": f"{bucket.fill_ratio:.0%}",
            "window_requests": window.current_count,
            "total_requests": self._request_counts[ip],
            **self.stats[ip]
        }

    def print_report(self):
        print("\n" + "═"*55)
        print("📊 FIREWALL REPORT")
        print("═"*55)
        print(f"{'IP':<18} {'Total':>7} {'✅Allow':>8} {'🚫Limit':>8} {'⛔Block':>8}")
        print("─"*55)
        for ip, s in self.stats.items():
            total = s["allowed"] + s["blocked"] + s["rate_limited"]
            print(f"{ip:<18} {total:>7} {s['allowed']:>8} {s['rate_limited']:>8} {s['blocked']:>8}")
        print("═"*55)



class NginxConfigBuilder:
   

    @staticmethod
    def generate_config(config: RateLimitConfig,
                         whitelist: list[str],
                         blacklist: list[str]) -> str:
        white_rules = "\n".join(f"    allow {ip};" for ip in whitelist)
        black_rules = "\n".join(f"    deny {ip};" for ip in blacklist)

        return f"""

http {{
     {config.requests_per_second} req/s, burst={config.burst_size}
    limit_req_zone $binary_remote_addr
        zone=api_rate:{config.burst_size}m
        rate={int(config.requests_per_second)}r/s;

    limit_conn_zone $binary_remote_addr zone=conn_limit:10m;

   {{ $geoip2_country_code country iso_code; }}
 {{ default 0; CN 1; RU 1; }}

    server {{
        listen 80;
        listen 443 ssl;

{white_rules}
{black_rules}

        location /api/ {{
            limit_req zone=api_rate
                      burst={config.burst_size}
                      nodelay;

            limit_conn conn_limit 20;

            limit_req_status 429;
            limit_conn_status 503;

            add_header Retry-After 60;

            proxy_pass http://backend;
        }}

        location /health {{
            limit_req off;
            return 200 'OK';
        }}
    }}
}}
"""

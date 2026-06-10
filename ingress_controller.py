import time
import asyncio
import random
import hashlib
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable



class ServiceStatus(Enum):
    HEALTHY   = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED  = "degraded"


class LoadBalanceStrategy(Enum):
    ROUND_ROBIN        = "round_robin"
    LEAST_CONNECTIONS  = "least_connections"
    IP_HASH            = "ip_hash"
    WEIGHTED           = "weighted"


class CircuitState(Enum):
    CLOSED   = "CLOSED"   
    OPEN     = "OPEN"     
    HALF_OPEN = "HALF_OPEN" 


@dataclass
class ServiceInstance:
    id: str
    host: str
    port: int
    weight: int = 1
    status: ServiceStatus = ServiceStatus.HEALTHY
    active_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    response_times: deque = field(default_factory=lambda: deque(maxlen=20))

    @property
    def avg_response_ms(self) -> float:
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)

    @property
    def error_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class IngressRule:

    path_prefix: str
    service_name: str
    strip_prefix: bool = False      
    rewrite_to: Optional[str] = None
    require_auth: bool = False
    rate_limit_rps: Optional[float] = None  
    timeout_ms: int = 5000


@dataclass
class CircuitBreaker:
   
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    probe_requests: int = 3

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0

    def record_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.probe_requests:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                print(f"    ✅ Circuit CLOSED — Service recovered")
        elif self.state == CircuitState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.monotonic()

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self.success_count = 0
            print(f"    ⚡ Circuit OPEN again — Probe failed")
        elif (self.state == CircuitState.CLOSED and
              self.failure_count >= self.failure_threshold):
            self.state = CircuitState.OPEN
            print(f"    ⚡ Circuit OPEN — {self.failure_count} failures")

    def can_proceed(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            elapsed = time.monotonic() - self.last_failure_time
            if elapsed >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                print(f"    🔄 Circuit HALF_OPEN — Testing recovery...")
                return True
            return False
        return True 



class LoadBalancer:


    def __init__(self, strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN):
        self.strategy = strategy
        self._rr_index: dict[str, int] = defaultdict(int)

    def select(self, service_name: str,
               instances: list[ServiceInstance],
               client_ip: str = "") -> Optional[ServiceInstance]:
        healthy = [i for i in instances if i.status == ServiceStatus.HEALTHY]
        if not healthy:
            return None

        if self.strategy == LoadBalanceStrategy.ROUND_ROBIN:
            return self._round_robin(service_name, healthy)
        elif self.strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
            return self._least_connections(healthy)
        elif self.strategy == LoadBalanceStrategy.IP_HASH:
            return self._ip_hash(healthy, client_ip)
        elif self.strategy == LoadBalanceStrategy.WEIGHTED:
            return self._weighted(healthy)
        return healthy[0]

    def _round_robin(self, service_name: str, instances: list[ServiceInstance]) -> ServiceInstance:
        idx = self._rr_index[service_name] % len(instances)
        self._rr_index[service_name] += 1
        return instances[idx]

    def _least_connections(self, instances: list[ServiceInstance]) -> ServiceInstance:
        return min(instances, key=lambda i: i.active_connections)

    def _ip_hash(self, instances: list[ServiceInstance], client_ip: str) -> ServiceInstance:
        hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        return instances[hash_val % len(instances)]

    def _weighted(self, instances: list[ServiceInstance]) -> ServiceInstance:
        total_weight = sum(i.weight for i in instances)
        r = random.uniform(0, total_weight)
        cumulative = 0
        for instance in instances:
            cumulative += instance.weight
            if r <= cumulative:
                return instance
        return instances[-1]



class IngressController:
  

    def __init__(self,
                 lb_strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN):
        self.lb = LoadBalancer(strategy=lb_strategy)
        self.services: dict[str, list[ServiceInstance]] = {}
        self.rules: list[IngressRule] = []
        self.circuit_breakers: dict[str, CircuitBreaker] = {}
        self.metrics: dict[str, dict] = defaultdict(lambda: {
            "requests": 0, "successes": 0, "failures": 0,
            "total_latency_ms": 0.0
        })

        print(f"🚪 Ingress Controller initialized")
        print(f"   Strategy: {lb_strategy.value}\n")


    def register_service(self, name: str, instances: list[ServiceInstance]):
        self.services[name] = instances
        self.circuit_breakers[name] = CircuitBreaker()
        print(f"  📦 Registered: {name} ({len(instances)} instances)")
        for inst in instances:
            print(f"      └─ {inst.id} @ {inst.address} [weight={inst.weight}]")

    def add_rule(self, rule: IngressRule):
        self.rules.append(rule)
        self.rules.sort(key=lambda r: len(r.path_prefix), reverse=True)


    def handle_request(self,
                        path: str,
                        method: str = "GET",
                        client_ip: str = "0.0.0.0",
                        headers: dict = None) -> dict:
    
        headers = headers or {}

        rule = self._match_rule(path)
        if rule is None:
            return {"status": 404, "body": "No matching route", "latency_ms": 0}

        instances = self.services.get(rule.service_name, [])
        instance = self.lb.select(rule.service_name, instances, client_ip)
        if instance is None:
            return {"status": 503, "body": "No healthy instances", "latency_ms": 0}

        cb = self.circuit_breakers[rule.service_name]
        if not cb.can_proceed():
            return {"status": 503, "body": f"Circuit OPEN for {rule.service_name}", "latency_ms": 0}

        backend_path = self._rewrite_path(path, rule)

        start = time.monotonic()
        instance.active_connections += 1
        instance.total_requests += 1
        self.metrics[rule.service_name]["requests"] += 1

        try:
            success, status, body = self._simulate_backend(instance, backend_path, method)
            latency_ms = (time.monotonic() - start) * 1000
            instance.response_times.append(latency_ms)

            if success:
                cb.record_success()
                self.metrics[rule.service_name]["successes"] += 1
                self.metrics[rule.service_name]["total_latency_ms"] += latency_ms
                return {"status": status, "body": body,
                        "instance": instance.id, "latency_ms": round(latency_ms, 2)}
            else:
                instance.failed_requests += 1
                cb.record_failure()
                self.metrics[rule.service_name]["failures"] += 1
                return {"status": status, "body": body,
                        "instance": instance.id, "latency_ms": round(latency_ms, 2)}
        finally:
            instance.active_connections -= 1

    def _match_rule(self, path: str) -> Optional[IngressRule]:
        for rule in self.rules:
            if path.startswith(rule.path_prefix):
                return rule
        return None

    def _rewrite_path(self, path: str, rule: IngressRule) -> str:
        if rule.rewrite_to:
            return rule.rewrite_to
        if rule.strip_prefix:
            return path[len(rule.path_prefix):]
        return path

    def _simulate_backend(self,
                           instance: ServiceInstance,
                           path: str,
                           method: str) -> tuple[bool, int, str]:
        if instance.error_rate > 0:
            fail_prob = instance.error_rate
        else:
            fail_prob = (
                0.95 if instance.status == ServiceStatus.UNHEALTHY else
                0.6  if instance.status == ServiceStatus.DEGRADED else
                0.05  
            )

        if random.random() < fail_prob:
            return False, 500, f"Internal Server Error from {instance.id}"

        base_latency = random.uniform(10, 100)
        time.sleep(base_latency / 1000)
        return True, 200, f"OK from {instance.id} → {path}"


    def health_check_all(self):
        print("\n🏥 Health Check:")
        for service_name, instances in self.services.items():
            for inst in instances:
                is_healthy = random.random() > 0.15  
                inst.status = (
                    ServiceStatus.HEALTHY if is_healthy
                    else ServiceStatus.UNHEALTHY
                )
                symbol = "✅" if is_healthy else "❌"
                print(f"  {symbol} {service_name}/{inst.id} @ {inst.address}")


    def print_metrics(self):
        print("\n" + "═"*60)
        print("📊 INGRESS METRICS")
        print("═"*60)
        print(f"{'Service':<20} {'Req':>6} {'✅':>6} {'❌':>6} {'Avg ms':>8} {'CB':>10}")
        print("─"*60)
        for svc, m in self.metrics.items():
            avg = (m["total_latency_ms"] / m["successes"]) if m["successes"] > 0 else 0
            cb_state = self.circuit_breakers[svc].state.value
            print(f"{svc:<20} {m['requests']:>6} {m['successes']:>6} "
                  f"{m['failures']:>6} {avg:>8.1f} {cb_state:>10}")
        print("═"*60)

    def print_instance_stats(self):
        print("\n📦 INSTANCE STATS")
        print("─"*55)
        for svc, instances in self.services.items():
            for inst in instances:
                status_sym = "✅" if inst.status == ServiceStatus.HEALTHY else "❌"
                print(f"  {status_sym} {svc}/{inst.id:<12} "
                      f"req={inst.total_requests:>4} "
                      f"err={inst.error_rate:.0%} "
                      f"avg={inst.avg_response_ms:.1f}ms "
                      f"conn={inst.active_connections}")

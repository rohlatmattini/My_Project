import streamlit as st
import random
import time
import pandas as pd
import asyncio
import concurrent.futures

st.set_page_config(page_title="Distributed Systems Suite", layout="wide")

st.title("🚀 Distributed Systems Project")
st.markdown(
    "### Multi-Region Database | Ingress Controller | Raft Consensus | Nginx Firewall"
)
st.markdown("---")


# ============================================================
# Helper: تشغيل async بأمان داخل Streamlit
# ============================================================
def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result(timeout=10)
        else:
            return loop.run_until_complete(coro)
    except Exception as e:
        st.error(f"Async error: {e}")
        return None


# ============================================================
# 1. GeoSharding Manager
# ============================================================
from geo_shard_manager import GeoShardManager, GeoRecord, COUNTRY_TO_REGION


def get_geo_manager():
    if "geo_manager" not in st.session_state:
        st.session_state.geo_manager = GeoShardManager()
    return st.session_state.geo_manager


# ============================================================
# 2. Ingress Controller
# ============================================================
from ingress_controller import (
    IngressController,
    IngressRule,
    ServiceInstance,
    LoadBalanceStrategy,
)


@st.cache_resource
def get_ingress():
    gw = IngressController(lb_strategy=LoadBalanceStrategy.ROUND_ROBIN)
    gw.register_service(
        "user-service",
        [
            ServiceInstance("user-1", "10.0.1.1", 8080),
            ServiceInstance("user-2", "10.0.1.2", 8080),
        ],
    )
    gw.register_service(
        "order-service",
        [
            ServiceInstance("order-1", "10.0.2.1", 8081),
        ],
    )
    gw.add_rule(IngressRule("/api/users", "user-service", strip_prefix=True))
    gw.add_rule(IngressRule("/api/orders", "order-service", strip_prefix=True))
    return gw


ingress_gw = get_ingress()

# ============================================================
# 3. Raft Node
# ============================================================
from raft_node import RaftNode, NodeState


@st.cache_resource
def get_raft_cluster():
    nodes = {}
    node_ids = ["node-1", "node-2", "node-3"]
    for nid in node_ids:
        peers = [p for p in node_ids if p != nid]
        nodes[nid] = RaftNode(nid, peers)
    for node in nodes.values():
        node.cluster = nodes
    return nodes


raft_nodes = get_raft_cluster()

# ============================================================
# 4. Nginx Firewall
# ============================================================
from nginx_firewall import NginxFirewall, RateLimitConfig


@st.cache_resource
def get_firewall():
    config = RateLimitConfig(
        requests_per_second=5.0, burst_size=10, block_threshold=30, block_duration=10
    )
    return NginxFirewall(config)


fw = get_firewall()

# ============================================================
# TABS
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs(
    ["🌍 GeoSharding", "🚪 Ingress Controller", "👑 Raft Consensus", "🛡️ Nginx Firewall"]
)

# ------------------- TAB 1: GeoSharding -------------------
# ------------------- TAB 1: GeoSharding -------------------
with tab1:
    st.header("🌍 Geographic Sharding")
    st.markdown("Multi-region database with consistent hashing and hot-spot detection")

    col1, col2 = st.columns(2)
    with col1:
        num_records = st.slider("Number of records", 500, 10000, 2000, key="geo_num")
    with col2:
        region_focus = st.selectbox(
            "Focus region",
            ["All", "ME", "EU", "NA", "AP", "AF", "SA_"],
            key="geo_region"
        )

    col_reset, col_run = st.columns([1, 3])
    with col_reset:
        if st.button("🔄 Reset Manager", key="geo_reset_btn"):
            if "geo_manager" in st.session_state:
                del st.session_state.geo_manager
            # امسح نتائج القديمة
            if "geo_results" in st.session_state:
                del st.session_state.geo_results
            st.success("Manager reset!")
            st.rerun()

    with col_run:
        if st.button("🚀 Run GeoSharding Simulation", key="geo_btn"):
            geo_mgr = get_geo_manager()

            with st.spinner("Routing records across regions..."):
                records = []

                if region_focus != "All":
                    allowed_countries = [
                        c for c, r in COUNTRY_TO_REGION.items() if r == region_focus
                    ]
                else:
                    allowed_countries = list(COUNTRY_TO_REGION.keys())

                for _ in range(num_records):
                    records.append(GeoRecord(
                        user_id=f"user_{random.randint(1, 99999)}",
                        country=random.choice(allowed_countries),
                        lat=0.0, lon=0.0,
                        data={}
                    ))

                distribution = geo_mgr.route_many(records)

                # احفظ النتائج في session_state
                region_counts = {}
                for shard_id, recs in distribution.items():
                    region = geo_mgr.shards[shard_id].region
                    region_counts[region] = region_counts.get(region, 0) + len(recs)

                # خزّن النتائج
                st.session_state.geo_results = {
                    "num_records": num_records,
                    "region_focus": region_focus,
                    "region_counts": region_counts,
                }

    # ← عرض النتائج من session_state (خارج if button)
    if "geo_results" in st.session_state:
        geo_mgr = get_geo_manager()
        res = st.session_state.geo_results

        st.success(
            f"✅ Routed {res['num_records']:,} records "
            f"| Region: {res['region_focus']}"
        )

        # Bar Chart
        chart_data = pd.DataFrame({
            "Region": list(res["region_counts"].keys()),
            "Records": list(res["region_counts"].values())
        })
        st.bar_chart(chart_data.set_index("Region"))

        # Metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Shards", len(geo_mgr.shards))
        col2.metric("Balance Score", f"{geo_mgr.balance_score():.2%}")
        hot_count = sum(1 for s in geo_mgr.shards.values() if s.hot_spot)
        col3.metric("Hot Spots", hot_count)

        # Shard Details
        with st.expander("📋 Shard Details"):
            shard_data = []
            for shard in geo_mgr.shards.values():
                shard_data.append({
                    "Shard ID": shard.shard_id,
                    "Region": shard.region,
                    "Records": shard.record_count,
                    "Load Factor": f"{shard.load_factor():.1%}",
                    "Hot Spot": "🔥" if shard.hot_spot else "✅"
                })
            st.dataframe(shard_data, use_container_width=True)

        # ← زر Split هون (خارج if button، دايماً ظاهر)
        hot_shards = [s for s in geo_mgr.shards.values() if s.hot_spot]
        if hot_shards:
            st.warning(f"🔥 {len(hot_shards)} Hot Spot(s) detected!")
            
            if st.button("✂️ Split Hot Shards Now", key="geo_split_btn"):
                with st.spinner("Splitting hot shards..."):
                    splits = geo_mgr.handle_hot_spots()
                
                for old, new in splits:
                    st.info(f"✂️ Split: **{old}** → **{old}** + **{new}**")
                
                # حدّث النتائج
                region_counts_new = {}
                for shard in geo_mgr.shards.values():
                    r = shard.region
                    region_counts_new[r] = (
                        region_counts_new.get(r, 0) + shard.record_count
                    )
                st.session_state.geo_results["region_counts"] = region_counts_new
                st.rerun()
        else:
            st.success("✅ No hot spots - System is balanced!")
# ------------------- TAB 2: Ingress Controller -------------------
with tab2:
    st.header("🚪 Ingress Controller / API Gateway")
    st.markdown("Load balancing, path routing, and circuit breaker simulation")

    col1, col2 = st.columns(2)
    with col1:
        path = st.text_input("Request Path", "/api/users/123")
    with col2:
        client_ip = st.text_input("Client IP", "192.168.1.100")

    if st.button("📤 Send Request", key="ingress_btn"):
        with st.spinner("Sending request..."):
            res = ingress_gw.handle_request(path, "GET", client_ip)

        status_color = "🟢" if res["status"] == 200 else "🔴"
        st.info(f"{status_color} **HTTP {res['status']}**")
        st.write(f"**Body:** {res.get('body', 'N/A')}")
        if "instance" in res:
            st.write(f"**Instance:** `{res['instance']}`")
        st.write(f"**Latency:** `{res.get('latency_ms', 0)} ms`")

    st.subheader("📊 Service Metrics")
    if st.button("🔄 Refresh Metrics", key="ingress_metrics_btn"):
        metrics_data = []
        for svc, m in ingress_gw.metrics.items():
            total = m["requests"]
            success_rate = (m["successes"] / total * 100) if total > 0 else 0
            avg_latency = (
                m["total_latency_ms"] / m["successes"] if m["successes"] > 0 else 0
            )
            cb_state = ingress_gw.circuit_breakers[svc].state.value
            metrics_data.append(
                {
                    "Service": svc,
                    "Total Requests": m["requests"],
                    "Successes": m["successes"],
                    "Failures": m["failures"],
                    "Success Rate": f"{success_rate:.1f}%",
                    "Avg Latency (ms)": f"{avg_latency:.1f}",
                    "Circuit Breaker": cb_state,
                }
            )
        if metrics_data:
            st.dataframe(metrics_data)
        else:
            st.info("No requests sent yet.")

    with st.expander("⚙️ Registered Services"):
        for svc, instances in ingress_gw.services.items():
            st.markdown(f"**{svc}**")
            for inst in instances:
                status_sym = "✅" if inst.status.value == "healthy" else "❌"
                st.write(
                    f"  {status_sym} `{inst.id}` @ `{inst.address}` "
                    f"| weight={inst.weight} "
                    f"| req={inst.total_requests} "
                    f"| err={inst.error_rate:.0%}"
                )

# ------------------- TAB 3: Raft Consensus -------------------
with tab3:
    st.header("👑 Raft Consensus Algorithm")
    st.markdown("Leader election and log replication simulation")

    # Status Table
    status_data = []
    leader_id = None
    for nid, node in raft_nodes.items():
        s = node.status()
        if s["state"] == "LEADER":
            leader_id = nid
        status_data.append(
            {
                "Node": nid,
                "State": s["state"],
                "Term": s["term"],
                "Leader": s["leader"] or "None",
                "Log Length": s["log_length"],
                "Commit Index": s["commit_index"],
            }
        )

    st.dataframe(status_data, use_container_width=True)

    if leader_id:
        st.success(
            f"👑 Current Leader: **{leader_id}** | Term: {raft_nodes[leader_id].current_term}"
        )
    else:
        st.warning("⚠️ No leader elected yet. Click 'Start Election'.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🗳️ Election")
        if st.button("🗳️ Start Election", key="raft_election_btn"):
            with st.spinner("Running election..."):
                elected = False
                for node in raft_nodes.values():
                    if node.state != NodeState.LEADER:
                        result = run_async(node._start_election())
                        if result is not None or True:
                            elected = True
                        break
            st.rerun()

    with col2:
        st.subheader("📝 Client Command")
        cmd_key = st.text_input("Key", "username", key="raft_key")
        cmd_value = st.text_input("Value", "admin", key="raft_val")

        if st.button("📝 Send Command", key="raft_cmd_btn"):
            leader = None
            for node in raft_nodes.values():
                if node.state == NodeState.LEADER:
                    leader = node
                    break

            if leader:
                with st.spinner("Replicating command..."):
                    result = run_async(
                        leader.client_request(
                            {"op": "set", "key": cmd_key, "value": cmd_value}
                        )
                    )
                if result:
                    st.success(f"✅ Committed: `{cmd_key}` = `{cmd_value}`")
                else:
                    st.error("❌ Command failed - check leader status")
            else:
                st.error("❌ No leader! Run election first.")
            st.rerun()

    # State Machine
    with st.expander("⚙️ State Machine (All Nodes)"):
        sm_data = []
        for nid, node in raft_nodes.items():
            sm_data.append(
                {
                    "Node": nid,
                    "State": node.state.value,
                    "Term": node.current_term,
                    "Log Length": len(node.log),
                    "Commit Index": node.commit_index,
                    "State Machine": str(node.state_machine),
                }
            )
        st.dataframe(sm_data, use_container_width=True)

    # Log Entries
    with st.expander("📜 Log Entries (Leader)"):
        if leader_id:
            leader_node = raft_nodes[leader_id]
            if leader_node.log:
                log_data = []
                for entry in leader_node.log:
                    log_data.append(
                        {
                            "Index": entry.index,
                            "Term": entry.term,
                            "Command": str(entry.command),
                            "Committed": "✅" if entry.committed else "⏳",
                        }
                    )
                st.dataframe(log_data, use_container_width=True)
            else:
                st.info("No log entries yet.")
        else:
            st.info("No leader elected.")

# ------------------- TAB 4: Nginx Firewall -------------------
with tab4:
    st.header("🛡️ Nginx-Style Rate Limiting & Firewall")
    st.markdown("Token bucket rate limiting, IP blacklist/whitelist, and auto-blocking")

    col1, col2, col3 = st.columns(3)
    with col1:
        test_ip = st.text_input("Test IP", "192.168.1.100", key="fw_ip")
    with col2:
        fw_path = st.text_input("Path", "/api/data", key="fw_path")
    with col3:
        requests_count = st.slider("Number of requests", 1, 50, 20, key="fw_count")

    if st.button("🔍 Test Rate Limiting", key="fw_test_btn"):
        results = {
            "✅ ALLOWED": 0,
            "🚫 RATE_LIMITED": 0,
            "⛔ BLOCKED": 0,
            "⭐ WHITELISTED": 0,
        }

        progress = st.progress(0)
        for i in range(requests_count):
            res = fw.process_request(test_ip, fw_path)
            res_str = res.name if hasattr(res, "name") else str(res)

            # ترتيب صحيح: WHITELISTED أول
            if "WHITELIST" in res_str:
                results["⭐ WHITELISTED"] += 1
            elif "RATE_LIMITED" in res_str:
                results["🚫 RATE_LIMITED"] += 1
            elif "BLOCK" in res_str:
                results["⛔ BLOCKED"] += 1
            elif "ALLOWED" in res_str:
                results["✅ ALLOWED"] += 1

            progress.progress((i + 1) / requests_count)
            time.sleep(0.01)

        progress.empty()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("✅ Allowed", results["✅ ALLOWED"])
        col2.metric("🚫 Rate Limited", results["🚫 RATE_LIMITED"])
        col3.metric("⛔ Blocked", results["⛔ BLOCKED"])
        col4.metric("⭐ Whitelisted", results["⭐ WHITELISTED"])

        # IP Status
        status = fw.get_ip_status(test_ip)
        st.info(f"""
**IP Status: `{test_ip}`**
- 🔵 Whitelisted: `{status["whitelisted"]}`
- 🔴 Blacklisted: `{status["blacklisted"]}`
- 🔒 Dynamically Blocked: `{status["dynamically_blocked"]}`
- 🪣 Bucket Fill: `{status["bucket_fill"]}`
- 📊 Window Requests: `{status["window_requests"]}`
- 📈 Total Requests: `{status["total_requests"]}`
        """)

    st.subheader("🔧 IP Management")
    col1, col2 = st.columns(2)
    with col1:
        whitelist_ip = st.text_input("Add to Whitelist", "10.0.0.1", key="wl_ip")
        if st.button("➕ Add Whitelist", key="wl_btn"):
            fw.add_to_whitelist(whitelist_ip)
            st.success(f"⭐ Added `{whitelist_ip}` to whitelist")

    with col2:
        blacklist_ip = st.text_input("Add to Blacklist", "1.2.3.4", key="bl_ip")
        if st.button("⛔ Add Blacklist", key="bl_btn"):
            fw.add_to_blacklist(blacklist_ip)
            st.success(f"🚫 Added `{blacklist_ip}` to blacklist")

    with st.expander("📊 Firewall Stats"):
        if fw.stats:
            stats_data = []
            for ip, s in fw.stats.items():
                total = s["allowed"] + s["rate_limited"] + s["blocked"]
                stats_data.append(
                    {
                        "IP": ip,
                        "Total": total,
                        "✅ Allowed": s["allowed"],
                        "🚫 Rate Limited": s["rate_limited"],
                        "⛔ Blocked": s["blocked"],
                    }
                )
            st.dataframe(stats_data, use_container_width=True)
        else:
            st.info("No requests processed yet.")

    with st.expander("⚙️ Current Configuration"):
        config_data = {
            "Setting": [
                "Requests/sec",
                "Burst Size",
                "Window Size",
                "Max per Window",
                "Block Threshold",
                "Block Duration",
            ],
            "Value": [
                fw.config.requests_per_second,
                fw.config.burst_size,
                fw.config.window_size,
                fw.config.max_per_window,
                fw.config.block_threshold,
                f"{fw.config.block_duration}s",
            ],
        }
        st.dataframe(config_data, use_container_width=True)

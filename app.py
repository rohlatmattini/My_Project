import streamlit as st
import random
import time
import pandas as pd

st.set_page_config(page_title="Distributed Systems Suite", layout="wide")

st.title("🚀 Distributed Systems Project")
st.markdown("### Multi-Region Database | Ingress Controller | Raft Consensus | Nginx Firewall")
st.markdown("---")

# ============================================================
# 1. GeoSharding Manager
# ============================================================
from geo_shard_manager import GeoShardManager, GeoRecord, COUNTRY_TO_REGION

@st.cache_resource
def get_geo_manager():
    return GeoShardManager()

geo_mgr = get_geo_manager()

# ============================================================
# 2. Ingress Controller
# ============================================================
from ingress_controller import IngressController, IngressRule, ServiceInstance, LoadBalanceStrategy

@st.cache_resource
def get_ingress():
    gw = IngressController(lb_strategy=LoadBalanceStrategy.ROUND_ROBIN)
    gw.register_service("user-service", [
        ServiceInstance("user-1", "10.0.1.1", 8080),
        ServiceInstance("user-2", "10.0.1.2", 8080),
    ])
    gw.register_service("order-service", [
        ServiceInstance("order-1", "10.0.2.1", 8081),
    ])
    gw.add_rule(IngressRule("/api/users", "user-service", strip_prefix=True))
    gw.add_rule(IngressRule("/api/orders", "order-service", strip_prefix=True))
    return gw

ingress_gw = get_ingress()

# ============================================================
# 3. Raft Node (Simulation)
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
        requests_per_second=5.0,
        burst_size=10,
        block_threshold=30,
        block_duration=10
    )
    return NginxFirewall(config)

fw = get_firewall()

# ============================================================
# TABS: تشغيل كل تاسك على حدة
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "🌍 GeoSharding", 
    "🚪 Ingress Controller", 
    "👑 Raft Consensus", 
    "🛡️ Nginx Firewall"
])

# ------------------- TAB 1: GeoSharding -------------------
with tab1:
    st.header("🌍 Geographic Sharding")
    st.markdown("Multi-region database with consistent hashing and hot-spot detection")
    
    col1, col2 = st.columns(2)
    with col1:
        num_records = st.slider("Number of records", 500, 10000, 2000, key="geo_num")
    with col2:
        region_focus = st.selectbox("Focus region", ["All", "ME", "EU", "NA", "AP"], key="geo_region")
    
    if st.button("🚀 Run GeoSharding Simulation", key="geo_btn"):
        with st.spinner("Routing records across regions..."):
            records = []
            countries = list(COUNTRY_TO_REGION.keys())
            for _ in range(num_records):
                records.append(GeoRecord(
                    user_id=f"user_{random.randint(1,99999)}",
                    country=random.choice(countries),
                    lat=0.0, lon=0.0,
                    data={}
                ))
            
            distribution = geo_mgr.route_many(records)
            
            # Results
            region_counts = {}
            for shard_id, recs in distribution.items():
                region = geo_mgr.shards[shard_id].region
                region_counts[region] = region_counts.get(region, 0) + len(recs)
            
            st.success(f"✅ Routed {num_records:,} records")
            
            # Chart
            chart_data = pd.DataFrame({
                "Region": list(region_counts.keys()),
                "Records": list(region_counts.values())
            })
            st.bar_chart(chart_data.set_index("Region"))
            
            # Metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Shards", len(geo_mgr.shards))
            col2.metric("Balance Score", f"{geo_mgr.balance_score():.2%}")
            hot_count = sum(1 for s in geo_mgr.shards.values() if s.hot_spot)
            col3.metric("Hot Spots", hot_count)
            
            # Shard table
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
                st.dataframe(shard_data)

# ------------------- TAB 2: Ingress Controller -------------------
with tab2:
    st.header("🚪 Ingress Controller / API Gateway")
    st.markdown("Load balancing, path routing, and circuit breaker simulation")
    
    col1, col2 = st.columns(2)
    with col1:
        path = st.text_input("Request Path", "/api/users/123")
    with col2:
        client_ip = st.text_input("Client IP", "192.168.1.100")
    
    if st.button("Send Request", key="ingress_btn"):
        res = ingress_gw.handle_request(path, "GET", client_ip)
        
        st.info(f"**Response:** HTTP {res['status']}")
        st.write(f"**Body:** {res.get('body', 'N/A')}")
        if "instance" in res:
            st.write(f"**Instance:** {res['instance']}")
        st.write(f"**Latency:** {res.get('latency_ms', 0)} ms")
    
    st.subheader("📊 Metrics")
    if st.button("Refresh Metrics", key="ingress_metrics_btn"):
        ingress_gw.print_metrics()
        st.write("Check console for detailed metrics (or view below):")
        metrics_data = []
        for svc, m in ingress_gw.metrics.items():
            metrics_data.append({
                "Service": svc,
                "Requests": m["requests"],
                "Successes": m["successes"],
                "Failures": m["failures"]
            })
        st.dataframe(metrics_data)
    
    with st.expander("⚙️ Registered Services"):
        for svc, instances in ingress_gw.services.items():
            st.markdown(f"**{svc}**")
            for inst in instances:
                st.write(f"  - {inst.id} @ {inst.address} (weight={inst.weight})")

# ------------------- TAB 3: Raft Consensus -------------------
with tab3:
    st.header("👑 Raft Consensus Algorithm")
    st.markdown("Leader election and log replication simulation")
    
    # Show current status
    status_data = []
    leader_id = None
    for nid, node in raft_nodes.items():
        s = node.status()
        if s["state"] == "LEADER":
            leader_id = nid
        status_data.append({
            "Node": nid,
            "State": s["state"],
            "Term": s["term"],
            "Leader": s["leader"] or "None",
            "Log Length": s["log_length"],
            "Commit Index": s["commit_index"]
        })
    
    st.dataframe(status_data)
    
    if leader_id:
        st.success(f"👑 Current Leader: **{leader_id}**")
    else:
        st.warning("No leader elected yet. Run election.")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🗳️ Start Election", key="raft_election_btn"):
            with st.spinner("Starting election..."):
                # Find a follower to start election
                for node in raft_nodes.values():
                    if node.state != NodeState.LEADER:
                        # Simulate election start
                        import asyncio
                        asyncio.run(node._start_election())
                        break
            st.rerun()
    
    with col2:
        cmd_key = st.text_input("Key", "test_key")
        cmd_value = st.text_input("Value", "test_value")
        if st.button("📝 Send Command", key="raft_cmd_btn"):
            leader = None
            for node in raft_nodes.values():
                if node.state == NodeState.LEADER:
                    leader = node
                    break
            if leader:
                import asyncio
                result = asyncio.run(leader.client_request({"op": "set", "key": cmd_key, "value": cmd_value}))
                if result:
                    st.success(f"Command committed: {cmd_key} = {cmd_value}")
                else:
                    st.error("Command failed")
            else:
                st.error("No leader available")
    
    with st.expander("📜 State Machine Values"):
        sm_data = []
        for nid, node in raft_nodes.items():
            sm_data.append({
                "Node": nid,
                "State Machine": str(node.state_machine)
            })
        st.dataframe(sm_data)

# ------------------- TAB 4: Nginx Firewall -------------------
with tab4:
    st.header("🛡️ Nginx-Style Rate Limiting & Firewall")
    st.markdown("Token bucket rate limiting, IP blacklist/whitelist, and auto-blocking")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        test_ip = st.text_input("Test IP", "192.168.1.100", key="fw_ip")
    with col2:
        path = st.text_input("Path", "/api/data", key="fw_path")
    with col3:
        requests_count = st.slider("Number of requests", 1, 50, 10, key="fw_count")
    
    if st.button("🔍 Test Rate Limiting", key="fw_test_btn"):
        results = {"✅ ALLOWED": 0, "🚫 RATE_LIMITED": 0, "⛔ BLOCKED": 0, "⭐ WHITELISTED": 0}
        for i in range(requests_count):
            res = fw.process_request(test_ip, path)
            key = res.value
            # تطابق القيم مع المفاتيح
            if key == "ALLOWED":
                results["✅ ALLOWED"] += 1
            elif key == "RATE_LIMITED":
                results["🚫 RATE_LIMITED"] += 1
            elif key == "BLOCKED_IP":
                results["⛔ BLOCKED"] += 1
            elif key == "WHITELISTED":
                results["⭐ WHITELISTED"] += 1
            time.sleep(0.05)
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("✅ Allowed", results["✅ ALLOWED"])
        col2.metric("🚫 Rate Limited", results["🚫 RATE_LIMITED"])
        col3.metric("⛔ Blocked", results["⛔ BLOCKED"])
        col4.metric("⭐ Whitelisted", results["⭐ WHITELISTED"])
        
        # Show status
        status = fw.get_ip_status(test_ip)
        st.info(f"""
        **IP Status:** {test_ip}
        - Whitelisted: {status['whitelisted']}
        - Blacklisted: {status['blacklisted']}
        - Dynamically Blocked: {status['dynamically_blocked']}
        - Bucket Fill: {status['bucket_fill']}
        - Window Requests: {status['window_requests']}
        - Total Requests: {status['total_requests']}
        """)
    
    st.subheader("IP Management")
    col1, col2 = st.columns(2)
    with col1:
        whitelist_ip = st.text_input("Add to Whitelist", "10.0.0.1", key="wl_ip")
        if st.button("➕ Add Whitelist", key="wl_btn"):
            fw.add_to_whitelist(whitelist_ip)
            st.success(f"Added {whitelist_ip} to whitelist")
    
    with col2:
        blacklist_ip = st.text_input("Add to Blacklist", "1.2.3.4", key="bl_ip")
        if st.button("⛔ Add Blacklist", key="bl_btn"):
            fw.add_to_blacklist(blacklist_ip)
            st.success(f"Added {blacklist_ip} to blacklist")
    
    with st.expander("📊 Firewall Stats"):
        stats_data = []
        for ip, s in fw.stats.items():
            stats_data.append({
                "IP": ip,
                "Allowed": s["allowed"],
                "Rate Limited": s["rate_limited"],
                "Blocked": s["blocked"]
            })
        st.dataframe(stats_data)
    
    with st.expander("⚙️ Current Configuration"):
        st.write(f"Requests/sec: {fw.config.requests_per_second}")
        st.write(f"Burst Size: {fw.config.burst_size}")
        st.write(f"Block Threshold: {fw.config.block_threshold}")
        st.write(f"Block Duration: {fw.config.block_duration}s")
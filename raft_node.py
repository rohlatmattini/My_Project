import asyncio
import random
import time
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)


class NodeState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass
class LogEntry:
    term: int
    index: int
    command: Any
    committed: bool = False

    def __repr__(self):
        status = "✓" if self.committed else "○"
        return f"[{status} T{self.term}|I{self.index}] {self.command}"


@dataclass
class VoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    term: int
    vote_granted: bool
    voter_id: str


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    node_id: str
    match_index: int = 0


class RaftNode:
    ELECTION_TIMEOUT_MIN = 1.5
    ELECTION_TIMEOUT_MAX = 3.0
    HEARTBEAT_INTERVAL = 0.5

    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers
        self.cluster = {}
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        self.state: NodeState = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None
        self.votes_received: int = 0
        self.last_heartbeat: float = time.time()
        self.election_timeout: float = self._new_election_timeout()
        self.state_machine: Dict[str, Any] = {}
        self.logger = logging.getLogger(f"Raft[{node_id}]")
        self._running = False

    def _new_election_timeout(self) -> float:
        return random.uniform(self.ELECTION_TIMEOUT_MIN, self.ELECTION_TIMEOUT_MAX)

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _quorum(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def _update_term(self, new_term: int):
        if new_term > self.current_term:
            self.current_term = new_term
            self.voted_for = None
            self.state = NodeState.FOLLOWER
            self.leader_id = None
            self.logger.info(f"Term updated → {new_term}, reverted to FOLLOWER")

    # --- تم تصحيح الإزاحة هنا ---
    async def _start_election(self):
        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = 1
        self.last_heartbeat = time.time()

        self.logger.info(f"🗳 Starting election | term={self.current_term}")

        req = VoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.last_log_index,
            last_log_term=self.last_log_term,
        )

        # نطلب التصويت من جميع العقد بشكل مباشر لضمان استجابة Streamlit
        for peer_id in self.peers:
            peer = self.cluster.get(peer_id)
            if peer:
                try:
                    resp = await peer.handle_vote_request(req)
                    await self._handle_vote_response(resp)
                except Exception as e:
                    self.logger.error(f"Failed to get vote from {peer_id}: {e}")

    async def handle_vote_request(self, req: VoteRequest) -> VoteResponse:
        self._update_term(req.term)
        if req.term < self.current_term:
            return VoteResponse(self.current_term, False, self.node_id)

        can_vote = self.voted_for is None or self.voted_for == req.candidate_id

        log_ok = req.last_log_term > self.last_log_term or (
            req.last_log_term == self.last_log_term
            and req.last_log_index >= self.last_log_index
        )

        if can_vote and log_ok:
            self.voted_for = req.candidate_id
            self.last_heartbeat = time.time()
            self.logger.info(f"✅ Voted for {req.candidate_id}")
            return VoteResponse(self.current_term, True, self.node_id)

        return VoteResponse(self.current_term, False, self.node_id)

    async def _handle_vote_response(self, resp: VoteResponse):
        self._update_term(resp.term)
        if self.state != NodeState.CANDIDATE:
            return
        if resp.vote_granted:
            self.votes_received += 1
            if self.votes_received >= self._quorum():
                await self._become_leader()

    async def _become_leader(self):
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        self.logger.info(f"👑 BECAME LEADER for term {self.current_term}")
        for peer_id in self.peers:
            self.next_index[peer_id] = self.last_log_index + 1
            self.match_index[peer_id] = 0

    # ── Log Replication ──────────────────────
    async def client_request(self, command: Any) -> bool:
        if self.state != NodeState.LEADER:
            return False
        entry = LogEntry(
            term=self.current_term, index=self.last_log_index + 1, command=command
        )
        self.log.append(entry)
        await self._broadcast_append_entries()
        return True

    async def _broadcast_append_entries(self):
        if self.state != NodeState.LEADER:
            return
        for peer_id in self.peers:
            await self._send_append_entries(peer_id)

    async def _send_append_entries(self, peer_id: str):
        peer = self.cluster.get(peer_id)
        if not peer:
            return
        next_idx = self.next_index.get(peer_id, 1)
        prev_idx = next_idx - 1
        prev_term = self.log[prev_idx - 1].term if prev_idx > 0 else 0
        entries = self.log[next_idx - 1 :]
        req = AppendEntriesRequest(
            self.current_term,
            self.node_id,
            prev_idx,
            prev_term,
            entries,
            self.commit_index,
        )
        resp = await peer.handle_append_entries(req)
        await self._handle_append_response(peer_id, resp, len(entries))

    async def handle_append_entries(
        self, req: AppendEntriesRequest
    ) -> AppendEntriesResponse:
        self._update_term(req.term)
        if req.term < self.current_term:
            return AppendEntriesResponse(self.current_term, False, self.node_id)
        self.last_heartbeat, self.leader_id = time.time(), req.leader_id
        if self.state != NodeState.FOLLOWER:
            self.state = NodeState.FOLLOWER
        # (بقية منطق السجلات مبسط للمحاكاة)
        if req.entries:
            self.log.extend(req.entries)
        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, self.last_log_index)
            await self._apply_committed()
        return AppendEntriesResponse(
            self.current_term, True, self.node_id, self.last_log_index
        )

    async def _handle_append_response(
        self, peer_id: str, resp: AppendEntriesResponse, sent: int
    ):
        self._update_term(resp.term)
        if self.state == NodeState.LEADER and resp.success:
            self.match_index[peer_id] = resp.match_index
            self.next_index[peer_id] = resp.match_index + 1
            await self._advance_commit_index()

    async def _advance_commit_index(self):
        for n in range(self.last_log_index, self.commit_index, -1):
            if self.log[n - 1].term == self.current_term:
                if (
                    1 + sum(1 for m in self.match_index.values() if m >= n)
                    >= self._quorum()
                ):
                    self.commit_index = n
                    await self._apply_committed()
                    break

    async def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            entry.committed = True
            cmd = entry.command
            if isinstance(cmd, dict) and cmd.get("op") == "set":
                self.state_machine[cmd["key"]] = cmd["value"]

    def status(self) -> dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.current_term,
            "leader": self.leader_id,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "state_machine": self.state_machine,
            "log": [repr(e) for e in self.log],
        }

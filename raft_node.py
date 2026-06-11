import asyncio
import random
import time
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)


class NodeState(Enum):
    FOLLOWER  = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER    = "LEADER"


@dataclass
class LogEntry:
    term:    int          
    index:   int          
    command: Any          
    committed: bool = False

    def __repr__(self):
        status = "✓" if self.committed else "○"
        return f"[{status} T{self.term}|I{self.index}] {self.command}"


@dataclass
class VoteRequest:
    term:           int
    candidate_id:   str
    last_log_index: int
    last_log_term:  int


@dataclass
class VoteResponse:
    term:         int
    vote_granted: bool
    voter_id:     str


@dataclass
class AppendEntriesRequest:
    term:          int
    leader_id:     str
    prev_log_index: int
    prev_log_term:  int
    entries:       List[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    term:    int
    success: bool
    node_id: str
    match_index: int = 0



class RaftNode:


    ELECTION_TIMEOUT_MIN = 1.5  
    ELECTION_TIMEOUT_MAX = 3.0   
    HEARTBEAT_INTERVAL   = 0.5   

    def __init__(self, node_id: str, peers: List[str]):
        self.node_id   = node_id
        self.peers     = peers          
        self.cluster   = {}            

        self.current_term: int         = 0
        self.voted_for:    Optional[str] = None
        self.log:          List[LogEntry] = []   

        self.commit_index: int = 0   
        self.last_applied: int = 0  

        self.next_index:  Dict[str, int] = {}   
        self.match_index: Dict[str, int] = {}   

        self.state:          NodeState     = NodeState.FOLLOWER
        self.leader_id:      Optional[str] = None
        self.votes_received: int           = 0
        self.last_heartbeat: float         = time.time()
        self.election_timeout: float       = self._new_election_timeout()

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
            self.voted_for    = None
            self.state        = NodeState.FOLLOWER
            self.leader_id    = None
            self.logger.info(f"Term updated → {new_term}, reverted to FOLLOWER")


    async def _start_election(self):
        self.current_term += 1
        self.state          = NodeState.CANDIDATE
        self.voted_for      = self.node_id
        self.votes_received = 1
        self.election_timeout = self._new_election_timeout()
        self.last_heartbeat   = time.time()

        self.logger.info(
            f"🗳  Starting election | term={self.current_term} | "
            f"need={self._quorum()} votes"
        )

        req = VoteRequest(
            term           = self.current_term,
            candidate_id   = self.node_id,
            last_log_index = self.last_log_index,
            last_log_term  = self.last_log_term,
        )

        tasks = [self._request_vote(peer_id, req) for peer_id in self.peers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _request_vote(self, peer_id: str, req: VoteRequest):
        peer = self.cluster.get(peer_id)
        if peer is None:
            return
        resp = await peer.handle_vote_request(req)
        await self._handle_vote_response(resp)

    async def handle_vote_request(self, req: VoteRequest) -> VoteResponse:
        self._update_term(req.term)

        if req.term < self.current_term:
            return VoteResponse(self.current_term, False, self.node_id)

        already_voted = (
            self.voted_for is not None and
            self.voted_for != req.candidate_id
        )
        if already_voted:
            return VoteResponse(self.current_term, False, self.node_id)

        log_ok = (
            req.last_log_term  > self.last_log_term or
            (req.last_log_term == self.last_log_term and
             req.last_log_index >= self.last_log_index)
        )
        if not log_ok:
            return VoteResponse(self.current_term, False, self.node_id)

        self.voted_for    = req.candidate_id
        self.last_heartbeat = time.time()
        self.logger.info(f"✅ Voted for {req.candidate_id} in term {req.term}")
        return VoteResponse(self.current_term, True, self.node_id)

    async def _handle_vote_response(self, resp: VoteResponse):
        self._update_term(resp.term)
        if self.state != NodeState.CANDIDATE:
            return
        if resp.vote_granted:
            self.votes_received += 1
            self.logger.info(
                f"Vote from {resp.voter_id} | total={self.votes_received}/{self._quorum()}"
            )
            if self.votes_received >= self._quorum():
                await self._become_leader()

    async def _become_leader(self):
        self.state     = NodeState.LEADER
        self.leader_id = self.node_id
        self.logger.info(f"👑 BECAME LEADER for term {self.current_term}")

        for peer_id in self.peers:
            self.next_index[peer_id]  = self.last_log_index + 1
            self.match_index[peer_id] = 0

        await self._broadcast_append_entries()


    async def client_request(self, command: Any) -> bool:
 
        if self.state != NodeState.LEADER:
            self.logger.warning(f"Not leader! Redirect to {self.leader_id}")
            return False

        entry = LogEntry(
            term    = self.current_term,
            index   = self.last_log_index + 1,
            command = command,
        )
        self.log.append(entry)
        self.logger.info(f"📝 Appended to log: {entry}")

        await self._broadcast_append_entries()
        return True

    async def _broadcast_append_entries(self):
        if self.state != NodeState.LEADER:
            return
        tasks = [self._send_append_entries(peer_id) for peer_id in self.peers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_append_entries(self, peer_id: str):
        peer = self.cluster.get(peer_id)
        if peer is None:
            return

        next_idx   = self.next_index.get(peer_id, 1)
        prev_index = next_idx - 1
        prev_term  = 0
        if prev_index > 0 and prev_index <= len(self.log):
            prev_term = self.log[prev_index - 1].term

        entries = self.log[next_idx - 1:] if next_idx <= len(self.log) else []

        req = AppendEntriesRequest(
            term           = self.current_term,
            leader_id      = self.node_id,
            prev_log_index = prev_index,
            prev_log_term  = prev_term,
            entries        = entries,
            leader_commit  = self.commit_index,
        )
        resp = await peer.handle_append_entries(req)
        await self._handle_append_response(peer_id, resp, len(entries))

    async def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        self._update_term(req.term)

        if req.term < self.current_term:
            return AppendEntriesResponse(self.current_term, False, self.node_id)

        self.last_heartbeat = time.time()
        self.leader_id      = req.leader_id
        if self.state != NodeState.FOLLOWER:
            self.state = NodeState.FOLLOWER

        if req.prev_log_index > 0:
            if req.prev_log_index > len(self.log):
                return AppendEntriesResponse(self.current_term, False, self.node_id)
            local_term = self.log[req.prev_log_index - 1].term
            if local_term != req.prev_log_term:
                self.log = self.log[:req.prev_log_index - 1]
                return AppendEntriesResponse(self.current_term, False, self.node_id)

        for entry in req.entries:
            idx = entry.index - 1   
            if idx < len(self.log):
                if self.log[idx].term != entry.term:
                    self.log = self.log[:idx]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, self.last_log_index)
            await self._apply_committed()

        return AppendEntriesResponse(
            self.current_term, True, self.node_id,
            match_index=self.last_log_index
        )

    async def _handle_append_response(self, peer_id: str, resp: AppendEntriesResponse, sent: int):
        self._update_term(resp.term)
        if self.state != NodeState.LEADER:
            return

        if resp.success:
            self.match_index[peer_id] = resp.match_index
            self.next_index[peer_id]  = resp.match_index + 1
            await self._advance_commit_index()
        else:
            self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)

    async def _advance_commit_index(self):
   
        for n in range(self.last_log_index, self.commit_index, -1):
            if self.log[n - 1].term != self.current_term:
                continue
            replicated = 1 + sum(
                1 for m in self.match_index.values() if m >= n
            )
            if replicated >= self._quorum():
                self.commit_index = n
                self.logger.info(f"📌 Commit index advanced → {n}")
                await self._apply_committed()
                break

    async def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            entry.committed = True
            self._apply_to_state_machine(entry.command)
            self.logger.info(f"⚙️  Applied: {entry}")

    def _apply_to_state_machine(self, command: Any):
        if isinstance(command, dict):
            op = command.get("op")
            if op == "set":
                self.state_machine[command["key"]] = command["value"]
            elif op == "del":
                self.state_machine.pop(command["key"], None)


    async def run(self):
        self._running = True
        self.logger.info(f"🚀 Node started | peers={self.peers}")
        while self._running:
            now = time.time()
            if self.state == NodeState.LEADER:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                await self._broadcast_append_entries()
            else:
                elapsed = now - self.last_heartbeat
                if elapsed >= self.election_timeout:
                    await self._start_election()
                else:
                    await asyncio.sleep(0.1)

    def stop(self):
        self._running = False


    def status(self) -> dict:
        return {
            "node_id":      self.node_id,
            "state":        self.state.value,
            "term":         self.current_term,
            "leader":       self.leader_id,
            "log_length":   len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "state_machine": self.state_machine,
            "log":          [repr(e) for e in self.log],
        }

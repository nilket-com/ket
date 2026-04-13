8. Sequence Diagram — Write with Proxying, Finalization & E_safe
Code


Copy
Client              Follower-F1          Leader-L             Follower-F2
  │                      │                  │                      │
  │──[HRFT frame]────────▶                  │                      │
  │  FrameHeader{                           │                      │
  │    msg=WRITE_REQ}     │                  │                      │
  │  WriteRequestMeta{    │                  │                      │
  │    proxy_hop=0,       │                  │                      │
  │    expected_epoch=5}  │                  │                      │
  │  Arrow: {key,value}   │                  │                      │
  │                      │                  │                      │
  │              F1: I'm not leader          │                      │
  │                      │──[HRFT frame]───▶│                      │
  │                      │  WriteRequestMeta│                      │
  │                      │  { proxy_hop=1,  │                      │
  │                      │    proxy_source= │                      │
  │                      │    F1.node_id }  │                      │
  │                      │  Arrow: same     │                      │
  │                      │  (zero-copy fwd) │                      │
  │                      │                  │                      │
  │                      │                  │── bytemuck::cast_ref  │
  │                      │                  │   → WriteRequestMeta  │
  │                      │                  │   (zero deser)        │
  │                      │                  │                      │
  │                      │                  │── Idempotency Cache ──│
  │                      │                  │   HashMap lookup:     │
  │                      │                  │   (client_uuid,       │
  │                      │                  │    sequence_no) → MISS│
  │                      │                  │                      │
  │                      │                  │── HLC Merge ──────────│
  │                      │                  │   leader_hlc =        │
  │                      │                  │   max(L_hlc,          │
  │                      │                  │       client_hlc) + 1 │
  │                      │                  │                      │
  │                      │                  │── Append to Raft Log  │
  │                      │                  │   index=1042, term=7  │
  │                      │                  │   epoch=5             │
  │                      │                  │                      │
  │                      │                  │──AppendEntries───────▶│
  │                      │                  │                  (F2) │
  │                      │                  │◀──────────Ack─────────│
  │                      │                  │                      │
  │                      │                  │──AppendEntries───────▶│
  │                      │◀─────────────────│    (to F1)           │
  │                      │──────Ack────────▶│                      │
  │                      │                  │                      │
  │                      │                  │── Quorum Achieved ────│
  │                      │                  │   commit_index = 1042 │
  │                      │                  │                      │
  │                      │                  │── Cache Insert ───────│
  │                      │                  │   cache.insert(       │
  │                      │                  │     write_id,         │
  │                      │                  │     WriteResponseMeta)│
  │                      │                  │                      │
  │                      │                  │── Emit Finalization ──│
  │                      │                  │   FinalizationMarker {│
  │                      │                  │     log_index: 1042,  │
  │                      │                  │     epoch: 5,         │
  │                      │                  │     e_safe: 1042,     │
  │                      │                  │     commit_hlc: ...,  │
  │                      │                  │     payload_hash: ... │
  │                      │                  │   }                   │
  │                      │                  │                      │
  │                      │                  │── Push to stream_fin  │
  │                      │                  │   subscribers via     │
  │                      │                  │   mpsc::Sender        │
  │                      │                  │                      │
  │                      │◀─[HRFT frame]────│                      │
  │                      │  WRITE_RESP meta │                      │
  │                      │  {status=OK,     │                      │
  │                      │   log_index=1042,│                      │
  │                      │   has_final=1}   │                      │
  │                      │  trailing:       │                      │
  │                      │  FinalizationMkr │                      │
  │                      │                  │                      │
  │◀─[HRFT frame]────────│                  │                      │
  │  (F1 proxies back    │                  │                      │
  │   same bytes,        │                  │                      │
  │   zero-copy)         │                  │                      │
  │                      │                  │                      │
  │                      │                  │── E_safe Propagation──│
  │                      │                  │   (via AppendEntries  │
  │                      │                  │    commit_index field)│
  │                      │                  │──────────────────────▶│
  │                      │◀─────────────────│   F1.e_safe = 1042   │
  │                      │                  │   F2.e_safe = 1042   │
  ▼                      ▼                  ▼                      ▼


  ═══ DUPLICATE WRITE (client retry) ═══

Client              Leader-L
  │                      │
  │──[HRFT frame]────────▶
  │  same write_id       │
  │                      │── bytemuck::cast_ref → 0ns
  │                      │── cache.get(write_id) → HIT
  │                      │
  │◀─[HRFT frame]────────│
  │  WriteResponseMeta { │
  │    status=DUPLICATE, │
  │    (original response│
  │     replayed from    │
  │     cache, bit-exact)│
  │  }                   │
  ▼                      ▼


  ═══ E_SAFE READ ENFORCEMENT ═══

Client              Follower-F1
  │                      │
  │──[HRFT frame]────────▶
  │  READ_REQ            │
  │  { consistency=      │
  │    LINEARIZABLE,     │
  │    min_hlc=T42 }     │
  │  Arrow: {key}        │
  │                      │
  │                      │── Check:
  │                      │   F1.e_safe_index >= needed?
  │                      │   1040 < 1042 → FAIL
  │                      │
  │◀─[HRFT frame]────────│
  │  READ_RESP           │
  │  { status=           │
  │    E_SAFE_BEHIND,    │
  │    leader_hint=L }   │
  │                      │
  │   Client follows leader_hint → retries at L
  ▼                      ▼

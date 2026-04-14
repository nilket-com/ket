# Configuration Schema

Wendelon nodes are configured via a static TOML file (e.g., `wendelon.toml`) and environment variables. 

## 1. TOML Structure

```toml
[node]
# The numeric ID of this node. If missing, it is generated at bootstrap.
id = 42
# Roles: ["data"], ["control"], or ["data", "control"]
roles = ["data", "control"]

[network]
# Control ring (Raft) bind address
control_bind = "0.0.0.0:4002"
# Data ring bind address
data_bind = "0.0.0.0:4001"
# Path to mTLS certificates
cert_path = "/etc/wendelon/certs/node.pem"
key_path = "/etc/wendelon/certs/node.key"

[storage]
# Path to the fast NVMe volume for active slabs and WAL
data_dir = "/mnt/nvme0/wendelon"
# The threshold to seal a slab (e.g., 1GB)
slab_seal_bytes = 1073741824
# The chunk size for L2 HTTP Range GETs (e.g., 16MB)
chunk_size_bytes = 16777216

[tiering]
# Optional L2 Object Storage credentials
s3_bucket = "nilket-cold-tier"
s3_region = "us-east-1"
# Upload slabs when NVMe is over this capacity percentage
evict_threshold_percent = 70

[tuning]
# How many virtual nodes per physical node
vnode_count = 256
# Default replication factor (number of distinct physical nodes per hash range)
replication_factor = 3
# The hashing algorithm (e.g., "highwayhash", "blake3", "rapidhash"). Used during bootstrap only.
hash_algo = "highwayhash"
# Raft heartbeat interval in milliseconds
heartbeat_interval_ms = 150
# Duration of a per-range lease in milliseconds
lease_duration_ms = 1500
# Percentage of dead keys needed to trigger E_safe compaction
compaction_threshold_percent = 50
# Max frame size in bytes (default 5 MiB)
max_frame_size = 5242880
# Depth of the io_uring submission queue
io_uring_queue_depth = 1024
```

## 2. Environment Variable Overrides
Every TOML configuration can be overridden by an environment variable prefixed with `WENDELON_`.
For example, `WENDELON_NETWORK_DATA_BIND="0.0.0.0:8080"` overrides the `[network] data_bind` field.
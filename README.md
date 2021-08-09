# FiosDB

The end goal is to have a fully distributed, fault tolerant and concurrent key value store database.

Phase 1: Single node key value store
- Support CRUD operations
- Allow multiple concurrent requests
- Data is persisted to disk. Recovery from failure (data is reloaded from disk)
- Cache pieces of index, record in memory for performance

Phase 2: Single partition key value store with replication
- Still single partition but data will be replicated to multiple nodes to increase fault tolerance
- Implement such that reads and writes could be routed to any of the nodes
- Implement with each operation requiring consensus - PAXOS or RAFT
- Implement with leader election where all requests are routed to the leader
- Implement with offloading leader election to zookeeper

Phase 3: Key value store with replication and partitioning
- Implement multiple partitions where each partition is a replicated KV store
- Implement repartitioning after adding and removing nodes (consistent hashing, rendezvous hashing)?

Phase 4: Multitenant, partitioned, replicated key value store
- Allow multiple different clients, each with access to only their own data
- Design/implement ways to keep track of clients + have authentication mechanism to protect clients data from each other
- Implement throttling (distributed throttling) to prevent a single client from consuming all of the capacity. Each client can have a configured amount of capacity 


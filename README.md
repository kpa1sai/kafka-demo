# ⚡ Kafka ISR & Leader Election Interactive Demo

This project is a self-contained, interactive sandbox designed to demonstrate core Apache Kafka reliability concepts:
1.  **In-Sync Replicas (ISR)**: The subset of replica brokers that are actively caught up with the leader.
2.  **Leader Election**: How Kafka automatically elects a new partition leader when the active leader fails.
3.  **Min In-Sync Replicas (`min.insync.replicas`)**: The threshold configuration determining how many replica acknowledgments are required to accept a write when `acks=all`.
4.  **Consistency vs. Availability Trade-off**: Visualizing write failures when the ISR drops below the minimum required replicas.

---

## 🛠️ Architecture & Setup

The demo consists of:
*   **3 Kafka Brokers** running in KRaft mode (no Zookeeper).
*   **A Continuous Producer** that publishes message streams with `acks=all`.
*   **A Custom Live Dashboard** showing the active cluster topology, partition leaders, replicas, and ISR status in real-time.

### Prerequisites
Make sure you have **Docker Desktop** installed and running on your system.

### Starting the Cluster

To start the demo, run the following command in your terminal:
```bash
docker compose up --build
```
This builds and starts the 3 Kafka brokers, the producer, and the dashboard.

---

## 🖥️ Real-Time Dashboard

To view the interactive monitoring dashboard in a dedicated terminal window:
```bash
docker compose attach dashboard
```
*(Or simply watch the docker compose logs, though attaching gives you the full-screen terminal UI.)*

The dashboard displays:
*   **Active Brokers**: Green or red indicators showing which brokers are alive.
*   **Partition State**: Current Leader, Replicas, and In-Sync Replicas (ISR) list.
*   **Health Status**: `HEALTHY` (all replicas in sync), `UNDER-REPLICATED` (some replicas offline), or `OFFLINE` (partition unreachable).

---

## 🎮 Interactive Demo Walkthrough (LinkedIn Video Script)

Follow these steps to record a perfect LinkedIn demo showing off your Kafka internals knowledge.

### 📍 Phase 1: The Healthy State
1.  Verify the cluster starts up and the dashboard shows all three brokers as green.
2.  Verify the topic `demo-topic` is automatically created with:
    *   **Replicas**: `[1, 2, 3]`
    *   **ISR**: `[1, 2, 3]`
    *   **Status**: `HEALTHY ✅`
3.  Observe the producer logs. It should output:
    `🟢 [Success] Sent msg #0005 -> partition 0 @ offset 4`
    *(Since `acks=all`, the write is safely replicated to all 3 brokers before acknowledging).*

---

### 📍 Phase 2: Broker Failure & Clean Leader Election
*Goal: Show how Kafka automatically handles a broker failure without losing write availability.*

1.  Identify the current partition leader from the dashboard (e.g., Broker `1`).
2.  Kill the leader broker by running:
    ```bash
    docker compose stop kafka-1
    ```
3.  **Observe the Dashboard**:
    *   Broker `1` turns red (offline).
    *   Kafka instantly triggers a leader election. A new leader (e.g., Broker `2`) is elected.
    *   The ISR list shrinks to `[2, 3]`.
    *   Topic status changes to `UNDER-REPLICATED ⚠️`.
4.  **Observe the Producer**:
    *   Writes **continue to succeed**!
    *   Since `min.insync.replicas=2`, and both Broker `2` and `3` are alive and in-sync, the producer receives the required acknowledgements.

---

### 📍 Phase 3: Min In-Sync Replicas Violation (Consistency over Availability)
*Goal: Show what happens when write availability is blocked to prevent data loss.*

1.  Kill a second broker (e.g., Broker `2`) to drop the active broker count to 1:
    ```bash
    docker compose stop kafka-2
    ```
2.  **Observe the Dashboard**:
    *   Broker `2` turns red.
    *   The ISR list shrinks to `[3]` (only Broker 3 remains).
    *   Topic status is still `UNDER-REPLICATED ⚠️`.
3.  **Observe the Producer**:
    *   Writes **begin to fail** with `NotEnoughReplicasException`!
    *   *Why?* The producer is configured with `acks=all` (requiring acknowledgment from all in-sync replicas), and the topic requires a minimum of **2** replicas in sync (`min.insync.replicas=2`). Since the ISR only contains `[3]` (size 1), Kafka rejects the writes to guarantee durability, rather than risking writing to a single node.

---

### 📍 Phase 4: Self-Healing & Catching Up
*Goal: Show how replicas catch up, rejoin the ISR, and writes resume automatically.*

1.  Recover the second broker:
    ```bash
    docker compose start kafka-2
    ```
2.  **Observe the Dashboard & Producer**:
    *   Broker `2` starts up and syncs the missed offsets from Broker `3`.
    *   Once fully caught up, Broker `2` automatically rejoins the ISR.
    *   The ISR grows back to `[2, 3]` (size 2).
    *   **The producer immediately and automatically resumes writing successfully!** (No restart or manual intervention needed).

---

### 📍 Phase 5: Complete Recovery & Leader Rebalancing
1.  Recover the final broker:
    ```bash
    docker compose start kafka-1
    ```
2.  **Observe**:
    *   Broker `1` rejoins the ISR.
    *   The ISR list returns to `[1, 2, 3]`.
    *   Topic status transitions back to `HEALTHY ✅`.
    *   Kafka may automatically rebalance the leader back to Broker `1` (its preferred leader) if configured, or you can trigger it manually.

---

## ✍️ Draft LinkedIn Post for Your Demo

Here is a ready-to-use template for your LinkedIn post:

***

### 🚀 Demystifying Kafka Internals: ISR, Leader Election, and Min In-Sync Replicas in Action!

Have you ever wondered how Apache Kafka balances **High Availability** and **Data Consistency** during broker failures?

I built a local 3-broker KRaft cluster sandbox to replicate real-world node outages and visually demonstrate how Kafka handles replication under the hood.

Here’s a breakdown of what happens in the video:

1️⃣ **The Healthy State**: The partition leader replicates messages to all followers. The In-Sync Replicas (ISR) list is `[1, 2, 3]`. With `acks=all`, every write is acknowledged only after reaching all three.
2️⃣ **Clean Leader Election**: I stop the leader (Broker 1). Kafka instantly detects this, elects Broker 2 as the new leader, and shrinks the ISR to `[2, 3]`. Writes keep succeeding because our `min.insync.replicas` is configured to `2`.
3️⃣ **Consistency Protection**: I stop Broker 2. The ISR drops to `[3]` (size 1). Since our cluster requires a minimum of 2 in-sync replicas to guarantee durability, Kafka immediately rejects new writes with a `NotEnoughReplicasException`. It chooses **Consistency** over **Availability**!
4️⃣ **Self-Healing**: I restart Broker 2. It catches up on missed offsets, rejoins the ISR list, and writes resume automatically without any manual intervention.

**💡 Key Takeaway**:
Understanding the relationship between `acks=all`, `min.insync.replicas`, and replication factor is critical for designing reliable Kafka pipelines. A misconfiguration can lead to silent data loss or unnecessary write outages.

Repo link to run this yourself: [Insert Your Repo URL]

#apachekafka #systemdesign #softwareengineering #devops #backend

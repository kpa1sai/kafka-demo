import os
import time
import sys
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaError
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text
from rich import box

# Configuration
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
TOPIC_NAME = 'demo-topic'
REFRESH_INTERVAL = 1.0 # seconds

console = Console()

def get_admin_client():
    """Attempts to connect to Kafka and return an AdminClient."""
    try:
        return KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='monitor-dashboard',
            request_timeout_ms=3000
        )
    except NoBrokersAvailable:
        return None
    except Exception:
        return None

def generate_layout(cluster_info, topic_info, error_msg=None) -> Layout:
    """Generates the Rich layout for the terminal dashboard."""
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main", ratio=1),
        Layout(name="footer", size=3)
    )

    # 1. Header
    header_text = Text("⚡ KAFKA ISR & LEADER ELECTION REAL-TIME DASHBOARD ⚡", style="bold cyan justify=center")
    layout["header"].update(Panel(header_text, box=box.ROUNDED, style="on blue"))

    # 2. Main Area (splits into Cluster Status and Topic Status)
    layout["main"].split_row(
        Layout(name="cluster_panel", ratio=1),
        Layout(name="topic_panel", ratio=2)
    )

    # 2a. Cluster Panel Content
    if cluster_info:
        cluster_id = cluster_info.get('cluster_id', 'Unknown')
        controller = cluster_info.get('controller')
        controller_id = controller.nodeId if controller else 'None'
        active_brokers = sorted([b.nodeId for b in cluster_info.get('brokers', [])])
        
        cluster_table = Table(show_header=False, box=box.SIMPLE)
        cluster_table.add_row("Cluster ID:", Text(str(cluster_id), style="green"))
        cluster_table.add_row("Active Controller ID:", Text(str(controller_id), style="yellow bold"))
        
        # We know our static cluster has IDs 1, 2, and 3
        brokers_text = Text()
        for b_id in [1, 2, 3]:
            if b_id in active_brokers:
                brokers_text.append(f" [Broker {b_id}] ", style="bold green on black")
            else:
                brokers_text.append(f" [Broker {b_id}] ", style="bold red on black strikethrough")
        
        cluster_table.add_row("Active Brokers:", brokers_text)
        
        cluster_panel_content = cluster_table
    else:
        cluster_panel_content = Text("Connecting to Kafka Cluster...", style="yellow italic")

    layout["main"]["cluster_panel"].update(
        Panel(cluster_panel_content, title="🌐 Cluster Topology", border_style="blue")
    )

    # 2b. Topic Panel Content
    if error_msg:
        topic_panel_content = Text(f"\nError: {error_msg}", style="bold red justify=center")
    elif not topic_info:
        topic_panel_content = Text(
            f"\nWaiting for topic '{TOPIC_NAME}' to be created...\n\n"
            f"Run: docker compose exec kafka-1 kafka-topics.sh --create --topic {TOPIC_NAME} ...",
            style="yellow italic justify=center"
        )
    else:
        # Table of partition info
        topic_table = Table(box=box.MINIMAL_DOUBLE_HEAD)
        topic_table.add_column("Partition", justify="center")
        topic_table.add_column("Leader", justify="center")
        topic_table.add_column("Replicas (Preferred)", justify="center")
        topic_table.add_column("In-Sync Replicas (ISR)", justify="center")
        topic_table.add_column("Status", justify="center")

        partitions = topic_info.get('partitions', [])
        for part in sorted(partitions, key=lambda x: x.get('partition')):
            part_id = part.get('partition')
            leader = part.get('leader')
            replicas = sorted(part.get('replicas', []))
            isr = sorted(part.get('isr', []))
            
            # Formatting replicas
            rep_strs = []
            for r in replicas:
                if r == leader:
                    rep_strs.append(f"[yellow bold]*{r}*[/yellow bold]")
                else:
                    rep_strs.append(str(r))
            rep_display = f"[{', '.join(rep_strs)}]"

            # Formatting ISR
            isr_display = f"[{', '.join(str(i) for i in isr)}]"
            
            # Status Logic
            if leader == -1 or not isr:
                status = Text("OFFLINE ⚠️", style="bold red blink")
            elif len(isr) < len(replicas):
                status = Text("UNDER-REPLICATED ⚠️", style="bold yellow")
            else:
                status = Text("HEALTHY ✅", style="bold green")

            # Leader formatting
            leader_display = Text(str(leader), style="bold yellow" if leader != -1 else "bold red")

            topic_table.add_row(
                str(part_id),
                leader_display,
                rep_display,
                isr_display,
                status
            )
        
        # Add min.insync.replicas information if available (assume 2 for this demo)
        topic_panel_content = Layout()
        topic_panel_content.split_column(
            Layout(topic_table, ratio=1),
            Layout(Text(f"Target: Replication Factor = 3 | min.insync.replicas = 2", style="dim cyan"), size=1)
        )

    layout["main"]["topic_panel"].update(
        Panel(topic_panel_content, title=f"📋 Topic: {TOPIC_NAME}", border_style="cyan")
    )

    # 3. Footer
    footer_text = Text()
    if error_msg or not cluster_info:
        footer_text.append("🔄 Reconnecting...", style="bold yellow")
    else:
        footer_text.append("🟢 Connected | ", style="green")
        footer_text.append("Simulations:\n", style="bold white")
        footer_text.append("  - Stop leader: docker compose stop kafka-<leader_id>\n", style="dim")
        footer_text.append("  - Stop second node (fails min ISR): docker compose stop kafka-<node_id>", style="dim")
    
    layout["footer"].update(Panel(footer_text, box=box.ROUNDED, border_style="dim"))

    return layout

def main():
    admin = None
    last_error_time = 0
    
    with Live(generate_layout(None, None), screen=True, refresh_per_second=4) as live:
        while True:
            try:
                if not admin:
                    admin = get_admin_client()
                    if not admin:
                        live.update(generate_layout(None, None, "Could not connect to Kafka bootstrap servers."))
                        time.sleep(2)
                        continue

                # Fetch cluster details
                try:
                    cluster_info_raw = admin.describe_cluster()
                    cluster_info = {
                        'cluster_id': cluster_info_raw.cluster_id,
                        'controller': cluster_info_raw.controller,
                        'brokers': cluster_info_raw.brokers
                    }
                except KafkaError as ke:
                    # Connection might be stale, trigger reconnect
                    admin.close()
                    admin = None
                    raise ke

                # Fetch topic details
                topic_info = None
                try:
                    topics_metadata = admin.describe_topics([TOPIC_NAME])
                    if topics_metadata:
                        raw_topic = topics_metadata[0]
                        # Convert kafka-python partition objects to lists of dicts
                        partitions = []
                        for p in raw_topic.get('partitions', []):
                            partitions.append({
                                'partition': p.partition,
                                'leader': p.leader,
                                'replicas': p.replicas,
                                'isr': p.isr
                            })
                        topic_info = {
                            'topic': raw_topic.get('topic'),
                            'partitions': partitions
                        }
                except KafkaError:
                    # Topic might not exist yet, which is fine
                    pass

                live.update(generate_layout(cluster_info, topic_info))
                
            except Exception as e:
                live.update(generate_layout(None, None, f"Exception: {str(e)}"))
                if admin:
                    try:
                        admin.close()
                    except Exception:
                        pass
                    admin = None
                time.sleep(2)
                
            time.sleep(REFRESH_INTERVAL)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting...")
        sys.exit(0)

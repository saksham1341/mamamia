import asyncio
import time
import statistics
import argparse
import uvicorn
import json
import os
from datetime import datetime
from mamamia.server.api import app
from mamamia.client.producer import ProducerClient
from mamamia.client.consumer import ConsumerClient
from mamamia.client.transport import HttpTransport, TcpTransport


async def run_producer_bench(
    url_or_host, log_id, count, producer_id, transport_type="http"
):
    if transport_type == "http":
        transport = HttpTransport(url_or_host)
    else:
        host, port = url_or_host.split(":")
        transport = TcpTransport(host, int(port))

    producer = ProducerClient(transport, log_id)
    start_time = time.perf_counter()

    for i in range(count):
        await producer.send(
            payload={"data": f"bench-{producer_id}-{i}"}, metadata={"ts": time.time()}
        )

    duration = time.perf_counter() - start_time
    await producer.close()
    return count, duration


async def run_consumer_bench(
    url_or_host, log_id, group_id, target_count, shared_state, transport_type="http"
):
    if transport_type == "http":
        transport = HttpTransport(url_or_host)
    else:
        host, port = url_or_host.split(":")
        transport = TcpTransport(host, int(port))

    consumer = ConsumerClient(transport, log_id, group_id)
    start_time = time.perf_counter()
    processed_locally = 0
    latencies = []

    while shared_state["processed"] < target_count:
        msg = await consumer.acquire_next(duration=30.0)
        if not msg:
            if shared_state["processed"] >= target_count:
                break
            await asyncio.sleep(0.1)
            continue

        await consumer.settle(msg["id"], success=True)
        shared_state["processed"] += 1
        processed_locally += 1
        if "ts" in (msg.get("metadata") or {}):
            latencies.append(time.time() - msg["metadata"]["ts"])

        if shared_state["processed"] >= target_count:
            break

    duration = time.perf_counter() - start_time
    await consumer.close()
    return processed_locally, duration, latencies


async def run_scenario(url_or_host, scenario, internal_server=False):
    server_task = None
    server = None
    transport_type = scenario.get("transport", "http")

    if internal_server:
        # For internal server, we use 8002 for HTTP and 9000 (default) for TCP
        config = uvicorn.Config(app, host="127.0.0.1", port=8002, log_level="error")
        server = uvicorn.Server(config)
        server_task = asyncio.create_task(server.serve())
        await asyncio.sleep(1)
        if transport_type == "http":
            url_or_host = "http://localhost:8002"
        else:
            url_or_host = "localhost:9000"

    log_id = f"bench-{int(time.time())}"
    group_id = "bench-group"
    msgs = scenario["msgs"]
    producers = scenario["producers"]
    consumers = scenario["consumers"]

    start_bench = time.perf_counter()

    msgs_per_producer = msgs // producers
    producer_tasks = []
    for i in range(producers):
        count = msgs_per_producer + (1 if i < msgs % producers else 0)
        producer_tasks.append(
            run_producer_bench(url_or_host, log_id, count, i, transport_type)
        )

    shared_state = {"processed": 0}
    consumer_tasks = []
    for i in range(consumers):
        consumer_tasks.append(
            run_consumer_bench(
                url_or_host, log_id, group_id, msgs, shared_state, transport_type
            )
        )

    results = await asyncio.gather(
        asyncio.gather(*producer_tasks), asyncio.gather(*consumer_tasks)
    )

    total_duration = time.perf_counter() - start_bench
    p_results, c_results = results

    all_latencies = []
    for r in c_results:
        all_latencies.extend(r[2])

    p_throughput = msgs / total_duration
    c_throughput = msgs / total_duration

    metrics = {
        "name": scenario["name"],
        "transport": transport_type,
        "msgs": msgs,
        "producers": producers,
        "consumers": consumers,
        "duration": total_duration,
        "p_throughput": p_throughput,
        "c_throughput": c_throughput,
        "avg_latency": statistics.mean(all_latencies) * 1000 if all_latencies else 0,
        "p95_latency": statistics.quantiles(all_latencies, n=20)[18] * 1000
        if len(all_latencies) > 1
        else 0,
    }

    if server_task and server:
        server.should_exit = True
        await server_task

    return metrics


def generate_html_report(results, output_path):
    rows = ""
    for r in results:
        rows += f"""
        <tr>
            <td>{r["name"]}</td>
            <td>{r["transport"]}</td>
            <td>{r["msgs"]}</td>
            <td>{r["producers"]}</td>
            <td>{r["consumers"]}</td>
            <td>{r["p_throughput"]:.2f}</td>
            <td>{r["c_throughput"]:.2f}</td>
            <td>{r["avg_latency"]:.2f}ms</td>
            <td>{r["p95_latency"]:.2f}ms</td>
        </tr>
        """

    html = f"""
    <html>
    <head>
        <title>Mamamia Benchmark Report</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 40px; background-color: #f4f7f6; color: #333; }}
            .container {{ max-width: 1200px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background-color: #3498db; color: white; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            tr:hover {{ background-color: #f1f1f1; }}
            .header-info {{ margin-bottom: 20px; font-style: italic; color: #666; }}
            .badge {{ padding: 4px 8px; border-radius: 4px; font-size: 0.9em; font-weight: bold; }}
            .badge-http {{ background-color: #e67e22; color: white; }}
            .badge-tcp {{ background-color: #27ae60; color: white; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Mamamia Performance Report</h1>
            <p class="header-info">Generated at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            <table>
                <thead>
                    <tr>
                        <th>Scenario</th>
                        <th>Transport</th>
                        <th>Messages</th>
                        <th>Producers</th>
                        <th>Consumers</th>
                        <th>Prod TPS</th>
                        <th>Cons TPS</th>
                        <th>Avg Latency</th>
                        <th>P95 Latency</th>
                    </tr>
                </thead>
                <tbody>
                    {rows.replace("<td>http</td>", '<td><span class="badge badge-http">HTTP</span></td>').replace("<td>tcp</td>", '<td><span class="badge badge-tcp">TCP</span></td>')}
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """
    with open(output_path, "w") as f:
        f.write(html)


def print_cli_report(results):
    print("\n" + "=" * 90)
    print(
        f"{'Scenario':<25} | {'Protocol':<8} | {'TPS':<10} | {'Avg Lat':<10} | {'P95 Lat':<10}"
    )
    print("-" * 90)
    for r in results:
        print(
            f"{r['name']:<25} | {r['transport']:<8} | {r['c_throughput']:<10.2f} | {r['avg_latency']:<10.2f} | {r['p95_latency']:<10.2f}"
        )
    print("=" * 90 + "\n")


async def main():
    parser = argparse.ArgumentParser(description="Mamamia Benchmarking Suite")
    parser.add_argument(
        "--config", default="benchmarks/default_config.json", help="Path to config file"
    )
    parser.add_argument(
        "--url", default="http://localhost:8002", help="Server URL (if not internal)"
    )
    parser.add_argument(
        "--internal-server", action="store_true", help="Use internal server"
    )

    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Error: Config file {args.config} not found.")
        return

    with open(args.config, "r") as f:
        config = json.load(f)

    results = []
    for scenario in config["scenarios"]:
        print(
            f"Running scenario: {scenario['name']} ({scenario.get('transport', 'http')})..."
        )
        metrics = await run_scenario(args.url, scenario, args.internal_server)
        results.append(metrics)

    if config.get("output", {}).get("cli", True):
        print_cli_report(results)

    html_path = config.get("output", {}).get("html")
    if html_path:
        generate_html_report(results, html_path)
        print(f"HTML report generated at: {html_path}")


if __name__ == "__main__":
    asyncio.run(main())

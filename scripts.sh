#!/bin/bash

SCREEN_SESSION="paxos_rust"

# Utility commands
kill_all_screen_session() {
    screen -ls | awk '/[0-9]+\./ {print $1}' | xargs -r -I{} screen -S {} -X quit
}

create_screen_session() {
    if ! screen -list | grep -q "$SCREEN_SESSION"; then
        screen -dmS "$SCREEN_SESSION"
    fi
}

start_terminal() {
    local command="$1"
    local window_name="$2"
    
    echo "Starting terminal with command: ${command}"

    create_screen_session
    
    screen -S "$SCREEN_SESSION" -X screen -t "$window_name" bash -c "$command"
}

validate_config() {
    local path="${1:-./etc/config.json}"

    echo "Checking configuration: $path"

    if ! command -v jq &>/dev/null; then
        echo "Error: jq is required but not installed."
        exit 1
    fi

    if [[ ! -f "$path" ]]; then
        echo "Error: Config file '$path' not found."
        exit 1
    fi

    local shard_count
    local parity_count
    local node_count

    shard_count=$(jq '.storage.shard_count' "$path")
    parity_count=$(jq '.storage.parity_count' "$path")
    node_count=$(jq '.nodes | length' "$path")

    if ! [[ "$shard_count" =~ ^[0-9]+$ ]] || ! [[ "$parity_count" =~ ^[0-9]+$ ]]; then
        echo "Error: shard_count and parity_count must be valid integers."
        exit 1
    fi

    local expected_count=$((shard_count + parity_count))
    if [[ "$node_count" -ne "$expected_count" ]]; then
        echo "Error: Number of nodes ($node_count) does not match shard_count + parity_count ($expected_count)."
        exit 1
    fi

    local unique_nodes
    unique_nodes=$(jq -r '.nodes[] | "\(.ip):\(.port)"' "$path" | sort | uniq | wc -l)

    if [[ "$unique_nodes" -ne "$node_count" ]]; then
        echo "Error: Duplicate node addresses found in the configuration."
        exit 1
    fi

    local unique_http_nodes
    unique_http_nodes=$(jq -r '.nodes[] | "\(.ip):\(.http_port)"' "$path" | sort | uniq | wc -l)

    if [[ "$unique_http_nodes" -ne "$node_count" ]]; then
        echo "Error: Duplicate node http addresses found in the configuration."
        exit 1
    fi

    local unique_memcached
    unique_memcached=$(jq -r '.nodes[] | "\(.memcached.ip):\(.memcached.port)"' "$path" | sort | uniq | wc -l)

    if [[ "$unique_memcached" -ne "$node_count" ]]; then
        echo "Error: Duplicate memcached addresses found in the configuration."
        exit 1
    fi

    local unique_rocks_db
    unique_rocks_db=$(jq -r '.nodes[] | .rocks_db.path' "$path" | sort | uniq | wc -l)

    if [[ "$unique_rocks_db" -ne "$node_count" ]]; then
        echo "Error: Duplicate rocks_db paths found in the configuration."
        exit 1
    fi

    echo "Configuration is valid."
}

# Actual commands
run_node() {
    local addr="${1:-127.0.0.1}"
    local port="${2:-8081}"
    local config_file="${3:-./etc/config.json}"

    echo "Starting node on ${addr}:${port}"

    # cargo run -- node ${addr}:${port} ${config_file} > ./log/node_${addr}_${port}.log
    cargo run -- node ${addr}:${port} ${config_file}
}

run_memcached() {
    local config_path="${1:-./etc/config.json}"
    local memory="${2:-64}"
    local PIDS=()

    validate_config "$config_path"

    cleanup_memcached() {
        echo -e "\nStopping Memcached instances..."
        for PID in "${PIDS[@]}"; do
            echo "Killing process $PID"
            kill -9 $PID
        done
        echo "All Memcached instances stopped."
        exit 0
    }
    trap cleanup_memcached SIGINT

    local instance_count
    instance_count=$(jq '.nodes | length' "$config_path")

    for ((i=0; i<instance_count; i++)); do
        local ip
        local port
        ip=$(jq -r ".nodes[$i].memcached.ip" "$config_path")
        port=$(jq -r ".nodes[$i].memcached.port" "$config_path")

        memcached -d -m "$memory" -l "$ip" -p "$port"
        sleep 1

        PID=$(pgrep -f "memcached.*-l $ip -p $port")
        PIDS+=("$PID")

        echo "Started Memcached on $ip:$port with PID $PID"
    done

    while true; do
        sleep 1
    done
}

run_all() {
    local config_path="${1:-./etc/config.json}"

    validate_config "$config_path"

    # Starting memcached
    start_terminal "./scripts.sh run_memcached ${config_path}"
    sleep 1

    # Starting nodes
    local node_count
    node_count=$(jq '.nodes | length' "$config_path")

    for ((i=0; i<node_count; i++)); do
        local addr
        local port

        addr=$(jq -r ".nodes[$i].ip" "$config_path")
        port=$(jq -r ".nodes[$i].port" "$config_path")

        start_terminal "./scripts.sh run_node ${addr} ${port}"
        sleep 1
    done

    echo "All services started."
}

bench_system() {
    echo "Running system benchmark..."
    if [ -n "$1" ]; then
        k6 run -e "$1" ./benchmark/script.js
    else
        echo "Error: No environment variable provided for benchmark, provide with at least BASE_URL=<leader>"
    fi
}

bench_baseline() {
    echo "Running benchmark on etcd for baseline..."
    k6 run ./benchmark/script.js
}

if [ "$1" == "run_node" ]; then
    run_node "${@:2}"
elif [ "$1" == "run_memcached" ]; then
    run_memcached "${@:2}"
elif [ "$1" == "run_all" ]; then
    run_all "${@:2}"
elif [ "$1" == "stop_all" ]; then
    kill_all_screen_session
elif [ "$1" == "bench_system" ]; then
    bench_system "${@:2}"
elif [ "$1" == "bench_baseline" ]; then
    bench_baseline
else
    echo "Usage: $0 {run_memcached|run_node|run_all|stop_all|bench_system|bench_baseline} [args]"
fi

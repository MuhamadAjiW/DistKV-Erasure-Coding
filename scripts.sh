#!/bin/bash

SCREEN_SESSION="paxos_rust"

# Utility commands
stop_all() {
    echo "Attempting to kill all screen sessions related to '$SCREEN_SESSION'..."
    screen -ls | grep "$SCREEN_SESSION" | awk '{print $1}' | xargs -r -I{} screen -S {} -X quit
    echo "Screen session cleanup initiated."
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
    
    screen -S "$SCREEN_SESSION" -X screen -t "$window_name" bash -c "${command}; exec bash"
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

    shard_count=$(jq -r '.storage.shard_count // -1' "$path")
    parity_count=$(jq -r '.storage.parity_count // -1' "$path")
    node_count=$(jq -r '.nodes | length // -1' "$path")

    if ! [[ "$shard_count" =~ ^[0-9]+$ ]] || ! [[ "$parity_count" =~ ^[0-9]+$ ]] || ! [[ "$node_count" =~ ^[0-9]+$ ]]; then
        echo "Error: shard_count, parity_count, or nodes array is missing or invalid in config."
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
    local addr="127.0.0.1:8081"
    local config_file="./etc/config.json"
    local file_output=""
    local trace=""
    local erasure=""

    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --addr)
                addr="$2"
                shift 2
                ;;
            --config_file)
                config_file="$2"
                shift 2
                ;;
            --file_output)
                file_output="--file_output"
                shift 1
                ;;
            --trace)
                trace="--trace"
                shift 1
                ;;
            --erasure)
                erasure="--erasure"
                shift 1
                ;;
            *)
                echo "Unknown option: $1"
                return 1
                ;;
        esac
    done

    if [ -z "$addr" ] || [ -z "$config_file" ]; then
        echo "Error: Missing required arguments for run_node."
        echo "Usage: run_node --addr <address> --config_file <path> [--file_output <true/false>] [--trace] [--erasure]"
        return 1
    fi

    if [ -n "$trace" ]; then
        echo "Starting node on ${addr} with config ${config_file} and tracing enabled..."
    else
        echo "Starting node on ${addr} with config ${config_file} and tracing disabled..."
    fi
    
    local cmd="cargo run --release -- --addr ${addr} --conf ${config_file} ${file_output} ${trace} ${erasure}"

    if [ -n "$file_output" ]; then
        local log_dir="./logs"
        mkdir -p "$log_dir"
        local log_file="${log_dir}/node_${addr//:/_}.log"
        echo "Logging to file: ${log_file}"
        cargo run --release -- --addr ${addr} --conf ${config_file} ${trace} ${erasure} > "${log_file}"
    else
        echo "Logging to terminal"
        cargo run --release -- --addr ${addr} --conf ${config_file} ${trace} ${erasure}
    fi
}

clean() {
    echo "Cleaning up node persistent data..."
    rm -rf ./db/node* ./logs/*
    echo "Cleanup complete."
}

run_all() {
    local config_path="./etc/config.json"
    local file_output=""
    local trace=""
    local continue=""
    local erasure=""

    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --config_file)
                config_path="$2"
                shift 2
                ;;
            --file_output)
                file_output="--file_output"
                shift 1
                ;;
            --trace)
                trace="--trace"
                shift 1
                ;;
            --continue)
                continue="--continue"
                shift 1
                ;;
            --erasure)
                erasure="--erasure"
                shift 1
                ;;
            *)
                echo "Unknown option: $1"
                echo "Usage: run_node --addr <address> --config_file <path> [--file_output] [--trace] [--erasure]"
                return 1
                ;;
        esac
    done

    validate_config "$config_path"

    echo "Starting all services based on config: ${config_path}"
    if [ -n "$continue" ]; then
        echo "Continuing with existing configuration."
    else
        echo "Starting fresh with new configuration."
        echo "Cleaning up previous node persistent data and logs."
        clean
    fi
    
    if [ -n "$trace" ]; then
        echo "Global tracing enabled for all nodes."
    else
        echo "Global tracing disabled for all nodes."
    fi

    local node_count
    node_count=$(jq -r '.nodes | length' "$config_path")

    for ((i=0; i<node_count; i++)); do
        local addr
        local ip=$(jq -r ".nodes[$i].ip" "$config_path")
        local port=$(jq -r ".nodes[$i].port" "$config_path")
        addr="${ip}:${port}"

        start_terminal "./scripts.sh run_node --addr ${addr} --config_file ${config_path} ${file_output} ${trace} ${erasure}" "node-${addr//:/_}"
        sleep 1
    done

    echo "All services started. Connect to the screen session 'paxos_rust' to see output (screen -r paxos_rust)."
}


virtual_users=(
    1 5 10 25 50 100
)

size=(
    1 100 10000 1000000 10000000
)

bench_system() {
    echo "Running system benchmark..."
    local base_url_env="$1"
    local base_url_arg=""
    if [ -n "$base_url_env" ]; then
        base_url_arg="-e \"BASE_URL=$base_url_env\""
    fi
    for vus_value in "${virtual_users[@]}"; do
        for size_value in "${size[@]}"; do
            echo "Using k6 with VUS=${vus_value}, SIZE=${size_value} and extra args: ${base_url_env}"
            # Start mpstat in the background
            mpstat 1 > "./benchmark/results/cpu_${size_value}b_${vus_value}vu.log" &
            MPSTAT_PID=$!
            # Run k6
            k6 run -e "VUS=$vus_value" -e "SIZE=$size_value" ${base_url_arg} --quiet ./benchmark/script-system.js > "./benchmark/results/_system_${size_value}b_${vus_value}vu.json"
            # Stop mpstat
            kill $MPSTAT_PID
            # Optional: Print average CPU usage
            awk '/^[0-9]/ {sum+=$3+$5} END {if(NR>0) print "Average CPU usage (%):", sum/NR; else print "No CPU data"}' "./benchmark/results/cpu_${size_value}b_${vus_value}vu.log" > "./benchmark/results/cpu_avg_${size_value}b_${vus_value}vu.txt"
        done
    done
}

bench_system_with_reset() {
    echo "Running system benchmark..."

    for vus_value in "${virtual_users[@]}"; do
        for size_value in "${size[@]}"; do
            # Reset the system before each benchmark
            stop_all
            sleep 5 # Wait for the system to cleanup
            
            run_all "$@"
            sleep 15 # Wait for the system to stabilize after restart

            echo "Using k6 with VUS=${vus_value}, SIZE=${size_value} and extra args: ${$@}"
            # Start mpstat in the background
            mpstat 1 > "./benchmark/results/cpu_${size_value}b_${vus_value}vu.log" &
            MPSTAT_PID=$!
            # Run k6
            k6 run -e "VUS=$vus_value" -e "SIZE=$size_value" --quiet ./benchmark/script-system.js > "./benchmark/results/_system_${size_value}b_${vus_value}vu.json"
            # Stop mpstat
            kill $MPSTAT_PID
            # Optional: Print average CPU usage
            awk '/^[0-9]/ {sum+=$3+$5} END {if(NR>0) print "Average CPU usage (%):", sum/NR; else print "No CPU data"}' "./benchmark/results/cpu_${size_value}b_${vus_value}vu.log" > "./benchmark/results/cpu_avg_${size_value}b_${vus_value}vu.txt"
        done
    done

    stop_all
    sleep 5
}


bench_baseline() {
    echo "Running benchmark on etcd for baseline..."

    for vus_value in "${virtual_users[@]}"; do
        for size_value in "${size[@]}"; do
            # We can't simply stop etcd and restart to expect the same leader, so we will run the baseline benchmark without resetting the system.
            # We will just wait out a few seconds to ensure no queued requests are pending.
            echo "Waiting for 15 seconds to ensure no pending requests..."
            sleep 15 # Http timeouts are usually 15 seconds, so this should be enough to ensure no pending requests.

            echo "Using k6 with VUS=${vus_value}, SIZE=${size_value}"
            # Start mpstat in the background
            mpstat 1 > "./benchmark/results/cpu_baseline_${size_value}b_${vus_value}vu.log" &
            MPSTAT_PID=$!
            # Run k6
            k6 run -e "VUS=$vus_value" -e "SIZE=$size_value" --quiet ./benchmark/script-etcd.js > "./benchmark/results/_baseline_${size_value}b_${vus_value}vu.json"
            # Stop mpstat
            kill $MPSTAT_PID
            # Optional: Print average CPU usage
            awk '/^[0-9]/ {sum+=$3+$5} END {if(NR>0) print "Average CPU usage (%):", sum/NR; else print "No CPU data"}' "./benchmark/results/cpu_baseline_${size_value}b_${vus_value}vu.log" > "./benchmark/results/cpu_avg_baseline_${size_value}b_${vus_value}vu.txt"
        done
    done
}

run_bench_suite() {
    timestamp=$(date +%Y%m%d_%H%M%S)
    stop_all
    add_netem_limits
    
    echo "Starting benchmark suite..."

    # Benchmark replication
    echo "Benchmarking replication..."
    
    bench_system_with_reset

    if [ -d ./benchmark/results/replication ]; then
        mv ./benchmark/results/replication ./benchmark/results/replication_$timestamp
    fi
    mkdir -p ./benchmark/results/replication
    mv ./benchmark/results/_system_*.json ./benchmark/results/replication/
    mv ./benchmark/results/cpu_*.log ./benchmark/results/replication/
    mv ./benchmark/results/cpu_avg_*.txt ./benchmark/results/replication/

    echo "Replication benchmark completed. Results are stored in ./benchmark/results/replication."

    # Benchmark erasure coding
    echo "Benchmarking erasure coding..."

    bench_system_with_reset --erasure

    if [ -d ./benchmark/results/erasure ]; then
        mv ./benchmark/results/erasure ./benchmark/results/erasure_$timestamp
    fi
    mkdir -p ./benchmark/results/erasure
    mv ./benchmark/results/_system_*.json ./benchmark/results/erasure/
    mv ./benchmark/results/cpu_*.log ./benchmark/results/erasure/
    mv ./benchmark/results/cpu_avg_*.txt ./benchmark/results/erasure/

    remove_netem_limits
    echo "Erasure coding benchmark completed. Results are stored in ./benchmark/results/erasure."

    echo "Benchmarking completed. Results are stored in ./benchmark/results/replication and ./benchmark/results/erasure."
    echo "You can analyze the results using k6's HTML report generation or other tools."    
}

add_netem_limits() {
    echo "Adding 200ms latency, 0.5mbit bandwidth, and 1% packet loss to loopback for ports 2080-2090..."
    # Mark packets to 2080-2090
    for port in {2080..2090}; do
        sudo iptables -A OUTPUT -t mangle -p tcp --dport $port -j MARK --set-mark 10
    done
    # Add tc rules for marked packets
    sudo tc qdisc add dev lo root handle 1: prio || true
    sudo tc filter add dev lo parent 1: protocol ip handle 10 fw flowid 1:1 || true
    sudo tc qdisc add dev lo parent 1:1 handle 10: netem delay 200ms rate 0.5mbit loss 1% || true
    echo "Netem limits applied. (200ms latency, 0.5mbit bandwidth, 1% loss)"
}

remove_netem_limits() {
    echo "Removing netem/iptables rules for loopback ports 2080-2090..."
    for port in {2080..2090}; do
        sudo iptables -t mangle -D OUTPUT -p tcp --dport $port -j MARK --set-mark 10 2>/dev/null || true
    done
    sudo tc qdisc del dev lo root 2>/dev/null || true
    echo "Netem limits removed."
}

if [ "$1" == "clean" ]; then
    clean
elif [ "$1" == "run_node" ]; then
    shift
    run_node "$@"
elif [ "$1" == "run_all" ]; then
    shift
    run_all "$@"
elif [ "$1" == "stop_all" ]; then
    stop_all
elif [ "$1" == "bench_system" ]; then
    bench_system "$2"
elif [ "$1" == "bench_system_with_reset" ]; then
    bench_system_with_reset "$2"
elif [ "$1" == "bench_baseline" ]; then
    bench_baseline
elif [ "$1" == "run_bench_suite" ]; then
    run_bench_suite
elif [ "$1" == "add_netem_limits" ]; then
    add_netem_limits
elif [ "$1" == "remove_netem_limits" ]; then
    remove_netem_limits
elif [ "$1" == "help" ] || [ -z "$1" ]; then
    echo "Usage: $0 {clean|run_node|run_all|stop_all|bench_system|bench_baseline|add_netem_limits|remove_netem_limits|help}"
    echo ""
    echo "Commands:"
    echo "  clean                                                                       :"
    echo "          Removes node persistent data (RocksDB) and log files."
    echo ""
    echo "  run_node --addr <address> --config_file <path> [--file_output <true/false>] : "
    echo "          Starts a single DistKV node."
    echo "          --addr         : e.g., 127.0.0.1:8081"
    echo "          --config_file  : Path to the configuration JSON."
    echo "          --file_output  : 'true' to log to file, 'false' (default) to log to terminal."
    echo ""
    echo "  run_all [--file_output <true/false>] [--config_file <path>]                 : "
    echo "          Starts all DistKV nodes in separate screen windows."
    echo "          --file_output : 'true' to log nodes to file, 'false' (default) to log to terminal."
    echo "          --config_file : Path to the configuration JSON (default: ./etc/config.json)."
    echo ""
    echo "  stop_all                                                                    : "
    echo "          Kills all screen sessions created by this script."
    echo ""
    echo "  bench_system '<leader_addr>'                                       : "
    echo "          Runs k6 benchmark against the running DistKV system."
    echo "          Leader url should be in the format 'http://<leader_ip>:<leader_http_port>'."
    echo ""
    echo "  bench_system_with_reset [<args>]                                               : "
    echo "          Runs k6 benchmark against the system, resetting nodes before each test."
    echo "          Accepts and forwards any arguments to run_all (e.g., --erasure, --trace)."
    echo "          Example: bench_system_with_reset --erasure"
    echo ""
    echo "  bench_baseline                                                              : "
    echo "          Runs k6 benchmark for baseline."
    echo ""
    echo "  run_bench_suite                                                             : "
    echo "          Runs the full benchmark suite, including replication and erasure coding benchmarks."
    echo ""
    echo "  add_netem_limits                                                           : "
    echo "          Adds 100ms latency and 1mbit bandwidth to loopback for ports 2080-2090."
    echo ""
    echo "  remove_netem_limits                                                        : "
    echo "          Removes the above netem/iptables rules."
    echo ""
    echo "  help                                                                        : "
    echo "          Displays this help message."
else
    echo "Invalid command! Use '$0 help' for usage."
    exit 1
fi

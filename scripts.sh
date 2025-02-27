#!/bin/bash

SCREEN_SESSION="paxos_rust"

kill_all_screen_session() {
    screen -ls | awk '/[0-9]+\./ {print $1}' | xargs -r -I{} screen -S {} -X quit
}

create_screen_session() {
    if ! screen -list | grep -q "$SCREEN_SESSION"; then
        screen -dmS "$SCREEN_SESSION"
    fi
}

# Scripts translated from scripts.ps1
connection_ip(){
    ip addr show ens33 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1
}

start_terminal() {
    local command="$1"
    local session_name="$2"
    
    echo "Starting terminal with command: ${command}"

    create_screen_session
    
    screen -S "$SCREEN_SESSION" -X screen -t "$window_name" bash -c "$command"
}

follower() {
    local addr="${1:-$(connection_ip)}"
    local leader_addr="${2:-0.0.0.0:8080}"
    local lb_addr="${3:-0.0.0.0:8000}"
    local port="${4:-8081}"
    local shard_count="${5:-4}"
    local parity_count="${6:-2}"

    if [ -z "$addr" ]; then
      addr="0.0.0.0"
    fi

  local command="cargo run -- follower ${addr}:${port} ${leader_addr} ${lb_addr} ${shard_count} ${parity_count}"

  echo "Starting follower on ${addr}:${port}"

  start_terminal "$command" "Follower ${port}"
}

followers() {
    local addr="${1:-$(connection_ip)}"
    local leader_addr="${2:-0.0.0.0:8080}"
    local lb_addr="${3:-0.0.0.0:8000}"
    local port="${4:-8081}"
    local shard_count="${5:-4}"
    local parity_count="${6:-2}"

    if [ -z "$addr" ]; then
        addr="0.0.0.0"
    fi

    local size=$((shard_count + parity_count - 1))
    echo "Starting $size followers"

    for ((i = 0; i < size; i++)); do
        local n_port=$((port + i))
        sleep 1
        follower "$addr" "$leader_addr" "$lb_addr" "$n_port" "$shard_count" "$parity_count"
    done
}

leader() {
    local addr="${1:-$(connection_ip)}"
    local lb_addr="${2:-0.0.0.0:8000}"
    local port="${3:-8080}"
    local shard_count="${4:-4}"
    local parity_count="${5:-2}"

    if [ -z "$addr" ]; then
        addr="0.0.0.0"
    fi

    local command="cargo run -- leader ${addr}:${port} ${lb_addr} ${shard_count} ${parity_count}"
    
    echo "Starting leader on ${addr}:${port}"

    start_terminal "$command" "Leader"
}

load_balancer() {
    local addr="${1:-$(connection_ip)}"
    local port="${2:-8000}"

    if [ -z "$addr" ]; then
        addr="0.0.0.0"
    fi

    local command="cargo run -- load_balancer ${addr}:${port}"
    
    echo "Starting load balancer on ${addr}:${port}"

    start_terminal "$command" "LoadBalancer"
}

run_all() {
    local addr="${1:-$(connection_ip)}"
    local lb_port="${2:-8000}"
    local leader_port="${3:-8080}"
    local follower_port="${4:-8081}"
    local shard_count="${5:-4}"
    local parity_count="${6:-2}"
    local run_local="${7:-false}"

    if [ "$run_local" == "true" ]; then
        addr="127.0.0.1"
    elif [ -z "$addr" ]; then
        addr="0.0.0.0"
    fi

    load_balancer "$addr" "$lb_port"
    sleep 1
    leader "$addr" "${addr}:${lb_port}" "$leader_port" "$shard_count" "$parity_count"
    sleep 1
    followers "$addr" "${addr}:${leader_port}" "${addr}:${lb_port}" "$follower_port" "$shard_count" "$parity_count"
    sleep 1
    
    local instance_count=$((shard_count + parity_count))
    start_terminal "./scripts.sh run_memcached $instance_count"

    echo "All services started."
}

# Memcached
run_memcached() {
    local instances="${1:-6}"
    local base_port="${2:-18080}"
    local memory="${3:-64}"
    local PIDS=()

    # Function to clean up Memcached processes on SIGINT (Ctrl+C)
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

    for ((i=0; i<instances; i++)); do
        port=$((base_port + i))
        memcached -d -m $memory -p $port
        sleep 1
        PID=$(pgrep -f "memcached -d -m $memory -p $port")
        PIDS+=($PID)
        echo "Started Memcached on port $port with PID $PID"
    done

    while true; do
        sleep 1
    done
}

if [ "$1" == "run_all" ]; then
    run_all "${@:2}"
elif [ "$1" == "leader" ]; then
    leader "${@:2}"
elif [ "$1" == "followers" ]; then
    followers "${@:2}"
elif [ "$1" == "follower" ]; then
    follower "${@:2}"
elif [ "$1" == "load_balancer" ]; then
    load_balancer "${@:2}"
elif [ "$1" == "run_memcached" ]; then
    run_memcached "${@:2}"
elif [ "$1" == "stop_all" ]; then
    kill_all_screen_session
else
    echo "Usage: $0 {run_all|leader|followers|follower|load_balancer|stop_all|run_memcached} [args]"
fi

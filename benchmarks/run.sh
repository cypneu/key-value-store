#!/bin/bash
set -e
set -f

NETWORK_NAME="redis-bench-net"
SERVER_CONTAINER="zig-server"
REDIS_CONTAINER="redis-reference"
IMAGE_NAME="zig-dev"

# Cleanup function
cleanup() {
  echo "Stopping servers and removing network..."
  docker stop $SERVER_CONTAINER >/dev/null 2>&1 || true
  docker rm $SERVER_CONTAINER >/dev/null 2>&1 || true
  docker stop $REDIS_CONTAINER >/dev/null 2>&1 || true
  docker rm $REDIS_CONTAINER >/dev/null 2>&1 || true
  docker network rm $NETWORK_NAME >/dev/null 2>&1 || true
}

# Register cleanup to run on exit or error
trap cleanup EXIT

echo "Creating Docker network: $NETWORK_NAME"
docker network create $NETWORK_NAME >/dev/null 2>&1 || true

echo "Starting Reference Redis container..."
docker run -d \
  --name $REDIS_CONTAINER \
  --network $NETWORK_NAME \
  redis >/dev/null

echo "Starting Zig server container..."
# We mount the current directory so the latest code is used/built
docker run -d \
  --name $SERVER_CONTAINER \
  --network $NETWORK_NAME \
  -v "$PWD":/app \
  -w /app \
  $IMAGE_NAME \
  sh -c "zig build -Doptimize=ReleaseFast && exec zig-out/bin/main" >/dev/null

echo "Waiting for servers to be ready..."
# Wait for Zig server "Listening" log message
MAX_RETRIES=30
count=0
while ! docker logs $SERVER_CONTAINER 2>&1 | grep -q "listening on"; do
  sleep 1
  count=$((count + 1))
  if [ $count -ge $MAX_RETRIES ]; then
    echo "Timeout waiting for Zig server to start. Logs:"
    docker logs $SERVER_CONTAINER
    exit 1
  fi
done

# Wait for Redis to be ready (simple check)
sleep 2

echo "Servers are ready. Running benchmarks..."
echo "==================================================="

# Helper to run benchmark against a specific host
run_single() {
  local host=$1
  local args=$2
  # -q for quiet (only numbers), -P for pipeline if needed
  docker run --rm \
    --network $NETWORK_NAME \
    redis \
    redis-benchmark -h $host -p 6379 -q $args
}

# Define benchmark scenarios
run_scenario() {
  local title=$1
  local args=$2

  echo "SCENARIO: $title"
  echo "---------------------------------------------------"
  echo ">> Reference Redis:"
  run_single $REDIS_CONTAINER "$args"
  echo ""
  echo ">> Zig Implementation:"
  run_single $SERVER_CONTAINER "$args"
  echo "==================================================="
}

# 1. Basic String Operations
run_scenario "Strings: SET/GET (100k reqs)" "-t set,get -n 100000"

# 2. List Operations
run_scenario "Lists: LPUSH/LPOP (100k reqs)" "-t lpush,lpop -n 100000"

# 3. Counter Operations
run_scenario "Counters: INCR (100k reqs)" "-t incr -n 100000"

# 4. Concurrency Stress
run_scenario "Concurrency: 50 Clients (SET/GET)" "-t set,get -n 100000 -c 50"

# 5. Pipelining Stress
run_scenario "Pipelining: Batch 16 (SET/GET)" "-t set,get -n 100000 -P 16"

# 6. Extended List Operations
run_scenario "Lists: RPUSH (100k reqs)" "-n 100000 RPUSH list_bench val"
run_scenario "Lists: LLEN (100k reqs)" "-n 100000 LLEN list_bench"
run_scenario "Lists: LRANGE (100k reqs)" "-n 100000 LRANGE list_bench 0 10"

# 7. Stream Operations
run_scenario "Streams: XADD (100k reqs)" "-n 100000 XADD stream_bench * field value"
run_scenario "Streams: Setup Small (50 items)" "-n 50 XADD stream_small * field value"
run_scenario "Streams: XRANGE (100k reqs)" "-n 100000 XRANGE stream_small - +"
run_scenario "Streams: XREAD (100k reqs)" "-n 100000 XREAD STREAMS stream_small 0-0"

# 8. Key/Type Operations
run_scenario "Keys: TYPE (100k reqs)" "-n 100000 TYPE key_bench"
run_scenario "Keys: KEYS (1k reqs)" "-n 1000 KEYS *"

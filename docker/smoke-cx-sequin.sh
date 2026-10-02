#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export IMAGE_REF="${IMAGE_REF:?Set IMAGE_REF to the locally built cx-sequin image}"

# Check the distributed notices, not just the source checkout.
docker run --rm --entrypoint cat "$IMAGE_REF" /usr/share/licenses/cx-sequin/LICENSE | cmp LICENSE -
docker run --rm --entrypoint cat "$IMAGE_REF" /usr/share/licenses/cx-sequin/NOTICE | cmp NOTICE -

override="$(mktemp)"
compose=(docker compose --project-name "cx-sequin-smoke-$$"
  -f docker/docker-compose.yaml -f "$override")

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans
  rm -f "$override"
}
trap cleanup EXIT

# Use isolated volumes and no host ports, so the check can run beside a dev stack.
cat > "$override" <<'YAML'
services:
  sequin:
    image: ${IMAGE_REF}
    pull_policy: never
    ports: !reset []
    healthcheck:
      test: ["CMD-SHELL", "curl --fail --silent http://localhost:7376/health | jq -e '.ok == true'"]
      interval: 2s
      timeout: 5s
      retries: 60
      start_period: 10s
  sequin_postgres:
    ports: !reset []
  sequin_redis:
    ports: !reset []
YAML

if ! "${compose[@]}" up --wait --wait-timeout 180 sequin; then
  "${compose[@]}" logs --no-color sequin sequin_postgres sequin_redis
  exit 1
fi

echo 'cx-sequin license and startup checks passed.'

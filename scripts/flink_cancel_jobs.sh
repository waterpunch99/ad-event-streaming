#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRY_RUN="${DRY_RUN:-0}"

cd "$PROJECT_ROOT"

if [[ "$#" -eq 0 ]]; then
  set -- "Clean Ad Events Job" "Campaign Metrics Job"
fi

list_output="$(docker compose exec -T flink-jobmanager flink list 2>/dev/null || true)"

for job_name in "$@"; do
  mapfile -t job_ids < <(
    printf '%s\n' "$list_output" |
      awk -F ' : ' -v name="$job_name" 'index($0, " : " name " (") > 0 {print $2}'
  )

  if [[ "${#job_ids[@]}" -eq 0 ]]; then
    printf 'No running Flink job found: %s\n' "$job_name"
    continue
  fi

  for job_id in "${job_ids[@]}"; do
    if [[ "$DRY_RUN" == "1" ]]; then
      printf 'Would cancel Flink job: %s (%s)\n' "$job_name" "$job_id"
    else
      printf 'Cancelling Flink job: %s (%s)\n' "$job_name" "$job_id"
      docker compose exec -T flink-jobmanager flink cancel "$job_id"
    fi
  done
done

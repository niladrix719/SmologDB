#!/usr/bin/env bash
# Appends a fake log line to a file every few tenths of a second, for the
# agent's -tail to follow.
#
#   ./scripts/loggen.sh                 # writes /tmp/app.log, 5 lines/s
#   ./scripts/loggen.sh /tmp/x.log 0.1  # other file, 10 lines/s
#   Ctrl+C to stop.
file=${1:-/tmp/app.log}
delay=${2:-0.2}
levels=(INFO INFO INFO WARN ERROR)
msgs=("user logged in" "request served" "cache miss" "db query slow" "payment failed")
n=0
while true; do
  n=$((n + 1))
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ${levels[RANDOM % ${#levels[@]}]} #$n ${msgs[RANDOM % ${#msgs[@]}]} user=$((RANDOM % 50))" >> "$file"
  sleep "$delay"
done

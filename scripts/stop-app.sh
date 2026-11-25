#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/env.sh"
for name in web spark-job; do
    if [[ ${1:-} == --web-only && $name != web ]]; then
        continue
    fi
    if [[ -r "$run/$name.pid" ]]; then
        pid=$(cat "$run/$name.pid")
        command=$(ps -p "$pid" -o args= || true)
        if [[ $command == *"$build"* ]]; then
            kill "$pid"
        fi
        rm -f -- "$run/$name.pid"
    fi
done

#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/env.sh"
exec java -Dloader.main=edu.sau.pro3.PosPublisher -cp "$build-boot.jar" \
    org.springframework.boot.loader.launch.PropertiesLauncher "$@"

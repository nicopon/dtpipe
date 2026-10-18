#!/usr/bin/env bash
# The lab's coordinator in the compose stack: creates, on first start, the bare repository its library is
# pushed to (a deployment points the coordinator at a git server instead), then runs the server.
# The repository is in the one directory the service mounts for it, so it creates nothing but its own.
set -euo pipefail

remote=/lab/library.git
[[ -f "$remote/HEAD" ]] || git init -q --bare -b main "$remote"

exec dotnet /app/DtPipe.Coordinator.Server.dll "$@"

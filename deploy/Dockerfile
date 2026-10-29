# syntax=docker/dockerfile:1
# The images of the distributed pipeline's two hosts, built from the repository root:
#
#   docker build -f deploy/Dockerfile --target coordinator --build-arg SOURCE_REVISION=$(git rev-parse HEAD) .
#
# SOURCE_REVISION is the full commit id the images are built from. .git is not in the build context, so
# the version a binary reports carries the revision only if it is handed over. Given, it must be a
# full commit id and the build fails when a binary does not report it; every image then has it as its
# org.opencontainers.image.revision label. Left out, the build warns, the binaries report no revision
# and the label is "unknown": a build that cannot say what it was built from.
#
# The dtpipe binary is built here for Linux, never copied from the host: a host build may link
# libraries the image does not have.
#
#   coordinator   the coordinator's server, with git (its library is a git repository) and no dtpipe
#   runner        the official runner: `dtpipe node --runner`, with dtpipe in /usr/local/bin, behind
#                 deploy/runner-entrypoint.sh, which restricts its traffic first (DTPIPE_EGRESS_ALLOW)
#   trial-node    the data node of the trial recipe (deploy/compose.trial.yaml): `dtpipe node --job`, not for production
#   runtime       the base of both: ASP.NET Core, curl for health checks, the unprivileged user
#   with-dtpipe   runtime plus the dtpipe binary: the base of the runner, and of any host that runs fragments
#
# Every process that serves or runs a pipeline runs as 1654:1654 (the `app` user of the .NET base
# images), and every HTTP service listens on 8080. The runner's image alone starts as root: its entry
# point installs the network rules, then starts dtpipe as 1654:1654 with no capability. /state
# belongs to that user and holds nothing until a deployment mounts it; a volume that replaces it must
# be writable by that uid. Another uid works if it can write /state and /tmp; the runner's applies
# no network rules, since installing them needs the start as root.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS source
WORKDIR /src
COPY Directory.Build.props ./
COPY src/ src/
ARG SOURCE_REVISION=""
RUN if [ -z "$SOURCE_REVISION" ]; then \
        echo "WARNING: no SOURCE_REVISION build argument: these images will not say which commit they were built from (--build-arg SOURCE_REVISION=\$(git rev-parse HEAD))." >&2; \
    else \
        printf '%s' "$SOURCE_REVISION" | grep -Eq '^[0-9a-f]{40}$' \
        || { echo "SOURCE_REVISION must be the full commit id (git rev-parse HEAD), not '$SOURCE_REVISION'." >&2; exit 1; }; \
    fi

# Self-contained single file, as build.sh publishes it, for the architecture being built.
FROM source AS dtpipe-build
ARG TARGETARCH
ARG SOURCE_REVISION=""
RUN --mount=type=cache,id=dtpipe-nuget,target=/root/.nuget/packages,sharing=locked \
    rid="linux-$([ "$TARGETARCH" = arm64 ] && echo arm64 || echo x64)" \
    && dotnet publish src/DtPipe/DtPipe.csproj -c Release -r "$rid" --self-contained true \
        -p:PublishSingleFile=true -p:DebugType=none -p:DebugSymbols=false -p:SourceRevisionId="$SOURCE_REVISION" -o /out \
    && mv /out/DtPipe /out/dtpipe \
    && { [ -z "$SOURCE_REVISION" ] || /out/dtpipe --version | grep -qF "+$SOURCE_REVISION"; } \
    || { echo "dtpipe does not report the revision $SOURCE_REVISION." >&2; exit 1; }

# Framework-dependent: the runtime image carries ASP.NET Core.
FROM source AS hosts-build
ARG SOURCE_REVISION=""
RUN --mount=type=cache,id=dtpipe-nuget,target=/root/.nuget/packages,sharing=locked \
    dotnet publish src/DtPipe.Coordinator.Host/DtPipe.Coordinator.Host.csproj -c Release -p:SourceRevisionId="$SOURCE_REVISION" -o /out/coordinator \
    && { [ -z "$SOURCE_REVISION" ] \
        || grep -qaF "+$SOURCE_REVISION" /out/coordinator/dtpipe-coordinator.dll; } \
    || { echo "The coordinator does not carry the revision $SOURCE_REVISION." >&2; exit 1; }

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && mkdir /state && chown 1654:1654 /state
WORKDIR /app
USER 1654:1654

# A single-file binary unpacks its native libraries on first run: under /tmp, which any uid may write,
# not under a HOME that a uid with no entry in /etc/passwd lacks.
FROM runtime AS with-dtpipe
ARG SOURCE_REVISION=""
LABEL org.opencontainers.image.revision=${SOURCE_REVISION:-unknown}
COPY --from=dtpipe-build --chmod=755 /out/dtpipe /usr/local/bin/dtpipe
ENV DOTNET_BUNDLE_EXTRACT_BASE_DIR=/tmp/.net

FROM runtime AS coordinator
ARG SOURCE_REVISION=""
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
USER 1654:1654
COPY --from=hosts-build /out/coordinator /app
ENV Coordinator__StateDirectory=/state
LABEL org.opencontainers.image.revision=${SOURCE_REVISION:-unknown}
EXPOSE 8080
# Liveness: /livez is anonymous and answers 200 while the process answers, a passive standby (Coordinator:Standby)
# included, which a check on /healthz would mark unhealthy and an orchestrator replace. Without a standby it answers
# at the moment /healthz does: once the server serves, its store open, and its library too when it has a remote.
# Readiness is /healthz (503 passive on a standby). An orchestrator that probes on its own replaces this check.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 CMD curl -fs http://localhost:8080/livez || exit 1
ENTRYPOINT ["dotnet", "/app/dtpipe-coordinator.dll"]

# Root for the entry point only: it needs NET_ADMIN, SETUID, SETGID and SETPCAP, and dtpipe never has them.
FROM with-dtpipe AS runner
USER root
RUN apt-get update && apt-get install -y --no-install-recommends iptables && rm -rf /var/lib/apt/lists/*
COPY --chmod=755 deploy/runner-entrypoint.sh /usr/local/bin/runner-entrypoint
ENV DtPipeNode__DtPipe=/usr/local/bin/dtpipe \
    DtPipeNode__StateDirectory=/state
ENTRYPOINT ["runner-entrypoint"]

# The bricks are the branches of the job file the recipe mounts at /etc/trial-node/bricks.yaml.
FROM with-dtpipe AS trial-node
ENV DtPipeNode__DtPipe=/usr/local/bin/dtpipe \
    DtPipeNode__StateDirectory=/state
ENTRYPOINT ["dtpipe", "node", "--job", "/etc/trial-node/bricks.yaml"]

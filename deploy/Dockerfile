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
#   runner        the official runner, with dtpipe in /usr/local/bin
#   trial-node    the data node of the trial recipe (deploy/compose.yaml), with dtpipe: not for production
#   runtime       the base of both: ASP.NET Core, curl for health checks, the unprivileged user
#   with-dtpipe   runtime plus the dtpipe binary: the base of the runner, and of any host that runs fragments
#
# Every image runs as 1654:1654 (the `app` user of the .NET base images) and every HTTP service
# listens on 8080. /state belongs to that user and holds nothing until a deployment mounts it; a
# volume that replaces it must be writable by that uid. Another uid works if it can write /state and /tmp.

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
    dotnet publish src/DtPipe.Coordinator.Server/DtPipe.Coordinator.Server.csproj -c Release -p:SourceRevisionId="$SOURCE_REVISION" -o /out/coordinator \
    && dotnet publish src/DtPipe.PipelineNode.Runner/DtPipe.PipelineNode.Runner.csproj -c Release -p:SourceRevisionId="$SOURCE_REVISION" -o /out/runner \
    && { [ -z "$SOURCE_REVISION" ] \
        || { grep -qaF "+$SOURCE_REVISION" /out/coordinator/DtPipe.Coordinator.Server.dll \
             && grep -qaF "+$SOURCE_REVISION" /out/runner/DtPipe.PipelineNode.Runner.dll; }; } \
    || { echo "A host does not carry the revision $SOURCE_REVISION." >&2; exit 1; }

# The trial recipe's data node, built from this checkout by compose and never published.
FROM source AS trial-build
ARG SOURCE_REVISION=""
RUN --mount=type=cache,id=dtpipe-nuget,target=/root/.nuget/packages,sharing=locked \
    dotnet publish src/DtPipe.PipelineNode.TrialNode/DtPipe.PipelineNode.TrialNode.csproj -c Release -p:SourceRevisionId="$SOURCE_REVISION" -o /out/trial-node \
    && { [ -z "$SOURCE_REVISION" ] \
        || grep -qaF "+$SOURCE_REVISION" /out/trial-node/DtPipe.PipelineNode.TrialNode.dll; } \
    || { echo "The trial node does not carry the revision $SOURCE_REVISION." >&2; exit 1; }

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
# /healthz is anonymous and answers 200 once the server serves, which is after its store is open, and its library
# too when it has a remote. An orchestrator that probes on its own (a compose file, a pod spec) replaces this check.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 CMD curl -fs http://localhost:8080/healthz || exit 1
ENTRYPOINT ["dotnet", "/app/DtPipe.Coordinator.Server.dll"]

FROM with-dtpipe AS runner
COPY --from=hosts-build /out/runner /app
ENV DtPipeNode__DtPipe=/usr/local/bin/dtpipe \
    DtPipeNode__StateDirectory=/state
ENTRYPOINT ["dotnet", "/app/DtPipe.PipelineNode.Runner.dll"]

# The bricks come from the file DtPipeNode__BricksFile names, which the recipe mounts.
FROM with-dtpipe AS trial-node
COPY --from=trial-build /out/trial-node /app
ENV DtPipeNode__DtPipe=/usr/local/bin/dtpipe \
    DtPipeNode__StateDirectory=/state
ENTRYPOINT ["dotnet", "/app/DtPipe.PipelineNode.TrialNode.dll"]

# syntax=docker/dockerfile:1
# The lab's images, built from the repository root (compose.yaml sets the context):
#
#   docker build -f samples/distributed-lab/docker/Dockerfile --target <target> .
#
# The dtpipe binary is built here for Linux, never copied from the host: a host build may link
# libraries the image does not have. Every target that runs fragments, plans or seeds carries it in
# /usr/local/bin.
#
#   coordinator      the coordinator's server, with git (its library is a git repository)
#   coordinator-lab  the same, plus the lab's scenarios, library seed and rights seed under /lab
#   runner           the official runner
#   node             the lab's data node host, its node files and seed script under /lab
#   idp              the lab's test identity provider, its clients file under /lab
#
# coordinator and runner hold nothing of the lab; everything lab-specific comes from the targets
# built on them or from compose.yaml's environment.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS source
WORKDIR /src
COPY Directory.Build.props ./
COPY src/ src/
COPY samples/distributed-lab/Lab.Idp/ samples/distributed-lab/Lab.Idp/
COPY samples/distributed-lab/Lab.NodeHost/ samples/distributed-lab/Lab.NodeHost/

# Self-contained single file, as build.sh publishes it, for the architecture being built.
FROM source AS dtpipe-build
ARG TARGETARCH
RUN --mount=type=cache,id=dtpipe-lab-nuget,target=/root/.nuget/packages,sharing=locked \
    rid="linux-$([ "$TARGETARCH" = arm64 ] && echo arm64 || echo x64)" \
    && dotnet publish src/DtPipe/DtPipe.csproj -c Release -r "$rid" --self-contained true \
        -p:PublishSingleFile=true -p:DebugType=none -p:DebugSymbols=false -o /out \
    && mv /out/DtPipe /out/dtpipe

# Framework-dependent: the runtime image carries ASP.NET Core.
FROM source AS hosts-build
RUN --mount=type=cache,id=dtpipe-lab-nuget,target=/root/.nuget/packages,sharing=locked \
    dotnet publish src/DtPipe.Coordinator.Server/DtPipe.Coordinator.Server.csproj -c Release -o /out/coordinator \
    && dotnet publish src/DtPipe.PipelineNode.Runner/DtPipe.PipelineNode.Runner.csproj -c Release -o /out/runner \
    && dotnet publish samples/distributed-lab/Lab.NodeHost/Lab.NodeHost.csproj -c Release -o /out/node \
    && dotnet publish samples/distributed-lab/Lab.Idp/Lab.Idp.csproj -c Release -o /out/idp

# curl for the healthchecks; every HTTP service listens on 8080, the image's default.
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app

FROM runtime AS with-dtpipe
COPY --from=dtpipe-build /out/dtpipe /usr/local/bin/dtpipe

FROM with-dtpipe AS coordinator
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY --from=hosts-build /out/coordinator /app
ENV Coordinator__DtPipe=/usr/local/bin/dtpipe \
    Coordinator__StateDirectory=/state
ENTRYPOINT ["dotnet", "/app/DtPipe.Coordinator.Server.dll"]

FROM coordinator AS coordinator-lab
COPY samples/distributed-lab/pipelines/ /lab/pipelines/
COPY samples/distributed-lab/library-seed/ /lab/library-seed/
COPY samples/distributed-lab/rights-seed.json /lab/rights-seed.json

FROM with-dtpipe AS runner
COPY --from=hosts-build /out/runner /app
ENV DtPipeNode__DtPipe=/usr/local/bin/dtpipe \
    DtPipeNode__StateDirectory=/state
ENTRYPOINT ["dotnet", "/app/DtPipe.PipelineNode.Runner.dll"]

FROM with-dtpipe AS node
COPY --from=hosts-build /out/node /app
COPY samples/distributed-lab/nodes/ /lab/nodes/
COPY samples/distributed-lab/seed/seed.sh /lab/seed/seed.sh
ENTRYPOINT ["dotnet", "/app/Lab.NodeHost.dll"]

FROM runtime AS idp
COPY --from=hosts-build /out/idp /app
COPY samples/distributed-lab/idp-clients.json /lab/idp-clients.json
ENTRYPOINT ["dotnet", "/app/Lab.Idp.dll"]

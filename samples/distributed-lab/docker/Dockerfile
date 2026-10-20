# syntax=docker/dockerfile:1
# The lab's images, built by compose.yaml from the repository root. Each extends an image of the
# product's Dockerfile (deploy/Dockerfile), which compose.yaml builds first and hands over as a
# named build context:
#
#   product-coordinator   the coordinator's server
#   product-with-dtpipe   the runtime plus the dtpipe binary
#   product-runtime       the runtime alone
#
#   coordinator-lab  the coordinator, plus the lab's scenarios, library seed and rights seed under /lab
#   node             the lab's data node host, its node files and seed script under /lab; it
#                    seeds its own database on first start (node-entrypoint.sh)
#   idp              the lab's test identity provider, its clients file under /lab
#
# The runner of the stack is the product's own image: nothing of the lab is added to it. Every
# image keeps the product's unprivileged user.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS source
WORKDIR /src
COPY Directory.Build.props ./
COPY src/ src/
COPY samples/distributed-lab/Lab.NodeHost/ samples/distributed-lab/Lab.NodeHost/

# Framework-dependent: the runtime image carries ASP.NET Core.
FROM source AS hosts-build
RUN --mount=type=cache,id=dtpipe-lab-nuget,target=/root/.nuget/packages,sharing=locked \
    dotnet publish samples/distributed-lab/Lab.NodeHost/Lab.NodeHost.csproj -c Release -o /out/node \
    && dotnet publish src/DtPipe.Coordinator.TrialIdp/DtPipe.Coordinator.TrialIdp.csproj -c Release -o /out/idp

FROM product-coordinator AS coordinator-lab
USER root
# The library's remote is a directory the stack mounts, owned by whoever runs the stack.
RUN git config --system --add safe.directory /lab/library.git
USER 1654:1654
COPY samples/distributed-lab/pipelines/ /lab/pipelines/
COPY samples/distributed-lab/library-seed/ /lab/library-seed/
COPY samples/distributed-lab/rights-seed.json /lab/rights-seed.json
COPY --chmod=755 samples/distributed-lab/docker/coordinator-entrypoint.sh /lab/coordinator-entrypoint.sh
ENTRYPOINT ["/lab/coordinator-entrypoint.sh"]

FROM product-with-dtpipe AS node
USER root
RUN mkdir -p /lab/state && chown 1654:1654 /lab/state
USER 1654:1654
COPY --from=hosts-build /out/node /app
COPY samples/distributed-lab/nodes/ /lab/nodes/
COPY --chmod=755 samples/distributed-lab/seed/seed.sh /lab/seed/seed.sh
COPY --chmod=755 samples/distributed-lab/docker/node-entrypoint.sh /lab/node-entrypoint.sh
ENTRYPOINT ["/lab/node-entrypoint.sh"]

FROM product-runtime AS idp
COPY --from=hosts-build /out/idp /app
COPY samples/distributed-lab/idp-clients.json /lab/idp-clients.json
ENTRYPOINT ["dotnet", "/app/DtPipe.Coordinator.TrialIdp.dll"]

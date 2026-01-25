# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy csproj and restore
COPY src/QueryDump/QueryDump.csproj src/QueryDump/
RUN dotnet restore src/QueryDump/QueryDump.csproj

# Copy everything else and build
COPY . .
WORKDIR /src/src/QueryDump
RUN dotnet publish -c Release -r linux-x64 --self-contained true -p:PublishSingleFile=true -o /app/publish

# Stage 2: Runtime
# We use runtime-deps because we are publishing self-contained single-file
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled
WORKDIR /app
COPY --from=build /app/publish/QueryDump .

# Entry point
ENTRYPOINT ["./QueryDump"]

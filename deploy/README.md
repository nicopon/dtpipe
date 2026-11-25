# Running the coordinator and the runner

The images of a distributed pipeline's two hosts come from one Dockerfile, `deploy/Dockerfile`.
Each release publishes them on GHCR, `ghcr.io/nicopon/dtpipe-coordinator` and
`ghcr.io/nicopon/dtpipe-runner`, tagged with the version, its major.minor and `latest`, for
`linux/amd64` and `linux/arm64`. [`deploy/compose.yaml`](./compose.yaml) is a recipe that runs
them to try them. A deployment runs the images with its own orchestrator: nothing here ships a
chart, and there is no image of the `dtpipe` CLI or of a ready-made data node. This page is the
list of what a deployment must provide. The behaviour of the server is in
[src/DtPipe.Coordinator.Server/CLAUDE.md](../src/DtPipe.Coordinator.Server/CLAUDE.md); the
settings a node host takes are `DtPipeNode__*` (the runner) or the host team's own.

## Trying it

```bash
docker compose -f deploy/compose.yaml up --wait
```

**Not for production.** The recipe starts a coordinator, which keeps its state in a SQLite file on
its volume, and a runner, with no identity provider: every identity is verified by a key (below). It
listens in plain HTTP on the loopback of the host only, so a key crosses the network in clear. Every
file it mounts is under `deploy/`; the images of the coordinator and the runner come from GHCR. It
gives the rights two identities, the first administrator and the runner, and no flow: a person adds
the nodes in Access, as [Your first distributed
pipeline](../docs/guides/distributed-first-pipeline.md) shows.

- **People.** The pages are at <http://coordinator.localtest.me:8080>, and the first
  administrator signs in with the key `docker compose -f deploy/compose.yaml exec coordinator cat
  /secrets/admin.key` prints; nobody else may do anything until they are added in Access. The
  address needs `localtest.me`, a public name that resolves to `127.0.0.1`; without a network that
  resolves it, add `coordinator.localtest.me` to `/etc/hosts`.
- **Local images.** `DTPIPE_VERSION` picks a published version (default `latest`);
  `DTPIPE_COORDINATOR_IMAGE` and `DTPIPE_RUNNER_IMAGE` name any other image, for example one built
  as [below](#the-images).
- **`Coordinator__Initialize` stays on.** The recipe is meant to start again after
  `docker compose down -v`; a deployment sets it for the first start only, as
  [below](#the-smallest-coordinator).
- **The keys.** On the first `up`, the one-off `keys` service (the runner's image, run once as
  root to hand the files to `1654`) writes a random key, `dtp_<id>_<43 characters>`, for each id
  of `DTPIPE_KEYS` that has none, mode `0600`, into the coordinator's volume, and copies each
  node's into a volume of that node's alone, owned by root, mode `0400`: a runner runs whatever
  branch a pipeline holds, so it never sees another identity's key, nor its own. The coordinator reads them as `@/secrets/<id>.key`
  (`Coordinator__Bootstrap__AdminKey`, `Coordinator__Bootstrap__Identities__<n>__*`), and each node
  as `DtPipeNode__Oidc__ClientSecret`; it finds the coordinator's issuer in the coordinator's
  metadata. `up` again keeps the keys, and `down -v` makes new ones. The coordinator makes no key
  itself; outside Docker, `dtpipe keygen <id>` makes the same key.
  `Coordinator__Auth__AllowHttpKeys` lets it take a key over HTTP, which the log and
  every page then say; a real deployment is reached over HTTPS and sets none of it.
- **The data node** is yours: a person declares it with `dtpipe node --job`
  ([REFERENCE.md](../REFERENCE.md#a-node-from-the-cli-dtpipe-node)) over their own database, and
  the studio titles its bricks by their ids. A team whose service is .NET adds a node to it with
  `AddDtPipeNode`
  ([src/DtPipe.PipelineNode.Hosting/CLAUDE.md](../src/DtPipe.PipelineNode.Hosting/CLAUDE.md)). The
  library of the recipe holds `shop-orders-by-country`, which runs on a node named `shop`.

## The images

A release publishes them; to build them from a checkout instead:

```bash
docker build -f deploy/Dockerfile --target coordinator \
    --build-arg SOURCE_REVISION=$(git rev-parse HEAD) -t dtpipe-coordinator .
docker build -f deploy/Dockerfile --target runner \
    --build-arg SOURCE_REVISION=$(git rev-parse HEAD) -t dtpipe-runner .
```

- `SOURCE_REVISION` is the full commit id the images are built from, and `.git` is not in the
  build context, so it must be handed over. Given, a wrong value fails the build, and so does a
  `dtpipe` binary or a host that does not report it; it is also the image's
  `org.opencontainers.image.revision` label, which is how a deployment tells what it runs. Left
  out, the build only warns: the binaries report no revision and the label says `unknown`.
- The coordinator runs as `1654:1654` (the `app` user of the .NET images). The runner's image starts
  as root, the time its entry point takes to install [its network rules](#the-runners-network), and
  runs `dtpipe` as `1654:1654`, never as root. The coordinator listens on 8080 and the runner on no
  port, so a runner needs no ingress. Both keep their state in `/state`, which that user owns. A
  named volume mounted there inherits the owner; a bind mount must be owned by that uid. A runner
  started as another uid runs `dtpipe` under it, with no rules (an empty list or
  `DTPIPE_EGRESS_RULES=off`), and that uid must write `/state` and `/tmp`; the coordinator under a
  uid with no home is not checked.
- Run them under an init (`docker run --init`, `init: true` in compose): the runner starts
  `dtpipe` child processes, and dotnet as PID 1 ignores the signal of its own crash.
- The runner's entry point, `deploy/runner-entrypoint.sh`, ends in `dtpipe node --runner`
  ([REFERENCE.md](../REFERENCE.md#a-node-from-the-cli-dtpipe-node)), configured by
  `DtPipeNode__*` and nothing else: it takes no argument that names a setting. A stop signal ends
  it with exit code 130, a cancellation.
- Two anonymous probes. `GET /healthz` is readiness: 200 whenever the coordinator serves, its
  store open, and 503 `passive` on a [standby](#a-passive-standby). `GET /livez` is liveness: 200
  while the process answers, a standby included. The coordinator image checks `/livez` itself
  (`docker ps` shows `healthy`); an orchestrator's own probes replace that check. The runner has
  no such route and its image no check.
- Without Docker, each release attaches the coordinator as the self-contained folder
  `coordinator/` of `dtpipe-coordinator-<platform>`, and `./build.sh` publishes it as
  `dist/release/coordinator/`; `dist/release/coordinator/dtpipe-coordinator` (`.exe` on Windows)
  takes the same `Coordinator__*` settings from its environment. The folder is the whole
  coordinator, `wwwroot` and the native SQLite library included: copy it as one. The library is a
  git repository, so `git` must be on the `PATH`, as the image carries it.

## What a runner runs

A runner hosts no data: it runs the steps between the nodes, and its host is shared by the steps of
every run. A step on a runner reads and writes its `arrow:` edges and nothing else: a file, a
database, a checkpoint, a cursor file or a DuckDB `duck-init` is refused when the pipeline is
planned, again by the runner before it starts the step, and by `dtpipe` itself, which the runner
starts with `--edges-only`: its SQL runs with DuckDB's external access off, and the step sees none
of the runner's variables (a `${{NAME}}` is refused). A reader such as
`generate:` belongs on a node, as a brick. Each step works in a directory of its own under the
runner's state directory, its temporary directory too, where DuckDB spills; the directory is
removed when the step ends.

The runner holds at most eight steps at once (`DTPIPE_RUNNER_MAX_RUNS` in the recipe,
`DtPipeNode__MaxRuns` for any node); a launch past them is refused by name. A step's SQL takes the
threads and memory the container reports, so a memory limit on the runner's container bounds what
its steps take together.

## The runner's network

A runner runs whatever step a pipeline holds, so its image limits what it can reach.
`DTPIPE_EGRESS_ALLOW` names the addresses it may open a connection to. The entry point installs
the rules as root (`iptables`, `ip6tables`), reads them back, and starts `dtpipe node --runner`
as `1654:1654` with no capability, an empty bounding set and `no_new_privs`: `dtpipe` can neither
change the rules nor gain a privilege back. A `DtPipeNode__Oidc__ClientSecret` given as `@path` is
read by the entry point, as root, so the file may be root's alone; the runner then makes itself
not dumpable, so a step, which runs as the same user, reads neither its environment nor its
memory.

```bash
docker run -d --init --cap-add NET_ADMIN -e DTPIPE_EGRESS_ALLOW=10.20.0.0/24 \
    -e DtPipeNode__CoordinatorUrl=http://coordinator:8080 ... dtpipe-runner
```

- **The list.** CIDR masks separated by commas, IPv4 and IPv6; a host is written `/32` or
  `/128`. An entry that is not a mask stops the start, named.
- **What to list.** The coordinator (its node channel, its data plane and, when it issues the
  keys, its token endpoint) and the identity provider, if there is one. The runner opens no other
  connection, DuckDB included, and listens on no port.
- **An address, not a name.** The rules hold addresses; the coordinator's may change when its
  container is created again. Give a mask that covers its subnet, or an address the deployment
  fixes for it.
- **Out**: the masks and the loopback, always (`arrow:` reaches the node's pipes there, and
  Docker's resolver listens at `127.0.0.11`). Any other connection is refused at once: a
  coordinator outside the list shows in the runner's log as `No route to host`, retried.
- **In**: nothing but the replies to connections the runner opened, and IPv6 neighbour
  discovery. A health check reaches the runner on its loopback or not at all; it has none.
- **A subnet includes its gateway**, and through it every port the Docker host publishes. The
  recipes list Docker's local address pools, `172.16.0.0/12,192.168.0.0/16`, which hold their
  network whatever subnet Compose gives it; set `DTPIPE_EGRESS_ALLOW` where the pools are
  elsewhere. A deployment lists its coordinator alone, at an address it fixes.
- **DNS** goes to Docker's resolver on the loopback, which answers the network's names and
  forwards the others from outside the container: an outside name resolves, though its address
  stays unreachable. The rules cannot close that channel; a network with no outside access, or a
  policy around the runner, can. On Docker's default bridge (`docker run` with no `--network`)
  the resolver is the host's, not on the loopback: list it, or run on a network of your own.
- **IPv6** gets the same rules whenever the kernel has it, whether or not the network gives the
  container an address. A kernel without IPv6 has no IPv6 traffic; its masks are then ignored,
  with a warning.
- **An empty list** restricts nothing: the runner starts, and warns.
- **Capabilities, at the start only**: `NET_ADMIN` for the rules, and `SETUID`, `SETGID` and
  `SETPCAP` to start `dtpipe` as `1654` with none. Docker's default set holds the last three, so
  `--cap-add NET_ADMIN` is enough; with `cap_drop: [ALL]`, add all four. A list the entry point
  cannot apply (a capability missing, no `iptables`, a kernel that refuses a rule) stops the
  start with exit code 1 and an `[ERROR]` line that names the cause: a runner never runs as
  though it were protected.
- **A platform that grants no `NET_ADMIN`** (a managed container service): leave
  `DTPIPE_EGRESS_ALLOW` empty. The entry point then asks for no `NET_ADMIN`, only the three
  capabilities that start `dtpipe` as `1654`, installs nothing and warns. A platform that withholds
  one of those stops the start, naming it: start the image as another user, with
  `DTPIPE_EGRESS_RULES=off`. The platform's own network then restricts the runner.
- **Kubernetes**, and any profile that refuses `NET_ADMIN` or a start as root: set
  `DTPIPE_EGRESS_RULES=off` and restrict the runner with a NetworkPolicy (out: the coordinator
  and the provider; in: nothing) or its equivalent. The entry point then installs nothing and
  warns that the network must do it. Started as another user it installs nothing either, and a
  non-empty list stops the start unless the rules are `off`; give that pod
  `allowPrivilegeEscalation: false` and no capability, since the entry point cannot drop them
  itself.
- **Reading the rules.** `docker exec <runner> iptables -S` shows them, and `iptables -L OUTPUT
  -v -n` counts the connections refused, which are not logged. `docker exec` enters as root
  with the container's capabilities: whoever may exec into the runner may change its rules.
- **Checked** by `tests/scripts/validate_runner_egress.sh` (the images of a checkout,
  `DTPIPE_RUNNER_IMAGE` and `DTPIPE_COORDINATOR_IMAGE`), on OrbStack only: not on a Linux Docker
  host, nor on Kubernetes.

## The smallest coordinator

One coordinator, one volume, no other service and no identity provider. The coordinator keeps its
state in a SQLite file, `coordinator.db`, and its library as a git repository, both under
`/state`. It makes no key: the first administrator's is yours to make, 32 random bytes in
base64url after `dtp_<id>_`, in a file only the coordinator's user reads:

```bash
mkdir -p secrets
printf 'dtp_admin_%s' "$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')" \
    > secrets/admin.key
chmod 600 secrets/admin.key && sudo chown 1654:1654 secrets/admin.key
docker run -d --init --name coordinator -p 8080:8080 -v coordinator-state:/state \
    -v "$PWD/secrets":/secrets:ro \
    -e Coordinator__Initialize=true \
    -e Coordinator__PublicUrl=https://coordinator.example \
    -e Coordinator__Bootstrap__AdminKey=@/secrets/admin.key \
    dtpipe-coordinator
```

- The key signs `admin` in through the form at the public address. The rights get that person
  once, while they hold no administrator with a live key; from then on Access makes every key, and
  shows each once.
- `Coordinator__Initialize=true` is for the first start only. A store that holds nothing stops the
  start unless it is set, because rights begun again could grant what was revoked and run ids
  would be given twice. Remove it afterwards, so that a volume that was not mounted stops the
  start instead of beginning again.
- The volume is the coordinator's whole state: **back it up**, as
  [below](#backing-up-a-sqlite-coordinator). The database is a single file that one process holds;
  a second coordinator on the same volume is refused at its start.
- `Coordinator__Sqlite__Directory` puts the database in another directory, for example a volume of
  its own mounted at `/data`, and `Coordinator__Library__Directory` does the same for the library's
  git repository (`library` under `/state` by default). Both must be writable by `1654`; the
  server trusts the library's directory itself, whoever owns the volume.

## Keeping the state in a database

SQLite is the default. When the state belongs in a database the operator already runs, with its
backups and its availability, the coordinator keeps it in PostgreSQL or SQL Server instead. Two
settings switch it. The connection string is a secret: it comes from the environment or as `@path`
to a file, and one given on the command line stops the start.

```bash
-e Coordinator__Store=postgresql                          # or sqlserver
-e Coordinator__ConnectionString=@/secrets/state.connection
```

The database holds the rights, the run journal, the armed pipelines, the cursors, the hosts set
aside, the session keys and the signing keys of the key issuer. The library stays a git
repository, in the volume or at its remote.

- **One database for each coordinator.** The tables have generic names (`runs`, `rights`) in the
  user's default schema, and the lock below is the database's. Give each coordinator a database
  of its own, or at least a default schema of its own.
- **The lock.** A coordinator holds its database for the life of its process: an advisory lock
  on PostgreSQL, an application lock (`sp_getapplock`) on SQL Server, the file itself on SQLite.
  A start that finds it held retries every second for `Coordinator__StoreLockWait` (30 seconds
  unless set), then stops, naming the store.
- **A lost session stops the coordinator.** The connection that holds the lock is the only one the
  coordinator uses, and it is probed every 5 seconds. When it is lost the coordinator exits with
  code 1, since another may take the database; it never reconnects by itself, because a session
  opened again holds no lock.
- **Kubernetes: use the `Recreate` strategy.** The default rolling update starts the new pod
  before it stops the old one, and the new one stops once `StoreLockWait` has passed. With
  `Recreate` the old pod is gone first. A [standby](#a-passive-standby) changes what an update
  can be.
- **Keep-alives, so that a dead coordinator lets go of the lock.** On PostgreSQL the coordinator
  sets TCP keep-alives on its session (idle 10 s, interval 5 s, count 3), and the server ends a
  vanished session, and its lock, within about 25 seconds. On SQL Server the server's own
  keep-alive does it, 30 seconds unless the server sets another.
- **Rights the user needs.** To create tables in its database, and, on SQL Server, to call
  `sp_getapplock`. The coordinator creates its tables on a start that may begin
  (`Coordinator__Initialize`) and refuses a database whose schema is newer than itself.
- **Tested** on PostgreSQL 18 and on Azure SQL Edge. The SQL is a portable subset, but no other
  version of SQL Server is tested.

### A passive standby

A second coordinator on the same database can wait for it, ready to take over when the first one
dies. Give it the same settings and one more:

```bash
-e Coordinator__Standby=passive
```

- **What it does.** A start that finds the database held waits for it without bound, trying each
  second, instead of stopping after `StoreLockWait` (which it refuses beside it). Meanwhile it
  reads nothing and writes nothing: `/livez` answers 200, `/healthz` 503 `passive`, and every
  other request 503 with `Retry-After`, a node's connection included. Once it holds the database
  it starts as any restart does: it reads the state, ends the runs the other left in flight and
  asks the nodes to give up their fragments. The nodes retry every 2 seconds and find it. Between
  the responder stopping and the coordinator serving, while it reads its state and fetches its
  library, nothing answers on its port.
- **What it needs.** PostgreSQL or SQL Server: a SQLite file belongs to one process, and the
  setting is refused on it. A library remote (`Coordinator__Library__Remote__Url`): without one
  the library would be in the other instance's volume alone, and the setting is refused. Each
  instance has a state directory of its own.
- **How fast it takes over.** As fast as the database lets the lock go. A coordinator that stops
  closes its session, and the lock goes at once; one that dies holds it until the keep-alives
  above end its session, about 25 seconds on PostgreSQL and 30 on SQL Server. Then the standby
  reads the state and the library. Runs in flight are lost, as at any restart.
- **Probes.** Route traffic by `/healthz` (a Kubernetes readiness probe), restart by `/livez` (a
  liveness probe): a probe of liveness on `/healthz` would restart a standby forever.
- **Updates on Kubernetes.** A standby is never ready, so a rolling update whose floor is one
  ready pod waits for it forever: the new pod is ready only once the old one has gone, which the
  update does only once the new one is ready. With `replicas: 1`, `maxSurge: 1` and
  `maxUnavailable: 1`, the new pod starts passive while the old one stops, and takes the
  database once it has left: the update saves the scheduling and the image pull, not the
  restart. With two replicas, an active and a standby, `maxUnavailable` must be 2, which stops
  both: `Recreate` stays the simpler choice there.
- **Stopping it.** A stop signal while it waits ends the process (exit code 143 on `SIGTERM`),
  nothing taken.
- **Tested** by `PassiveCoordinatorTests`, on PostgreSQL in CI and by hand on SQL Server.

## Backing up a SQLite coordinator

There is no online backup. One process holds `coordinator.db` exclusively and in WAL mode, so a
copy made while it runs lacks what the journal has not given back to the file, and no other tool
(`sqlite3 .backup`, `litestream`) can open it beside the coordinator. Back it up stopped, and copy
the **whole state**: `coordinator.db`, its `-wal` file if there is one, and `library/`. A file alone
restores the rights and the runs but not the library.

```bash
docker stop coordinator
docker run --rm -v coordinator-state:/state:ro -v "$PWD":/backup alpine \
    tar -C /state -czf /backup/coordinator-state.tgz .
docker start coordinator
```

To restore, stop the coordinator, empty the volume (or use a new one), unpack the copy into it and
start the coordinator **without** `Coordinator__Initialize`:

```bash
docker run --rm -v coordinator-state:/state -v "$PWD":/backup:ro alpine \
    sh -c 'find /state -mindepth 1 -delete && tar -C /state -xzf /backup/coordinator-state.tgz'
```

If `Coordinator__Sqlite__Directory` puts the database on another volume, back up both and restore
both.

- Without `Initialize`, a volume that did not get its database stops the start ("the rights
  document is absent") instead of beginning again, so a restore that missed the file shows at once.
- What comes back is the state of the copy: rights edits, armed pipelines, runs, session keys and
  library saves made since are gone, and the run ids that followed the copy are given again.
- With a library remote (`Coordinator__Library__Remote__Url`), `library/` is aligned to the remote
  at the start: the remote is the authority, so a save pushed since the copy is kept. Without one,
  `library/` in the volume is the only copy, and a restore brings it back as it was at the copy.
- Keep the copy as private as the volume: it holds the session keys, unencrypted.

## Backing up a PostgreSQL or SQL Server coordinator

The state is in the database, so the database's own tools back it up (`pg_dump`, a snapshot, a
SQL Server backup), without stopping the coordinator. The library is not in it: it is at the
remote, or in the volume as described above. Restore the database, start the coordinator
**without** `Coordinator__Initialize`, and what comes back is the state of the copy. A copy holds
the session keys unencrypted: keep it as private as the database.

## What a deployment provides

- **A store**: the coordinator's volume (SQLite, the default), or a PostgreSQL or SQL Server
  database of its own, [above](#keeping-the-state-in-a-database).
- **The library.** With `Coordinator__Library__Remote__Url` (and `__Token`, a secret, `__User`,
  `__Timeout`), every save is pushed to that git remote before it is accepted. Without it,
  `/state/library` holds the **only copy**: it is in the volume, so back the volume up, or accept
  losing the library with it. A deployment chooses one of the two.
- **The first keys.** `Coordinator__Bootstrap__AdminKey` (required without a provider) and
  `Coordinator__Bootstrap__Identities__<n>__*`, each `@path` to a file or a value of the
  environment, never of the command line. The coordinator issues the tokens a key is exchanged
  for, under `Coordinator__PublicUrl`, and refuses to take a key over HTTP.
  `Coordinator__Studio__ClientId` names the client of a person's token, which `dtpipe studio`
  presents.
- **An identity provider**, if people or services are verified by one (`Coordinator__Authority`,
  https in production, on an origin of its own). The pages sign people in with
  `Coordinator__Ui__ClientId` and `Coordinator__Ui__ClientSecret` (a secret): a confidential
  client whose redirect URI is `https://<public address>/signin-oidc`.
  `Coordinator__Auth__PersonKeys=false` then admits people through it only.
- **People**: an identity of kind `person` in Access, with its role; with a provider,
  `Coordinator__Roles__Admin`, `__Designer`, `__Reader` too. Nobody else may do anything.
- **TLS**, by a proxy in front, and `Coordinator__PublicUrl`: see below.

A node host (the runner, or a team's own) takes its coordinator's address and its secret, a key or
the provider's (`DtPipeNode__CoordinatorUrl`, `DtPipeNode__Oidc__ClientSecret`, `@path` to read it
from a file); its client is its name unless `DtPipeNode__Oidc__ClientId` says otherwise, and its
issuer is the first one the coordinator names unless `DtPipeNode__Oidc__Authority` does: beside a
provider, a node verified by a key sets it to the coordinator's address. Its rights are the
coordinator's, never its own.

A node gives each fragment's `dtpipe` child its edge compression on the command line
(`--bind-compression`, `--bind-compression-level`; `DtPipeNode__ArrowCompression`, `lz4` by
default, and `DtPipeNode__ArrowCompressionLevel`), never through the environment. A `dtpipe` older
than 1.16 does not know the flags and refuses to start, loudly, so a node and the `dtpipe` it runs
are upgraded together. A 1.16 child reads a compressed edge whichever way it was written.

Rows cross the coordinator's hub in clear unless the nodes seal them. To seal them, make one key
for the whole trust domain, mount it as a file on every node host, and set two settings there:

```bash
(umask 077; openssl rand -base64 32 > e2e.key)
docker run -d --init --cap-add NET_ADMIN -e DTPIPE_EGRESS_ALLOW=10.20.0.0/24 \
    -v "$PWD/e2e.key:/run/secrets/e2e.key:ro" \
    -e DtPipeNode__EndToEndEncryption__Enabled=true \
    -e DtPipeNode__EndToEndEncryption__PreSharedKey=@/run/secrets/e2e.key \
    -e DtPipeNode__CoordinatorUrl=http://coordinator:8080 ... dtpipe-runner
```

`dtpipe node --e2e --e2e-key @/run/secrets/e2e.key` takes the same two on a command line.

- **Nodes only.** The key is mounted on the nodes, never on the coordinator.
- **Every node alike**, mode and key. An edge whose ends differ is refused before anything
  launches, and named
  ([what the coordinator checks](../docs/distributed.md#sealing-the-data-plane)).
- **The runner** reads a key given as `@path` in its entry point, as root, before it drops its
  privileges, as it reads its client secret.
- **No rotation.** The key is read once at start: stop everything, change it on every node, start
  again.
- **A floor.** `Coordinator__DataPlane__RequiredProtection` on the coordinator names the lowest
  level a replica may announce; one under it runs no fragment
  ([REFERENCE.md](../REFERENCE.md#coordinator-settings-coordinator__)).

## HTTPS in front

The coordinator listens in plain HTTP and never terminates TLS: a proxy or ingress does. Tell the
coordinator the address people reach it at, once, as `Coordinator__PublicUrl` (for example
`https://coordinator.example`: a scheme, a host and a port if not the default, no path). The
server builds from it the address it tells the studio (`/.well-known/oauth-protected-resource`),
the sign-in's return address, and the `Secure` flag of the session cookie. It reads no
`X-Forwarded-*` header and ignores the `Host` of a request, so no caller can change them. Without
it the request's own scheme and host stand, and behind a TLS proxy the sign-in would send
`http://` return addresses.

The node channel is SignalR: the proxy must pass WebSocket upgrades and keep long requests open.
TransportR, which carries the data between nodes, is plain HTTP: requests, one server-sent event
stream per client, and HTTP streams that must not be buffered. Behind nginx, the POST that sends a
stream wants `proxy_request_buffering off`, `proxy_http_version 1.1` (the default only since
nginx 1.29.7) and `client_max_body_size 0` (1 MB by default, then a 413). These three come from
nginx's documentation and have not been tried here. A node may also reach the coordinator
directly, inside the network.

## On a managed container platform

A platform that starts the containers for you (an App Service with sidecar containers, a container
service of a cloud) gives them a local disk, a public TLS address and, often, a volume that the
container's user cannot write. The coordinator keeps nothing it cannot lose in `/state`, and a
runner runs beside it:

```bash
# the coordinator
Coordinator__PublicUrl=https://coordinator.example
Coordinator__Store=postgresql                          # or sqlserver
Coordinator__ConnectionString=<the database, a secret of the platform>
Coordinator__Library__Remote__Url=https://git.example/team/library.git
Coordinator__Library__Remote__Token=<a secret of the platform>
Coordinator__Bootstrap__AdminKey=<the first administrator's key>
Coordinator__Initialize=true                           # the first start only
# a runner in the same app
DtPipeNode__CoordinatorUrl=http://127.0.0.1:8080
DtPipeNode__Oidc__ClientSecret=<the runner's key>
DTPIPE_EGRESS_RULES=off
```

- **The state is a database and a remote.** A [database](#keeping-the-state-in-a-database)
  (PostgreSQL or SQL Server) holds the rights and the journal, and a git remote the library, so
  `/state` stays on the container's own disk, owned by `1654`. A volume the platform mounts is
  often not writable by that user, and neither a SQLite file nor a library kept on it opens: the
  start names the file or the directory it cannot write. A managed PostgreSQL asks for TLS:
  `SSL Mode=Require` in the connection string.
- **Two addresses.** `Coordinator__PublicUrl` is the address of people, studios, remote nodes and
  the token endpoint. A runner beside the coordinator reaches its node channel and its data plane
  on loopback, `DtPipeNode__CoordinatorUrl`, which its rules always leave open; it still asks the
  public address for its token, since that is where the coordinator's issuer is.
- **Nothing to expose for a runner.** It listens on no port. Its end of a transfer is an
  outgoing connection.
- **An update that overlaps.** A platform that starts the new instance before it stops the old
  one gives the new coordinator a database that is still held: it waits `Coordinator__StoreLockWait`
  and stops. A runner of the new instance may also take its token from the old coordinator, which
  the new one refuses with `401`; it asks again at each attempt, and gets a good one once the old
  instance is gone. Stop, update and start the app, or choose a strategy that replaces the
  instance (a single revision; on Kubernetes, `Recreate`). A [standby](#a-passive-standby) on a
  platform is probed on `/livez`: `/healthz` answers 503 until it takes over.
- **A setting the platform leaves empty** reads as not set, `Coordinator__Initialize` included.

## Secrets

The database's connection string, the remote's token and the pages' client secret are given to the
container as environment variables by the orchestrator's secret mechanism, and a key the deployment
gives as a file it mounts (`@path`). The coordinator keeps a key as its hash only. They are in no
`ARG`, no `ENV` of the image and no command line, and the server writes none of them to a log or a
response. The token reaches git in the environment of the one process that talks to the remote. A
person who may inspect the container (`docker inspect`, the pod's spec) can read its environment:
give that right with the same care as the secrets. The nodes' end-to-end key is a file of the same
kind, on the nodes only: the coordinator is never given it. A node's own key is given as
`@path` or as the value in `DtPipeNode__Oidc__ClientSecret`, which a platform fills from its
secret store; only the command line (`--client-secret`) takes a reference alone.

What has been checked:

- **Tests.** The public address (`PublicUrlTests`) and the stores (`SqlStoreTests`: SQLite and PostgreSQL in CI, SQL
  Server by hand), in [tests/DtPipe.Coordinator.Server.Tests](../tests/DtPipe.Coordinator.Server.Tests).
- **A restart.** On real containers with SQLite, the coordinator ended and started again, and a copy of its state
  directory put back: it read back the rights, the library, the armed pipelines, the run journal and a session cookie.
- **The recipe, outside Docker.** Its coordinator settings, with keys made by the `keys` service's script, take the
  runner's key and the administrator's and bootstrap both identities.
- **Sealing, outside Docker.** A data node and a runner started with one key ran the pipeline of the first guide; a
  runner left in clear was refused by Deploy and by Run, with the edge named.
- **The recipe, in containers** (macOS Docker host, OrbStack). `up --wait` brings the coordinator healthy, the runner
  connected with its key and the page answering behind its sign-in. The `docker run` above, from the image of a checkout
  and a key file mounted read-only, joined a coordinator whose other nodes held the same key, and said its key id, never
  the key.
- **The entry point.** It reads a key given as `@path` from a file only root can read, which `dtpipe` cannot, and
  refuses a missing file and an empty one.
- **Four data nodes and a runner sealed with one key, beside a coordinator given neither.** Every edge of the pipelines
  ran `encrypted-psk` at both ends. A replica left in clear was listed, and the run went to a sealed one. Deploy refused
  the edge by name once none was sealed.
- **Where the key does not appear.** `docker inspect` of a node and of the runner shows the path of the key file. The
  key's value is in no log, no answer of the coordinator, no file of its state directory and nowhere in its database.

- **A managed platform** (Azure App Service with sidecar containers, PostgreSQL, keys, by hand).
  A pipeline ran from a node outside, through the public address, a coordinator and its runner
  on loopback, and back, and the coordinator started again on the same database.

Not checked: a platform other than that one, a real TLS proxy, a Linux Docker host, a volume owned by another uid, a SQLite file on a network volume, an
https remote with a token.

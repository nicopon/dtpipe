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

To run everything in Docker, add `deploy/compose.trial.yaml`: it builds a data node from the
checkout (the first start takes a few minutes), gives it and a scheduler a key, and seeds the flow
matrix and the pipeline that run on it.

```bash
docker compose -f deploy/compose.yaml -f deploy/compose.trial.yaml up --build --wait
```

- **People.** The pages are at <http://coordinator.localtest.me:8080>, and the first
  administrator signs in with the key `docker compose -f deploy/compose.yaml exec coordinator cat
  /secrets/admin.key` prints; nobody else may do anything until they are added in Access. The
  address needs `localtest.me`, a public name that resolves to `127.0.0.1`; without a network that
  resolves it, add `coordinator.localtest.me` to `/etc/hosts`.
- **A run.** With the second file, the library holds `trial-totals`: a generated source on the data
  node, summed by the runner, written back to the data node. It runs from the pages, or as a
  scheduler would: a `launcher-1` token (client `launcher-1`, secret its key,
  `/secrets/launcher-1.key` in the coordinator's container, from
  `http://coordinator.localtest.me:8080/connect/token`) posted to `/service/runs` with
  `{"pipelineId": "trial-totals"}`. The result is in the data node's `/state/totals.csv`. The
  pipeline is armed first, from its page: launched unarmed, the coordinator answers 409.
- **Version.** The data node of the second file is built from the checkout you run the recipe in,
  so run it from the checkout of a tag, with `DTPIPE_VERSION` set to that version without the `v`
  (`DTPIPE_VERSION=1.11.0`). The first file alone builds nothing.
- **Local images.** `DTPIPE_VERSION` picks a published version (default `latest`);
  `DTPIPE_COORDINATOR_IMAGE` and `DTPIPE_RUNNER_IMAGE` name any other image, for example one built
  as [below](#the-images). `SOURCE_REVISION` is handed to the image the second file builds.
- **`Coordinator__Initialize` stays on.** The recipe is meant to start again after
  `docker compose down -v`; a deployment sets it for the first start only, as
  [below](#the-smallest-coordinator).
- **The keys.** On the first `up`, the one-off `keys` service (the runner's image, run once as
  root to hand the files to `1654`) writes a random key, `dtp_<id>_<43 characters>`, for each id
  of `DTPIPE_KEYS` that has none, mode `0600`, into the coordinator's volume, and copies each
  node's into a volume of that node's alone: a runner runs whatever branch a pipeline holds, so it
  never sees another identity's key. The coordinator reads them as `@/secrets/<id>.key`
  (`Coordinator__Bootstrap__AdminKey`, `Coordinator__Bootstrap__Identities__<n>__*`), and each node
  as `DtPipeNode__Oidc__ClientSecret`; it finds the coordinator's issuer in the coordinator's
  metadata. `up` again keeps the keys, and `down -v` makes new ones. The coordinator makes no key
  itself. `Coordinator__Auth__AllowHttpKeys` lets it take a key over HTTP, which the log and
  every page then say; a real deployment is reached over HTTPS and sets none of it.
- **The data node** (`src/DtPipe.PipelineNode.TrialNode`) is what a team's own service looks like to
  the coordinator: it adds a node to itself and declares two bricks in a file. It is not published.
  A team that writes no .NET declares its bricks with `dtpipe node` instead
  ([REFERENCE.md](../REFERENCE.md#a-node-from-the-cli-dtpipe-node)): the recipe keeps the .NET host
  because it gives its bricks a title and a description, which a command line has no place for. The
  library of the first file holds `shop-orders-by-country`, which runs on a node named `shop` that a
  person declares with `dtpipe node`.

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
- Both run as `1654:1654` (the `app` user of the .NET images), listen on 8080 and keep their
  state in `/state`, which that user owns. A named volume mounted there inherits the owner; a
  bind mount must be owned by that uid. The runner's `dtpipe` runs under another uid that can
  write `/state` and `/tmp`; the coordinator under a uid with no home is not checked.
- Run them under an init (`docker run --init`, `init: true` in compose): the runner starts
  `dtpipe` child processes, and dotnet as PID 1 ignores the signal of its own crash.
- `GET /healthz` answers 200 whenever the coordinator serves; it is anonymous. The coordinator
  image checks it itself (`docker ps` shows `healthy`); an orchestrator's own probe replaces that
  check. The runner has no such route and its image no check.

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
  `Recreate` the old pod is gone first.
- **Keep-alives, so that a dead coordinator lets go of the lock.** On PostgreSQL the coordinator
  sets TCP keep-alives on its session (idle 10 s, interval 5 s, count 3), and the server ends a
  vanished session, and its lock, within about 25 seconds. On SQL Server the server's own
  keep-alive does it, 30 seconds unless the server sets another.
- **Rights the user needs.** To create tables in its database, and, on SQL Server, to call
  `sp_getapplock`. The coordinator creates its tables on a start that may begin
  (`Coordinator__Initialize`) and refuses a database whose schema is newer than itself.
- **Tested** on PostgreSQL 18 and on Azure SQL Edge. The SQL is a portable subset, but no other
  version of SQL Server is tested.

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

## HTTPS in front

The coordinator listens in plain HTTP and never terminates TLS: a proxy or ingress does. Tell the
coordinator the address people reach it at, once, as `Coordinator__PublicUrl` (for example
`https://coordinator.example`: a scheme, a host and a port if not the default, no path). The
server builds from it the address it tells the studio (`/.well-known/oauth-protected-resource`),
the sign-in's return address, and the `Secure` flag of the session cookie. It reads no
`X-Forwarded-*` header and ignores the `Host` of a request, so no caller can change them. Without
it the request's own scheme and host stand, and behind a TLS proxy the sign-in would send
`http://` return addresses.

The node channel and the data plane are SignalR: the proxy must pass WebSocket upgrades and keep
long requests open. A node may also reach the coordinator directly, inside the network.

## Secrets

The database's connection string, the remote's token and the pages' client secret are given to the
container as environment variables by the orchestrator's secret mechanism, and a key the deployment
gives as a file it mounts (`@path`). The coordinator keeps a key as its hash only. They are in no
`ARG`, no `ENV` of the image and no command line, and the server writes none of them to a log or a
response. The token reaches git in the environment of the one process that talks to the remote. A
person who may inspect the container (`docker inspect`, the pod's spec) can read its environment:
give that right with the same care as the secrets.

Checked: the public address, in `PublicUrlTests`, and the stores, in `SqlStoreTests` (SQLite and
PostgreSQL in CI, SQL Server by hand), of
[tests/DtPipe.Coordinator.Server.Tests](../tests/DtPipe.Coordinator.Server.Tests). Checked on the
real containers, on SQLite: the coordinator ended and started again, and a copy of its state
directory put back, read back the rights, the library, the armed pipelines, the run journal and a
session cookie. Checked outside Docker: the recipe's coordinator settings, with keys made by the
`keys` service's script, take the runner's key and the administrator's and bootstrap the four
identities of the second file. Not checked: the recipe's containers since the keys (the `keys`
service under `up --wait`, the volumes it hands over), a real TLS proxy, a Linux Docker host, a
volume owned by another uid, a SQLite file on a network volume, an https remote with a token.

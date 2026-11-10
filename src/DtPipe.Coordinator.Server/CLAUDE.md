# DtPipe.Coordinator.Server

Mechanics for editing the coordinator's server. The rules themselves are in the root `CLAUDE.md` and
`.claude/rules/distributed-pipelines.md`;
the hub, the barrier and admission are `DtPipe.Coordinator`'s (`src/DtPipe.Coordinator/CLAUDE.md`).

The server hosts `DtPipe.Coordinator` (`AddCoordinatorHub`) and adds what a person needs around it:
the node channel (`NodeChannel`, `NodeChannelHub`), bricks, the git-backed library,
the plan a launch makes, deployments and the run queue, rights, and the pages. Its settings are the
`Coordinator` configuration section (`CoordinatorServerOptions`), from the environment first. It is a
library: a host owns `Program` and calls `AddCoordinatorServer` and `MapCoordinatorServerAsync`
(`CoordinatorServer.cs`); the product's host is `DtPipe.Coordinator.Host`, whose `Program` adds nothing.

The files sit in one folder per domain, and a namespace follows its folder (`DtPipe.Coordinator.Server.<Folder>`). The root
holds what a host calls and what no domain owns. The rules below name classes, not paths.

| Folder | Object |
|---|---|
| root | `CoordinatorServer`, `CoordinatorServerOptions`, `EventBus`, `States`, `HealthEndpoints`, `SecretReference` |
| `Runs/` | the run queue, its dispatch, journal and orphans, and the run and service routes |
| `Deployments/` | deployments and their ledger, arming, planning and placement, fault injection |
| `Library/` | the git-backed library, its routes, remote and arming, the designer's check, bricks and bindings, pipeline health |
| `Nodes/` | the node inventory, enrollment, the node routes and the requests a node answers |
| `Access/` | rights, their routes and bootstrap, the trusted IdP, the account pages, the key issuer and identity keys |
| `Cursors/` | `CursorKeeper` |
| `Stores/` | the store interfaces, their SQL implementations, `SqlDatabase`, the dialects, `StoreRegistration`, `PassiveResponder` |

- **The server trusts an IdP for who, and decides what.** `TrustedIdp` validates the tokens of
  `Coordinator:Authority` for `Coordinator:Audience`, and those of its own key issuer (see "An identity is verified by the IdP
  or by a key"); a token names its client (`sub`) and grants nothing. `RightsIdentityProvider` replaces TransportR's `IClientIdentityProvider` and gives the
  subject its identity's groups in `RightsStore`, none for an unknown or disabled one. TransportR
  records those groups at `Connect` and at each token renewal, so an identity edit applies when the
  node connects again. The node name belongs to the rights, never to the host: `NodeChannelHub`
  files a known host under its identity's name whatever it proposes, and everything it reports
  under the name its connection announced. A token no identity knows is no node: its host waits in
  `EnrollmentQueue` (bounded, expired, never evicting a waiting subject), and what it proposes is
  untrusted text, cut here and escaped by the page. A disabled identity is refused when its host
  announces, and by `RightsStore.SubjectOf` and `GroupsOf` as an unknown one is, so no run is placed
  on its node. A node whose identity is gone leaves the inventory with its last replica; one that is
  renamed is the same node under its new name, once its host announces again. An identity has a **kind**,
  which the rights give and the token never does: a `node` (the default) hosts fragments and belongs to
  groups; a `monitor`, a `launcher` and a `person` belong to none and their names are labels, not a node's
  (`RightsStore.NodeIdentity`, `SubjectOf` and `GroupsOf` ignore them). A `person` holds a `Role` (`admin`, `designer` or
  `reader`, cumulative) and is verified by a key, found by its id, or by the IdP, found by its `Account` (the value of
  `Coordinator:Roles:Claim`, compared without case, never its `sub`: one person per account). A client id is unique among the identities of every kind,
  which is what lets an id name an author (`AuditEntry.By`, `RunView.LaunchedBy`). A launcher lists the **pipelines** it may launch
  (`Identity.Pipelines`, `Launches`): empty means none, and no other kind keeps a list (a monitor reads every pipeline). The
  host of such a service is refused when it announces, as a known identity that is no node, never queued, and
  `RightsIdentityProvider` gives it no group, so the data plane never admits it. Every sort of service reads its permission from here: add a sort as a kind, never as a claim.
  `[CI: DtPipe.Coordinator.Server.Tests] [local: the lab's smoke.py rights]`
- **A person's rights are the rights', and for a person of the IdP the configuration's too, never the token's.** People sign in through
  the provider (`Coordinator:Ui:ClientId`, a confidential authorization-code client, PKCE, the code
  in the query so the Lax correlation cookies ride the redirect) into the `dtpipe-coordinator`
  cookie, or with a key into the same cookie (see "A person verified by a key"). `CoordinatorRoles` (an `IClaimsTransformation`)
  reads, on every request, the role of a person verified by a key in the rights (`TrustedIdp.KeyPersonOf`: the claims the key
  issuer issued, so a provider's token naming the same subject is no such person), and for a person of the provider the higher of
  `Coordinator:Roles:Admin|Designer|Reader` (lists of the claim `Coordinator:Roles:Claim`, `email` by default) and the role of the
  enabled person of the rights whose `Account` is that claim (`RightsStore.ProviderPerson`): cumulative roles, none for anyone
  unlisted and absent from the rights (403, never reader by default). Without a provider no such person exists: one is refused
  when it is added and at start. The pages accept the
  cookie only. Only a page navigation (`TrustedIdp.IsPageNavigation`) is sent to sign in: to the form where people may hold a key,
  to the provider otherwise; any other request is told 401, with `WWW-Authenticate: Bearer resource_metadata="…"` on `/api`.
  `[CI: PersonKeyTests] [local: the lab's smoke.py rights — roles and sessions]`
- **A person verified by a key is judged against the rights on every request.** Where a key can be taken
  (`CoordinatorServerOptions.SignsPeopleInByKey`: the issuer, over HTTPS or with `Coordinator:Auth:AllowHttpKeys`, and people
  allowed a key), `GET /account/login` shows a form (a password field, an antiforgery token, `no-store`) beside a link to the
  provider's sign-in (`/account/login/idp`) where there is a provider, and the metadata's `dtpipe_auth` names `key`; elsewhere the provider's sign-in begins at
  once. `POST /account/login` (`AccountPages.SignInAsync`) takes the key under the token endpoint's rules (`KeyExchangeLimits`, a
  bounded form, one answer for every refusal, a log that names an id's shape only) and signs in a session
  holding the identity's id and the key's id, issued by the key issuer, never the key. The cookie's `OnValidatePrincipal` and the
  person scheme of the key issuer (`KeyPerson`) both ask `RightsStore.SignedInPerson` on every request (an enabled person, verified
  by a key, whose named key is live) and `Coordinator:Auth:PersonKeys`: a revocation, a disablement or a removal ends a session at
  its next request and an hour's token with it. A person of a key signs out here only. `Coordinator:Auth:PersonKeys=false`
  refuses a person by name when it is added and at start (`KeyIssuer.CheckIdentities`), maps no form, gives a person no token and
  takes `key` out of the metadata's `dtpipe_auth`. `[CI: PersonKeyTests, RouteTableTests]`
- **A person and a service are told apart by the claims of a token the issuer made from the identity's kind, whichever
  issuer.** A service's token comes from
  client credentials, so its subject is its client (`sub == client_id`, RFC 9068): the default
  policy (the node channel, TransportR's hub and streams) accepts that and nothing else. `/api`
  accepts the cookie or a person's bearer token obtained through `Coordinator:Studio:ClientId`
  (`dtpipe studio`'s public client): that `client_id`, a `sub` that is not it. The person scheme (`PersonBearer`) reads a token's
  `iss` without believing it, as the service scheme does, and hands it to `IdpPerson` or to `KeyPerson`, which validates it with
  the store's keys. Each scheme fails the
  other kind of token in `OnTokenValidated`, so the refusal holds whatever endpoint reads it, and an
  empty `Studio:ClientId` lets no bearer reach `/api`. **Limit, fail-closed**: a provider whose
  service tokens carry a `sub` other than their `client_id` has its services refused here.
  A supervisor is the third caller and a service's token like a node's: the `monitor` policy
  (`/health` only) takes the service scheme and then asks the rights whether the subject is an enabled
  `monitor`, so a node, a stranger and a disabled monitor are refused 403 with a valid token, and a
  person's or an anonymous request 401. A monitor reaches `/api` never (the person scheme fails its token)
  and the node channel only to be refused at the announcement.
  A scheduler is the fourth: the `launcher` policy (`/service` only) is the same check under the same scheme (`ServiceKindHandler`
  reads the kind the policy asks), so a node, a monitor, a stranger and a disabled launcher are refused 403 with a valid token,
  and a person's, a session cookie's or an anonymous request 401. Which pipelines it may launch is not the policy's, which
  cannot see the body: `ServiceEndpoints` checks the identity's list once the request is well-formed and before it says whether
  the pipeline exists or is armed, so a launcher learns nothing of a pipeline off its list; a run off the list is read as no run (404). It cancels only a run it started (`RunView.LaunchedBy` is its client id), a run of
  another identity or of a person being no run to it as well; a person's `/api/runs/cancel` is unchanged.
  `[CI: DtPipe.Coordinator.Server.Tests TrustedIdpTests, ServiceRoutesTests, NodeChannelHubTests] [local: the lab's smoke.py rights, bricks - health, a service launches a run]`
- **The public address is a setting, never a header.** The server listens in plain HTTP and a proxy in front terminates TLS.
  `Coordinator:PublicUrl` (a scheme, a host, a port; no path, user, query or fragment: it stops the start without echoing the value)
  makes `MapCoordinatorServerAsync` read every request as made to that address, first in the pipeline, whatever `Host` or `X-Forwarded-*` it
  carries; no forwarded header is ever applied. `OriginOf` (the metadata document, the `WWW-Authenticate` challenge), the OpenID
  Connect return address and the `SameAsRequest` cookies (session, correlation, nonce) all read the request's scheme and host, so
  they follow. Unset, the request stands as it came, which is right for a server reached directly. A deployment behind a TLS proxy
  must set it, or the sign-in's return address says `http://`. `[CI: PublicUrlTests]` `[unchecked: a real TLS proxy]`
- **A refused page gives a way out.** A person with no role who opens a page is signed out here
  and at the provider and lands on `/account/denied`: keeping the provider's session would sign
  the same person in again on the next try. A person who lacks only the admin role gets a 403 page
  linking back to `/` and to `/account/logout`. A person of a key signs out here alone, the provider never having signed them in.
  `AccountPages` writes both and the sign-in form; a message it shows is HTML-encoded first.
  `[CI: PersonKeyTests] [local: the lab's smoke.py rights — roles and sessions]`
- **A request that changes something carries `X-Dtpipe-Csrf`** (`RequireCsrfHeader`, on every
  `/api` group), a bearer's too: another site cannot set it without a CORS preflight this server
  never answers. The pages' `api()` helpers send it. Never add a cookie-authenticated route outside
  those groups.
  `[CI: RouteTableTests] [local: the lab's smoke.py rights]`
- **A route's authorization is the group it is mapped on, and `MapCoordinatorServerAsync` declares the groups.** The reader,
  designer and admin groups of `/api` (their roles, and the CSRF filter they inherit) are built once there. `NodeEndpoints`,
  `LibraryEndpoints`, `RightsEndpoints` and `RunEndpoints` each map their domain on the reader group they extend and on
  the designer or admin group they receive, as a host maps its own routes on the groups returned (`CoordinatorApiGroups`); `HealthEndpoints`
  maps `/health` on its own group, outside `/api`, for a monitor, and `/api/health` on the reader group; `ServiceEndpoints` maps `/service`
  on its own group, outside `/api`, for a launcher, with no CSRF filter since a service's token is bearer only; `CoordinatorServer` keeps
  the `account` group, `/healthz`, `/livez`, the metadata document, `/account/*` and the hubs. `[CI: RouteTableTests]`
- **Every rights change names its author** (`AuditEntry.By`, by `TrustedIdp.CallerOf`: the signed-in email, or the identity's
  id when it has none, a person verified by a key; a person of the provider with no email by its name and ` (idp)`, which no id
  holds; `Coordinator:Bootstrap` for what the bootstrap adds). `[CI: PersonKeyTests, ProviderPersonTests, KeyOnlyCoordinatorTests]`
- **The last administrator stays.** `RightsStore.Edit`, under the write gate and whatever the route, refuses a change that leaves
  no administrator where there was one, naming who was: revoking the last live key of the last one, disabling, removing or
  demoting them. An administrator is an enabled person whose role is `admin`, of the IdP or with a live key; while a provider
  signs people in, anyone `Coordinator:Roles:Admin` lists counts too, and then nothing is refused. A key that expires changes
  nothing: an administrator whose keys all lapsed stays, the bootstrap leaves an identity of its id as it is, and the start says so. `[CI: KeyOnlyCoordinatorTests, ProviderPersonTests]`
- **An identity is verified by the IdP or by a key, chosen when it is added.** `Identity.Auth` (`IdentityAuth`): `idp`, the default
  and what an identity written without it reads as, holds no key; `key` holds `IdentityKey` records, each the SHA-256 of a key
  (`ClearKey`: `dtp_{clientId}_{secret}`, 256 random bits as 43 base64url characters), its last four characters and its dates. An
  edit never changes the verification, as it never changes the kind, and `IdentityKinds.Keyed` lists the kinds a key may verify.
  `POST /api/rights/identities/{id}/keys` answers the key, once, with `Cache-Control: no-store`; a key made while another is live
  starts a rotation, and a third live key is refused until the old one is revoked (`…/keys/{keyId}/revoke`, which keeps the record
  with its date and cuts the identity's replicas). `GET …/keys` and `/api/rights` name a key by its id, its four characters and its
  dates. `ClearKey` has no `ToString` that shows the secret. `RightsStore.KeyHolder` is the one judge of a presented key: the client
  id it names only finds the identity, a disabled one or one the IdP verifies opens nothing, and every live key is compared in constant
  time. `[CI: IdentityKeyRoutesTests, IdentityKeyTests (SQLite and PostgreSQL)] [local: their SQL Server cases, which need DTPIPE_TEST_SQLSERVER or DTPIPE_TEST_REUSE_INFRA]`
- **The server issues the tokens of a key, and trusts them as it trusts the provider's.** `KeyIssuer`'s issuer is
  `Coordinator:PublicUrl`; there is none without it, and then an identity verified by a key is refused
  when it is added and a start whose rights hold one stops naming it. An `Authority` equal to the issuer stops the start: the issuer
  is what tells the two apart. It maps `GET /.well-known/openid-configuration`, `GET /.well-known/jwks.json` and `POST
  /connect/token`, client credentials only, the key as the form's `client_secret`; the client id must be the one the key names, and
  `KeyHolder` judges. Every refusal of the client is the same `invalid_client` (400) and every answer is `no-store`; a refusal logs
  the client id only when it has an id's shape, so a key sent in its place reaches no log. `KeyExchangeLimits` counts per source and
  per client id, known or not, in windows of a minute (the server reads no forwarded header, so behind a proxy the source is the
  proxy). The record's kind decides the token's shape. A service's: `sub == client_id ==` the identity's id, `aud` the audience, lasting
  `Coordinator:Auth:ServiceTokenLifetime` (10 minutes, 5 at least). A person's: `client_id` is `Coordinator:Studio:ClientId`,
  `sub` the identity's id, `dtpipe_key` the id of the key it was exchanged with (never the key, never its hash), lasting
  `Coordinator:Auth:PersonTokenLifetime` (an hour, 5 minutes at least); without a studio's client, or with
  `Coordinator:Auth:PersonKeys=false`, a person's key is refused as any other client is. While `PublicUrl` is plain HTTP the exchange is refused unless
  `Coordinator:Auth:AllowHttpKeys`, and while that setting is in effect the start logs a warning and every page shows a banner
  (`/api/info` `httpKeys`, `AccessPage`). The metadata lists the issuer after `Authority`, which the studio and a node with no
  `Oidc:Authority` read first: a node verified by a key sets `Oidc:Authority` to the coordinator's address. **Without `Coordinator:Authority` the issuer is
  the only one: `AddTrustedIdp` registers neither `IdpBearer`, `IdpPerson` nor OpenID Connect, every token goes to the key issuer's
  schemes, the challenge is the cookie's (the form), the metadata lists the issuer alone and `dtpipe_auth` is `key`, the form links to
  no provider and `/account/login/idp` is not mapped, `/api/rights` names no `idp`, and an identity of the IdP is refused when it is
  added and at start (`KeyIssuer.CheckIdentities`). A start without a provider needs `PublicUrl`, a key exchange it can make (HTTPS, or
  `AllowHttpKeys`), `PersonKeys` and `Coordinator:Bootstrap:AdminKey`, and refuses `Ui:ClientSecret`. **Same checks, local
  key source**: `KeyBearer` checks signature, issuer, audience and lifetime (30 seconds of skew) as `IdpBearer` does, with the keys of
  `IssuerKeys`, never through a request to the server's own address. `IssuerKeys` keeps RSA keys on a shelf of their own in the store
  (`issuer_keys`: an `IXmlRepository` keyed `IssuerKeys.Shelf`, since DataProtection's key manager reads every
  element of its own repository as one of its keys), each protected by DataProtection, so a restored store or a second instance
  validates the same tokens, and a key the session keys cannot open is skipped with a warning. **The issuer must be the one that
  verifies the identity**: the service scheme (`Bearer`) reads a token's `iss` without believing it and hands the token to
  `KeyBearer` or `IdpBearer`, which validate it in full and fail, on every request, a key issuer's token for an identity not
  verified by a key and a provider's token for one that is (a subject the rights do not know stays the provider's, for enrollment).
  `RightsIdentityProvider`, `ServiceKindHandler` and the announcement sit behind it, TransportR's renewal included.
  `[CI: KeyIssuerTests, IdentityKeyRoutesTests; Hosting.Tests KeyTokenTests — a node's token client, a real SQLite store]`
  `[unchecked: two instances that make a key at the same first start]`
- **The state is kept in one store, SQLite unless the deployment chose another.** `Coordinator:Store` is `sqlite` (the default),
  `postgresql` or `sqlserver`; `StoreRegistration` registers the seven stores of the chosen kind, opened when the first is
  asked for, which the server does before it serves. A setting of another store (`Coordinator:Sqlite:Directory`,
  `Coordinator:ConnectionString`) stops the start rather than being ignored. **One coordinator holds a database**: `SqlDatabase` is one
  connection for the life of the process, out of the pool, on which every read and write runs, and which holds the database: SQLite's
  file exclusively (`coordinator.db` in `Coordinator:Sqlite:Directory`, the state directory unless set; WAL, `synchronous=FULL`, no
  shared memory, so the file can sit on a network volume), PostgreSQL's session advisory lock, SQL Server's session application lock.
  A start that finds it held tries each second for `Coordinator:StoreLockWait` (30 s), then refuses naming the provider; with
  `Coordinator:Standby=passive` (PostgreSQL or SQL Server, with a library remote: refused otherwise, and beside `StoreLockWait`) it
  tries without bound, as a standby, and then starts as a restart does. Never a
  second connection and never a reconnection (`ConnectRetryCount=0` for SqlClient): a session opened again would hold no lock. A server's
  connection is probed every 5 seconds and PostgreSQL's asks the server for `tcp_keepalives_*`; lost, every call fails and the
  coordinator stops with exit code 1, since another may take the database. Nothing else can open SQLite's file while the process runs,
  so a backup is taken stopped and takes the whole state directory (`deploy/README.md`). The schema's number is the
  `schema_version` table's; a newer database is refused. What a provider writes differently is its `StoreDialect` (the schema, an
  upsert, the first rows of an ordered query, the connection), every other statement being one text, and keys compare as ordinal
  text on every provider (binary collations). A failure of the driver, its expiry and its cancellation included, is a
  `StoreUnavailableException` that carries no message of the driver. **`Coordinator:ConnectionString` is a secret** (`SecretReference`:
  the environment or `@path`) and the family never references `ConnectionStringSanitizer`, so no message or log carries a part of it,
  the host included: they name the provider, the setting and the driver's code. `Coordinator:Initialize` has one meaning for every
  store: a document or counter that is absent begins only when it is true. A new store takes its contract from `IRunStore`,
  `IRightsStore`, `IDeploymentStore`, `ICursorStore`, `IIgnoredHostStore` and `IXmlRepository`, and its own tests, as `SqlStoreTests`
  (per provider). `[CI: SqlStoreTests on SQLite and PostgreSQL (agentic-ci.yml)]` `[local: SqlStoreTests on SQL
  Server (DTPIPE_TEST_REUSE_INFRA, or coordinator-sqlserver.yml by hand); lab.sh stack restart-check with LAB_STACK_STORE=sqlite,
  restored from a copy as deploy/README.md's backup]` `[unchecked: the file on a network volume; a power cut between a write and its
  sync; a key longer than SQL Server's columns]`
- **A passive standby reads nothing until it holds the database.** `MapCoordinatorServerAsync` starts `PassiveResponder` on the
  host's addresses before anything else, opens `SqlDatabase` (whose wait is the standby's), then stops the responder and runs the
  start as it is. The responder is a server of its own over the host's configuration: it reads none of the server's services,
  so it opens neither the store, the library nor the session keys, and answers `/livez` 200, `/healthz` 503 `passive` and
  anything else 503 with `Retry-After`. The connection that took the lock is the one `SqlDatabase` keeps: never let go and
  taken again, or another standby would take it between. Nothing answers between the responder stopping and the host's server
  listening, the time of the start. A stop signal during the wait ends the process, nothing taken; `ApplicationStopping` ends
  the wait for a host that stops it. The image's health check reads `/livez`; `/healthz` stays readiness.
  `[CI: PassiveCoordinatorTests on PostgreSQL (agentic-ci.yml), SqlStoreTests — the refusals]` `[local: PassiveCoordinatorTests on
  SQL Server; lab.sh stack failover-check]` `[unchecked: a node's real reconnection, which the tests stand for by its token and
  its negotiation]`
- **The session cookie's keys are in the store, and a start that cannot read them stops.** `TrustedIdp.AddSessionKeys` points
  DataProtection at the `IXmlRepository` the host registers (`SqlXmlRepository`: the `session_keys` table, one row per
  element), under a fixed application name so that two hosts of one coordinator read each other's cookies, and `EnsureSessionKeys` reads
  the key ring before anything is served: a repository that fails throws, never returns nothing, since DataProtection would then mint a
  key and sign everyone out unannounced. A restart signs nobody out. **Limit**: the keys are stored as DataProtection hands them over,
  unencrypted (as the directory they replace held them), and they cross the network: the database is as readable as the account that owns it.
  `[CI: SessionKeysTests, SqlStoreTests]`
  `[local: lab.sh stack restart-check, on either store]`
- **The rights are kept in the store, read once, and memory is the truth.** `RightsStore` is a thin face over `IRightsStore`
  (`SqlRightsStore`): one JSON document and its version in the `rights` table, on the connection every store borrows
  (`SqlDatabase`, which the container disposes). It is loaded at start, and every reader (`CanSend` on each transfer, `Find` on
  each request) takes the document in memory without a lock, so none waits on the store. A change is made on a copy, written with a
  compare-and-set on the version and published only when the store kept it: a store that refuses (`StoreUnavailableException`, 503)
  leaves the rights as they were. One write gate serializes the changes and is held across the store call, which fails at once when
  the connection is lost and after the driver's command timeout otherwise. A write that finds another version (another writer, or an earlier
  write that timed out and was kept) reads the document again and refuses that change (409): **a 503 on a change may still have
  been applied**, and then the route's other effects (cutting an identity's replicas) did not run: repeating the change finds nothing to change, and `POST /api/rights/identities/{id}/reconnect` cuts them. A store with no document starts from `Coordinator:RightsSeed` once (`Create` is atomic, and a start that loses
  the race reads the winner's document) or empty, and refuses unless `Coordinator:Initialize` is true, since rights begun
  again could grant what was revoked. `[CI: RightsStoreTests, RightsRoutesTests, SqlStoreTests]`
- **A host adds to the server through two seams, never through a setting.** `MapCoordinatorServerAsync` returns the groups
  of `/api` behind every guard (`CoordinatorApiGroups`), and `AddCoordinatorServer` takes the host's own pages as `AdminView`s:
  under the view's path a page navigation signs in as any page does, and anyone but an admin gets the 403 page that names the
  view. The product's host adds neither. The lab's bench (the Lab view, its scenario catalog, planning by hand and fault
  injection) is its own coordinator's, `samples/distributed-lab/Lab.Coordinator`: nothing of it enters this project or its
  pages. `[CI: validate_distributed_boundary.sh — names under src/]` `[CI: RouteTableTests — the bench's routes] [local: the lab's
  smoke.py rights — a reader opens the Lab view]` `[unchecked: whether a new route belongs to the bench]`
- **Fault injection ends a fragment's child only on a replica whose host enabled it.** A replica that did
  not answers that it ended none, and `DeploymentManager.KillAsync` (`FaultInjection`) returns the count of children ended
  over every replica of the node: `0` says none was. The product's host maps no route to it. `[CI: PipelineNode.Tests FaultInjectionTests, RunQueueTests]`
- **The server never opens a node's data.** What reads it asks the node that hosts it, through
  `NodeRequests`: a brick preview and a
  library save's check (`DesignValidator`): each source brick's node sends its first row as Arrow IPC
  (`SampleBrick`, written by the `arrow:-` writer its fragment uses, with the contract that read produced), a runner dry-runs the
  job as the runner will see it (`ValidateJob`), and each sink brick's node checks its target against the contract that
  reaches it (`CheckSink`); a binding's verdict is the same two questions (`CheckSource`, `CheckSink`) asked against a pin. A branch that reaches a node's data without being its brick is refused
  before anything runs. **The server runs no dtpipe**: a library save's check goes to a runner,
  and with none online it refuses. A node announces a
  variable's name, never its value. `[local: the lab's smoke.py bricks — a library save's check, a sink that refuses]`
- **The coordinator manages flows and nothing around them.** It never does a schedule or a trigger (a run is started by a person, or by a service the rights
  grant `launcher`, which asks and is answered with the run's id: nothing is called back), a dependency between pipelines, an automatic retry or
  a backfill, a priority in the queue (`RunDispatch` stays a first-come admission check), or a transformation
  (it runs no dtpipe: see "The server never opens a node's data"). A supervisor asks it whether a pipeline can be
  launched and it answers (`PipelineHealth`); it checks nothing on its own account. A distribution plan is a placement, not a
  calendar. `[CI: RunDispatchTests — the order of the queue, TrustedIdpTests — a service never reaches /api]`
  `[unchecked: the rest; nothing fails when a scheduler, a trigger or a retry is added]`
- **What `dtpipe studio` calls is a contract** (`DtPipe.Jobs/Coordinator`: `CoordinatorApi`, the
  records): the server maps those routes and writes those records, and the metadata document
  announces `CoordinatorApi.Contract`; a change a deployed studio would misread takes a new number.
  The library's listing and `GET /api/library/{id}` are part of it: the studio opens a pipeline from them.
  `/api/catalog` lists nodes and bricks with each brick's reference (`CatalogBrick.Branch`: `{input: brick:<node>/<id>}` for a source,
  `{output: …}` for a sink), never its text and never a schema, which depends on the rows; `/api/bricks/recognize`
  says what each top-level branch of a job is: the brick it references in the direction it references it (`BrickRef.Of` against
  the catalog), so the studio never decides a brick itself; `/api/bricks/{node}/{id}/sample` asks the node for at most
  `MaxSampleRows` rows as Arrow IPC (`SampleBrick`, which the node host bounds in bytes before it
  sends, below the channel's message limit), and `/api/bricks/{node}/{id}/preview` for the first rows and their types, read when a
  person looks at a brick and kept nowhere. `[CI: CoordinatorContractTests, StudioRoutesTests] [local: the lab's smoke.py bricks — the studio's routes]`
- **Every save into the library is checked first, and a refused one writes nothing.** The handler of
  `PUT /api/library/{id}` passes the text as received to `DesignValidator`, after the rights and the
  CSRF header and before `SaveAsync`: a job it refuses answers 422 with `LibraryRefused` (its report),
  and with no runner or no brick node online the check cannot run, so it refuses by name; never a
  commit that was not checked. It covers the studio and the pages alike, and `PUT /api/library/{id}/bindings`, which
  checks the job the library holds under the bindings it receives; `DeleteAsync` and the seeds loaded at
  start check nothing, and the arming checks a seed's references (see "A binding decides"). The push to the library's remote comes after the check and the write, never before. `[CI: StudioRoutesTests] [local: the lab's smoke.py bricks — a library save's check, a sink that refuses]`
- **A save may say which version it read.** `PUT /api/library/{id}` with `If-None-Match: *` creates
  only, with `If-Match: "<commit>"` replaces only that version: 412 otherwise, with the commit in
  place. The check runs under the library's lock. The version is the last commit of the job file,
  never the directory's: bindings saved beside it are no edit, and `GET /api/library/{id}` and the listing
  give that same commit. Without either header a save overwrites,
  as the pages do. The answer says when a deployment still runs another version (`deployed`), judged by content: the hashes
  of the job and of the bindings it was armed with, so a commit that puts the old text back is the same version. `[CI: StudioRoutesTests] [local: the lab's smoke.py
  library — a studio publishes]`
- **A save may carry the bindings it knows.** `LibrarySave.Bindings` sets the
  bindings it names into the ones the library keeps, entry by entry, in the same commit as the job; an alias it does not name
  keeps its node, and none leaves the bindings as they were. A binding is a node, a brick (its id on that node), a mode and,
  for a mode other than `strict`, a pin. **An entry that names no mode keeps the mode the library holds** (`Bindings.Merge`): a
  a publisher that names no mode must not turn a `tolerant` binding back into `strict`; `dtpipe studio` sends none. The modes are `strict`, `contract`, `compatible` and `tolerant`; a request for another is refused (400),
  and **a pin a request brings is never read**: the check writes it. A reference names its node and its brick, so the binding of its alias holds the mode and the pin
  alone, and `BrickReferences.Expand` puts the node and the brick back, inside the library's lock, whatever the request names.
  `[CI: BindingTests, StudioRoutesTests, StudioPublishTests]`
- **The library keeps a job and its bindings, no plan.** `pipelines/<id>/` holds `pipeline.yaml` and
  `bindings.json`. `[CI: LibraryRemoteTests, BindingTests]` `[unchecked: that nothing else is written there]`
- **The binding decides where a brick branch runs, and a bound branch never runs on the runner.** `bindings.json` maps the
  alias of each branch that references a brick to `{mode, pin}`; every branch that is neither a reference nor bound goes
  to the runner, which is stateless and never named. `AutoPlacer.Plan` places a referenced alias on its node or refuses the
  plan, never anywhere else: a bound branch that references no brick is an error naming the alias, the node and the
  brick. `AutoPlacer.Check` is the one function that says why bindings do not hold: a node with no identity (`unknown-node`), a
  node that does not offer the brick (`not-offered`), a mode that is none of the four (`unsupported-mode`), a binding
  in another mode that no check pinned, or pinned without the contract its mode compares, and a strict reference no check pinned (`unchecked`), a
  referenced brick whose version is no longer the pinned one (`brick-changed`), a binding for a branch
  that references no brick or is not in the job (`not-a-brick`, `no-branch`), all blocking, and a node with no connected replica
  (`offline`), which is not. A binding in a mode other than `strict` is judged by its node and its brick's id alone: the brick's
  version is whatever it was when the binding was checked. **Who a node is, the rights say; whether it is here, the inventory says.**
  `unknown-node` is a name no enabled node identity carries (a monitor's name is no node's); a node with an identity that
  no replica has announced yet (after a restart, until its host announces) is `offline`, in every mode, and its bricks are
  not known: `AutoPlacer.Plan` does not place the alias and the launch says the node has no connected replica.
  The bindings live beside the job. They are saved as given, an absent node included,
  through `PUT /api/library/{id}/bindings`, and a seed brings its own as `<id>.bindings.json`, which carries a mode and no pin: a pin is the node's keyed version. The list and the document carry
  the problems, the planner refuses a plan that has a blocking one, and a deployment is refused naming them. A deployment keeps
  the bindings it was armed with, so new bindings change nothing that runs until it is armed again.
  The designer's check (`DesignValidator`) samples each brick on the node its reference names. **A check writes the pins**: each source's, from the sample its node read
  (the brick's version and the contract that read produced), each sink's, from the contract that reaches it in the runner's dry-run
  (`Bindings.Pinned`, in the same commit as the job); a referenced brick, in every mode, gets the version it was checked at: a strict reference is `brick-changed`, and so blocks a launch, from the moment its
  brick's version moves until a person saves the pipeline again and arms it again (an armed deployment keeps the pins it was armed
  with). **An arming writes the pin of a reference that has none** (`LibraryArming`, behind `POST /api/library/{id}/deploy`): it runs the same check, saves
  the pins of those aliases alone in a commit of its own, and arms the library as it is then; a pin that exists is never moved by an arming, so a
  `brick-changed` waits for a save, and a pipeline with any other blocking problem is refused before the check runs. A contract the node did
  not send leaves a `contract` or `compatible` binding unpinned, so `unchecked`, never pinned to something that passes.
  `[CI: BindingTests, ArmedDeploymentTests (the arming), LaunchRoutesTests, StudioRoutesTests]`
- **A brick that changed since its binding was checked is judged once per version, by the node that hosts it, and a verdict that
  breaks refuses the run by name.** `BindingVerifier` gives, per non-strict binding of a launch: `unchanged` when the brick is at the
  pin's version; `tolerated` for `tolerant` with no node asked; otherwise the node's answer: a source's `CheckSource` (its own sample
  against the pinned contract, by hash in `contract`, by `dtpipe contract diff` in `compatible`: `same`, `compatible`, `breaking`),
  a sink's `CheckSink` with the pinned contract (`accepted`, `breaking`; `contract` and `compatible` are one there). The server runs
  no dtpipe. `RunQueue` asks after the plan is deployable, outside the ledger's lock, and refuses the run naming the alias, the
  node, the brick, the mode and the node's report for `breaking` and for `unread` (the node could not tell: offline, too slow, a
  version that does not know the request). A verdict is kept by pipeline, armed commit, alias, brick version and the binding's own
  hash (a bindings save moves no commit but re-pins); `unread` is never kept; `StateOf` says known, in flight or absent without
  asking a node. Limit: a `tolerant` source that changed upstream leaves the sink's pinned contract mute on the new schema. A pin
  moves only by a check a person triggers, never by a verdict. `[CI: ArmedDeploymentTests, BindingTests, Hosting.Tests BrickCheckTests]`
- **Health is read, never acted on, and a read is kept.** `PipelineHealth` says whether each armed pipeline can be launched, from
  the job at its armed commit (`IJobSource`, never the library as it is now) and its armed bindings: `AutoPlacer.Check`, the plan
  `LaunchPlanner.ChooseAsync` makes (`Launch.Causes`: no runner, no admissible runner, an edge the matrix forbids, a fragment with
  no eligible replica, anything else the planner refused) and the verdict of each binding in another mode. It asks no node and
  writes nothing (`PlanForRunAsync` is the launch's, not its). A verdict `StateOf` does not know starts once, in the background,
  on no request's token, and the pipeline is a `warning` until it ends (its end drops the kept read); a verdict the node could
  not give is remembered here for `Coordinator:Health:MaxAge`, since the verifier keeps none, so a silent node is asked once per
  age and not once per read. Every cause is `critical` (a binding that does not hold, a node absent, a brick changed beyond its
  mode, a verdict that breaks or is unread, no runner, a rejected edge, a fragment with no eligible replica, an unreadable job,
  a failed arming, a read that failed, a pipeline not armed), `warning` (a node with a connected replica that cannot run the fragment while another
  can, `ReplicasDisagree`, a verdict in flight, an arming under way) or `info`, which changes no status (a tolerant brick that
  changed, a later version in the library or none, the last run); the status is the worst. Never a `warning` for a single replica:
  nothing records how many are expected. One computation runs at a time, and its result is read again until the count of `nodes`,
  `rights`, `library`, `deploy` and `deployments` events moves or it is `MaxAge` old (5 minutes: TransportR's registrations move
  with no event), and `PipelineHealth.Unsettled` (5 seconds) old when a fragment of its plan has a connected replica that is not
  eligible, since that state ends with a registration and nothing publishes it. `GET /health/pipelines[/{id}]` (a monitor) and `GET /api/health` (the pages) answer 200 whenever the caller is
  admitted, with the status in the body; an id that is not armed is `critical`, so that a disarming alerts. "Launchable" is a
  plan that holds and a replica that could run each fragment, not that the run will succeed: the replicas' `Check` and `CheckSink`
  stay at arming and launching. `[CI: ArmedDeploymentTests (Health), TrustedIdpTests, RouteTableTests] [local: the lab's smoke.py bricks - health, ui_check.py]`
  `[unchecked: that a status is right for a state no test builds]`
- **A new state starts from the seeds, or empty.** `Coordinator:LibrarySeed` (one
  `.yaml` per pipeline, for a library that has no history, here or at its remote) and `Coordinator:RightsSeed` (for rights the store does not hold); with neither, the library has no pipeline
  and the rights no identity and an empty matrix: nobody may send anything until an administrator
  says so. **`Coordinator:Bootstrap` then adds what the deployment gives** (`RightsStore.Bootstrap`, before anything is served, in one
  change by `Coordinator:Bootstrap`, after the seed, at every start): `AdminKey`, the first administrator, a person of the id its key
  names, only while the rights hold no enabled administrator with a live key, and never over an identity of that id (one that is no
  administrator stops the start); `Identities`, each verified by its key and added when its id is absent, never changed after. Each key
  is a `SecretReference` (`@path`, or a value the environment gives: one on the command line stops the start) and names its identity;
  one that is malformed stops the start without being written. The server makes no key, and the matrix is not bootstrapped. The library is a git repository of its own, `library/` under the state directory unless
  `Coordinator:Library:Directory` puts it elsewhere, driven by the `git` command line with a fixed identity and with
  its own directory trusted (`safe.directory`, whoever owns a mounted volume; the image needs `git`), and starts with a commit of its own (empty without seeds)
  so that a write always has a commit to go back to; armed deployments and run history are in the store
  (see "A deployment is the coordinator's" and "A run is journaled as it goes"); a plan fixed by hand is in memory.
  `Coordinator:Initialize` lets every one of these begin where nothing is kept, the library's remote included.
- **The library is pushed to a git remote when one is configured, and the remote is the authority.** `Coordinator:Library:Remote:Url`
  (an `https://` URL, a `file://` URL or an absolute path; SSH is not supported), `Token`, `User` and `Timeout` are read by
  `LibraryRemoteSettings`, which refuses at start a URL that carries a user, a password, a query or a fragment, and a token for a remote
  that is not `https://`. No URL in this server carries a secret, so nothing needs one masked, and the family never references Core's
  `ConnectionStringSanitizer`. Without a remote the state directory holds the only copy of the library, which no restart replaces; with
  one, `library/` is a copy a restart reads back. **The token reaches git in the environment of the one process that talks to the remote**
  (`GIT_CONFIG_*` setting `http.<url>.extraHeader`, scoped to the URL so that a redirect elsewhere is not sent it; no git process inherits the
  `Coordinator__*` variables that hold the token and the other settings): never in `argv`, which
  `ps` lists, and never in `.git/config`, since the address too is an argument and never a configured `remote`. Set it from the
  environment, never `--Coordinator:Library:Remote:Token`. What git says goes to the log with the token replaced and is in no response.
  With a remote the library is read at start (`OpenAsync`): a remote that cannot be reached, a history that has parted from it, or a
  remote with no library when the coordinator has none either and `Coordinator:Initialize` is not true (a wrong address, or a
  remote that lost its data) stops the start; a remote that lost its data under a coordinator that may begin gets the local copy pushed back, and with no copy
  the seeds. **A write aligns, writes, pushes** (`PipelineLibrary.Written`, under the library's one lock): it fetches the remote's `main` and
  puts the library on it (behind, it moves forward; ahead, it drops what the remote lacks and logs each commit; parted, it refuses naming both
  heads), then writes and commits, then pushes without force. A failure from the write on puts the files and the history back exactly (`reset --hard` to the earlier `HEAD`,
  `clean` of the pipeline's directory, which a new pipeline would otherwise leave), so a refused save leaves nothing; a write that commits nothing
  pushes nothing. The write, the push and the undo run on no request token, and the push is bounded by `Timeout` (the process and
  its children are ended). The answer is a 503 `{error}` in this server's words: *could not be reached or refused the credentials*, *refused the save*
  (the remote answered about the ref: a hook, a right, a history it rejects) or *did not answer in time*. **Only a fetch that failed and a push the remote refused
  guarantee that nothing was kept**; after any other failure the push may have reached the remote, and the save shows here at the next save or
  start. The studio's contract is unchanged: it reads the `error` of a status it does not know. `JobAtAsync` (every launch, the health read, the binding verifier)
  reads an immutable commit without the lock once the library is open, so no launch waits for a save or for the remote; every other read waits for a
  push in flight, `Timeout` at most. `[CI: LibraryRemoteTests]` `[unchecked: a git server that goes down in the middle of a push and
  applies it; an https remote with a token; Windows; the health read after an alignment, which publishes no event and so may stay stale up to Coordinator:Health:MaxAge]`
- **Only a plan in `PlanMode.Manual` sends a data node a fragment written as a job.** No route of the product plans that
  way; the lab's coordinator's `/api/plan` and `/api/deploy` do, and only to a host that announces `raw-fragments`; every other
  caller plans in `Library`. `[CI: PlanModeTests] [local: the lab's smoke.py lab and sandbox (through /api/deploy), ui_check.py
  (the Lab view plans through /api/plan)]` `[unchecked: a new route that plans in Manual]`
- **A job names a brick by reference, a node holds its text, and the reference decides the placement.** A source is read
  by `input: brick:<node>/<brick>`, a sink written by `output: brick:<node>/<brick>` with the one branch it reads in `from`
  (`DtPipe.Jobs/BrickRef`, which only the distributed family and the designer read: the engine never sees `brick:`); a branch that
  says more, or reads none or several, is a misuse (`BrickRef.Misuse`, `not-a-brick`). A node announces a brick by its id, kind,
  title, description and **version**, never its text, its variables or its schema: the version is the SHA-256 of the canonical
  text (`BrickRules.Canonical`), so it changes exactly when what dtpipe would run changes. A secret written in the text would be an
  oracle for offline guesses, so a brick keeps its secrets in `${{NAME}}` or `keyring://` references. The coordinator composes a fragment from ids and versions it by the versions of its bricks
  (`FragmentJob.VersionOf`); the node composes the job from its own declarations and compares. A reference names its node and its
  brick, so `bindings.json` keeps for it the mode and the pin only: `BrickReferences.Expand` puts the node and the brick back for
  everything that reads a `Binding`, and `Strip` takes them out before a write. `BrickCatalog` keeps what it reads of each
  announced brick and reads a brick announced again anew; the node's name and whether it is online are read at every call, so a
  brick is offline as soon as its node has no connected replica, one in its grace period included. A branch that holds text is
  no brick: nothing matches a branch against a text. The planner never takes a branch apart.
  `[CI: BrickCatalogTests, MinimalBrickTests] [local: the lab's smoke.py, ui_check.py]` `[unchecked: concurrent readers of the branch
  kept per brick; that an entry goes with its announcement]`
- **A fragment declares each edge alias once**, so a branch that writes somewhere, is read on its own node or feeds
  several nodes sends on through a relay of its own alias, never with a second `output: arrow:-` on itself.
  `[local: the lab's smoke.py lab]`
- **A plan's edges join fragments of the plan.** A fragment the planner refuses is not in `Fragments`, and neither is an edge
  that names it, so a plan with errors still reads as a whole to the page, the run and any client of `/plan`.
  `[CI: PlanModeTests]`
- **The server designs nothing.** A pipeline reaches the library by a save that is checked
  (`PUT /api/library/{id}`) from `dtpipe studio`, whose designer is `DtPipe.Jobs`' (rule in the root
  `CLAUDE.md`); the pages list, read, restore, bind and run it, and show its job read only. A
  brick's canonical form, and its version under a given key, are pinned by `BrickFormTests`: changing either, or
  the key, gives every brick a new version. `HashValueTests` pins the values of the hashes the server compares or shows (a saved
  plan's job, a brick's version, a replica's bricks), and `Hosting.Tests FragmentJobTests` a fragment's version;
  they are computed by `ContentHash` (`DtPipe.Coordinator.Contracts`).
  `[CI: DtPipe.Coordinator.Server.Tests, Hosting.Tests FragmentJobTests] [local: the lab's smoke.py, ui_check.py]`
  `[unchecked: a site that hashes on its own]`
- **A deployment is the coordinator's, and nothing of it is left on a node.** A library pipeline is *armed*: `{pipeline, commit
  of the job, the bindings at that commit, when, by whom}` (`ArmedDeployment`), kept in the store once its deployment is ready and read
  back at start as ready, with no check, since no replica is connected yet. A run reads the job at the armed
  commit (`IJobSource`, the library) and the armed bindings, never the library as it is now. A node whose identity is gone stays
  armed, and a run is refused naming it. `DeployAsync` deploys a plan fixed by hand, in memory only. Deploying plans as a launch
  would, then asks every connected replica of each node whether it would run the fragments (`Check`, answered, spawning nothing)
  and is ready when each fragment has a replica that said yes and that admission could place a run on. One
  replica that refuses does not fail it while another can run the fragment (a rolling update leaves some at
  the old brick); it fails at once only when every replica asked has answered and none said yes, naming the
  node and why (`refused by <node>: …`), and a refused deployment is not kept. A replica that arrives
  later is as good as one that was asked, and one that goes takes nothing with it: what decides a run is
  admission, at the run. A deployment's gate is held while it is deployed or removed; a run passes it to
  read the state and does not hold it. `[CI: DeploymentManagerTests, RunQueueTests, ArmedDeploymentTests] [local: the lab's smoke.py]`
- **The armed deployments are in the store, read once, and memory is the truth.** `DeploymentStore` is a thin face over
  `IDeploymentStore` (`SqlDeploymentStore`): the `deployments` table, a row per pipeline holding its JSON document, on the
  connection every store borrows. `DeploymentService` loads it when it is built, so a store that cannot be read stops the start; a
  document that cannot be read, or that names another pipeline than its row, is skipped with a warning, and its pipeline is then not
  armed (`/health/pipelines/{id}` says `critical`, `not-armed`, for a supervisor that asks for it, and the pipeline is absent from the
  listing). A write names no version: the deployment's gate and `ThrowIfBusy` serialize the writers of one pipeline and one
  coordinator writes. **The store is written before memory changes and never under the ledger's lock.** A deployment replaces another
  only after the store has forgotten the earlier one; an armed deployment is ready only after the store has kept it; an undeploy
  forgets it in the store before memory does. A store that refuses (`StoreUnavailableException`, 503) therefore leaves the earlier
  deployment as it was, or, when it refuses the record of a deployment whose check passed, removes that deployment from memory and
  answers 503 (the clean-up that follows only logs when it fails, so the error the caller reads is the one that ended the arming). An id that is no library id
  (a plan fixed by hand) has no record and never reaches the store. A store that comes back empty is no failure to open: a pipeline with
  no record is not armed and nothing runs, which `/health` reports. **Limit**: a write that timed out may have been applied. An arming whose
  record was kept while memory refused it is read back ready at the next start, with no check; an undeploy or a replacement whose
  forgetting was kept while the call timed out leaves the deployment in memory until the next start, when it is gone. Arm the pipeline
  again (it replaces the record), or undeploy it after the restart.
  `[CI: ArmedDeploymentTests, DeploymentRoutesTests; SqlStoreTests]`
  `[local: lab.sh stack restart-check — an armed pipeline read back, on either store]` `[unchecked: a write that times out and is applied]`
- **The hosts set aside are in the store, read once, and an announcement never waits on it.** `EnrollmentQueue` loads an
  `IIgnoredHostStore` (`SqlIgnoredHostStore`: the `ignored_hosts` table, a row per subject with the instant it is let
  back in) when it is built, and the server builds it at start, so a store that cannot be read stops the start. `Propose`, which every
  announcement runs under the queue's lock, asks memory only. `Ignore` writes the store first, outside that lock, then memory; a store
  that refuses (`StoreUnavailableException`, 503) leaves the host waiting and not set aside. The instant is absolute, so a restart does
  not extend the 24 hours, and the bound `MaxIgnored` is kept by the store's write as by memory, the subject let back in soonest going
  first. The hosts that wait stay in memory: a host announces again within seconds, and what it proposed is untrusted text that
  expires with the host. **Limit**: an `Ignore` that timed out may have been applied, and then the host is set aside in the store
  while it still waits in memory until it announces again after the next start.
  `[CI: EnrollmentQueueTests, RightsRoutesTests; SqlStoreTests]`
- **Deployments and runs are decided under one lock.** `DeploymentManager` is what the routes call, over three
  parts and the one `DeploymentLedger` they share: `DeploymentService` (the deployments), `RunQueue` (the queue
  and the dispatch) and `FaultInjection`. A decision that reads both sides, such as a deployment refused while a run of
  its pipeline is queued or in flight, is taken under the ledger's lock; give a part no lock of its own.
  `[CI: RunQueueTests]` `[unchecked: the lock itself]`
- **One run at a time per data node, first come first served.** `RunDispatch` decides, at each
  enqueue and each finish, which waiting runs `RunQueue` sends: one that no run already
  sent, and no older run still waiting, shares a data node with. A run's data nodes are the nodes its deployment's bindings name
  (a fixed plan's, its fragments' nodes) that the inventory does not list as runners; a node it does not know counts
  (fail-closed). A run holds them from being sent until it finishes, and is never sent twice. Its plan is computed afterwards, and
  a plan that reaches a data node the run does not hold is refused (`RunDispatch.Unheld`). A runner never counts, so its fragments of two runs share
  its host with no resource bound. With no id, `/api/runs/cancel` refuses (409) while several runs
  are in flight rather than guess. `[CI: RunDispatchTests, RunQueueTests, ArmedDeploymentTests] [local: the lab's smoke.py library and sandbox]`
- **A node is one identity carried by one or more replicas, and `ReplicaRegistry` is the one table of
  them.** `NodeInventory` adds what the registry does not hold: the name the rights give the node, the
  groups, the last announcement of a node with no replica connected, and the page's view. The node is
  online while any replica is connected, and shows the announcement of the connected replica that
  arrived last (a reconnection of the same host does not change which); `NodeView.Replicas` lists each
  with the runs placed on it and a hash of its bricks, and `ReplicasDisagree` says two connected replicas
  offer different bricks. A read (a preview, a sample, a sink check) goes to one connected
  replica, the one with the fewest runs, and is not tried on a second when it fails. `Disconnect` interrupts
  the connections of every replica of an identity, and `PUT /api/rights/identities/{id}` does it when the
  name or the **groups** change, or when the identity goes from enabled to disabled: a replica's data plane
  client holds the groups it registered with, and until it registers again admission places no run on it; a
  disabled identity's replicas are cut at once, and the runs placed on them end with them. Enabling an
  identity again cuts nothing, no host of it being connected. An edit that leaves out a field (`name`,
  `description`, `groups` or `enabled`) leaves it as the identity has it: the omission neither enables, renames
  nor empties. `Origin`, where a replica says it runs, is untrusted
  text: cut at `Announce`, the ground of no decision.
  `[CI: NodeChannelHubTests, RightsRoutesTests] [local: the lab's smoke.py rights - identity, disabled; tools/campaign.py F3, S7]`
- **A run names who launched it, and a service may launch it only once.** `RunView.LaunchedBy` is a person's email or the id of
  the identity that launched it, a service or a person verified by a key, or `CallerOf`'s name for a person of the provider with
  none (an id holds no `@`, and no two identities share one, so a
  launcher's id names its own runs only); `RunQueue.Launch` takes the author and an optional `Idempotency-Key`, which means something only
  with its author: the lookup is `(LaunchedBy, key)` among the runs queued or in flight and the journaled ones, taken under the
  ledger's lock where the run is queued, and a run is journaled before it leaves the ledger, so a retry that races the end of the
  first finds it in one or the other. A repeat answers the first run whatever became of it (a new attempt takes a new key), the same
  key for another pipeline is a conflict, and a request with no key is never a repeat. The key lives as long as its run is
  journaled (`IRunStore.Window`, applied at start as when recording); a run in flight when the server stops is ended at the next
  start (`RunJournal.RestartedRefusal`) and answers its key as that run. `RunJournal.Find` scans every kept run, `List`'s limit being a page.
  `[CI: RunQueueTests, RunJournalTests, ServiceRoutesTests] [local: the lab's smoke.py bricks - a service launches a run]`
- **A state is written as a lowercase word, by the converter on the property that carries it.** `RunView.State` and
  `DeploymentView.State` are a `RunState` and a `DeploymentState`; the pages, the event stream and the journal's documents
  carry the name of the value in lowercase, never as declared. Where no property carries a deployment's state (the
  `deploy` event, the text of a refusal), `StateWords.Word` writes it; `undeployed` is the word of an event, not a
  state. A journal document whose state is none of the values, or a number, is skipped when the server starts.
  `[CI: StateWireTests, RunQueueTests]`
- **A run is journaled as it goes, and a restart ends what it interrupted.** `RunJournal` keeps the runs in an `IRunStore`, which is
  `SqlRunStore`: the `runs` table (a document per run, whether it is over, its instant), the `owed` table and the `counters` table that draws ids. `RunQueue` draws the id and writes the run when it is queued (under the ledger's lock, before
  the ticket is visible), when it starts and when it ends. History (`List`, `Find`, `PipelineHealth`) holds the runs that are over and
  is read from memory, loaded at start: one coordinator writes the store, so the window in memory is the store's, and a key's
  lookup `(LaunchedBy, key)` costs no call to the store under the ledger's lock. **A run not yet over that the store cannot keep
  throws `StoreUnavailableException`** (503 from the middleware of `CoordinatorServer`), so none is queued or started
  without its record; a run that is over is kept in memory whatever the store says and only logged, and a store that missed it
  leaves it in flight for the next start to end. A call that finds the connection lost fails at once. At start `RunJournal` gives each run it finds queued or running the state `error` and `RestartedRefusal`, and owes
  the fragments of the ones that had started (`RunJournal.Owed`, written to the store before the run is ended, and kept until a replica gives
  the fragment up or a day passes); `OrphanRuns` asks every replica of an owed fragment's node, once, when it announces with
  its data plane client (`NodeChannelHub.Announce`, off the hub's thread), to abandon it, until a replica answers that it held it,
  so a second restart before the replicas announce keeps the obligation; the authority is the journal, never what an announcement says. **An id is never given twice**: the counter is raised at start to
  the highest id read and never lowered, and a store with no counter and no run (a first start, or a database that lost its data) refuses
  to start unless `Coordinator:Initialize` is true, since the ids would begin again. A write is durable before its call returns (`synchronous=FULL` on SQLite, the commit on a server).
  A reason that goes into `RunView.Refusal` is the server's own text, never a driver's message.
  A run that had started carries `RunView.SinkMayBePartial` (also in `/service/runs`), true: its fragments had been launched and a sink may hold part of
  what they wrote. It is a label only: nothing ranks resumption strategies or reruns the run, and a run that was only queued, or that ended any other way, has none.
  `[CI: RunJournalTests (the label), RunQueueTests, OrphanRunsTests, NodeChannelHubTests; SqlStoreTests] [local: tools/campaign.py F4]` `[unchecked: a wired
  child with nothing to write ends only once a relay touches its torn-down transfer]`
- **A run that was placed records the replica of each fragment.** `RunJournal` keeps the `RunResult` whole,
  `RunResult.Replicas` included: an instance id and the `Origin` the replica announced, untrusted text the pages
  escape. A run refused at admission has none, and a document that names none reads as a run with none.
  `[CI: RunJournalTests]` `[unchecked: the pages' escaping of Origin]`
- **A run names the subject and the groups of each fragment's node.** `RunQueue` reads them from the
  rights when the run starts, through `LaunchPlanner.RunFragmentsOf` (`RightsStore.SubjectOf`: the client id of
  the identity named after the fragment's node, the `sub` its tokens carry; `GroupsOf`), and puts them in each
  `RunFragment`; admission
  then places the run only on replicas whose connection attests that subject and whose data plane client holds
  exactly those groups. A node that no identity carries any more, or whose identity is disabled, refuses the
  run (`refused`), and the server never sets `AdmissionOptions.AdmitUnboundFragments`.
  `[CI: RightsStoreTests, DeploymentManagerTests (disabled, removed), RunQueueTests] [local: the lab's smoke.py rights - a foreign replica]`
- **One source of rights.** `RightsStore` holds the identities and the flow matrix;
  `RightsFlowControl` replaces TransportR's `IFlowControlService`, so the hub checks the matrix as
  it is now on every transfer, and the planner and a deployment check every edge against it, each by one
  pass of `IPlanRegistry` (`PlanFlow`) and in the same sentence, a plan made under an earlier matrix included. A
  node in several groups sends only if every one of its groups may send to every group of the
  receiver, on both sides, and an edge carries the groups of both ends: a PlannedEdge's ProducerGroups
  accumulates the groups of every upstream unit (DependsOn) transitively alongside the producer's own groups,
  so an intermediate runner hop requires authorization from each upstream source group to the consumer groups. Never add a second matrix. The matrix changes whole
  (`PUT /api/rights/matrix`) or by edges added beside it, which revoke nothing, so two administrators who add at once keep both:
  `POST /api/rights/matrix/edges` (the page sends the pairs of groups a plan's refused edges lack), and `exchangeWithRunners` on a
  node added or accepted, in the same change as the identity, both ways between its groups and those the rights give every runner the
  inventory knows (the rights do not say which node is a runner, its announcement does; none known refuses the request).
  `[CI: FlowMatrixEvaluationTests, MatrixEdgesTests, LaunchRoutesTests, PlanRightsTests] [local: the lab's smoke.py rights]`
- **A library pipeline is planned when it is launched, and the runner is chosen by one function.** `RunQueue.RunOneAsync`, once
  the run is sent and outside the ledger's lock, asks `LaunchPlanner.ChooseAsync` for the plan of the armed job under the armed
  bindings, and so do `GET /api/library/{id}/plan` (the plan a launch would make now, nothing written) and arming. The
  runners online are tried by runs in flight, then by name; the first whose plan is deployable (the planner already judges its edges
  against the matrix, which is how a runner in the wrong groups is skipped) and whose every fragment has an eligible replica is
  used. A plan that does not use the runner fails whichever is chosen and is returned at once; when none serves, the refusal
  names each runner and why. `NodeInventory.PreferredRunner` is for reads that need any runner, never a plan. The groups a plan
  names are the rights' at that moment, so a node's groups changing needs no redistribution.
  `[CI: ArmedDeploymentTests, LaunchRoutesTests, PlanRightsTests, RightsStoreTests] [local: the lab's smoke.py rights - several groups]`
- **A run records the plan it ran.** `RunView.Plan` (`RunPlan`): the plan's id, the runner it used, each fragment at its version, and
  each binding with the brick version it met and its mode's verdict. A run refused before it had a plan has none.
  `[CI: ArmedDeploymentTests, RunJournalTests]`
- **A cursor is kept per sink branch, and only at the verdict of the run.** `CursorKeeper` holds a `CursorRecord` per (pipeline, sink
  alias) in memory (the truth, read once at start) and in `ICursorStore` (the `cursors` table; the
  store is written first). `RunQueue.RunOneAsync` asks `Launches` after the plan, which puts the kept value in the `RunFragment` of the
  one source brick upstream of the sink (through `plan.Units` dependencies), and `Commit` after `RunOutcome.Succeeded`, before the run
  leaves the ledger, so the next run of the pipeline reads it; the run records what became of each cursor (`RunView.Cursors`: kept, or the
  reason it was not, a store that refuses included, which leaves the run succeeded). The record keeps the versions of the sink and source
  bricks (`CursorBasis`); `Launches` refuses the run by name when either changed, a sink or source is gone, two cursors read from one
  fragment, or a sink has several sources. A person's `PUT` before any run keeps no basis: the first run adopts it. `RunQueue.Launch`
  refuses a launch while a run of a pipeline that has a cursor is queued or in flight, and so do the reset and set routes (both under
  the ledger's lock); before the first cursor exists the dispatch's one-run-per-data-node already serialises two runs. The routes are
  `CoordinatorApi.CursorsRoute` and `CursorRoute`: the reader group reads, the designer group (the right to launch) sets and resets, and
  `/service` has none. `[CI: CursorRunTests, CursorRoutesTests, SqlStoreTests, RouteTableTests]` `[unchecked: a store write that times out and is applied]`
- **A data node only reads and writes, and runs only its bricks, which it composes by id.**
  `PlanBuilder` gives a data node's fragment as `FragmentUnit`s and composes the job with
  `FragmentJob` (`DtPipe.Coordinator.Contracts`), the function the node calls again on its own
  declarations, so both hash the same bytes; a branch that is none of the node's bricks is a planner
  error naming why (`BrickRules`), never a job sent to run. Only planning by hand
  (`PlanMode.Manual`) sends a job as written, and only to a host that announces `raw-fragments`; the library
  and `AutoPlacer` never do. `BrickRules.Processors` is the one list of processors, shared with `JobGraph`;
  it must equal the `stream-processor` entries of `dtpipe providers --format json`. The replica decides again at
  `Launch`, with the same function. `[CI: PipelineNode.Tests BrickRulesProcessorsTests] [local: the lab's smoke.py
  bricks, sandbox]`
- The pages are this library's `wwwroot`, which `dotnet publish` puts at the root of the `wwwroot` beside any host that
  references it, beside the host's own pages: `index.html` + `js/` (the coordinator's views, ES modules, no build). `/healthz`, the protected resource
  metadata (`CoordinatorApi.Metadata`, RFC 9728: the provider, the contract, the studio's client) and
  the key issuer's routes are the only anonymous routes, with `/livez`; `/health` is a monitor's and `/service` a launcher's. Verify a change with the lab's `./lab.sh smoke`, and a page change with
  `./lab.sh ui` too.

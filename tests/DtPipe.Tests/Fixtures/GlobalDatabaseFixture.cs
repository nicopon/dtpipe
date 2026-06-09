using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DtPipe.Tests.Helpers;
using Testcontainers.PostgreSql;
using Xunit;

namespace DtPipe.Tests.Fixtures;

public class GlobalDatabaseFixture : IAsyncLifetime
{
    private const string SqlServerPassword = "yourStrong(!)Password";
    private const string OraclePassword = "password";

    private PostgreSqlContainer? _postgres;
    private IContainer? _sqlServer;
    private IContainer? _oracle;

    public string? PostgresConnectionString { get; private set; }
    public string? SqlServerConnectionString { get; private set; }
    public string? OracleConnectionString { get; private set; }
    // Only set in REUSE_INFRA mode: Testcontainers containers start fresh, no reset needed.
    public string? OracleAdminConnectionString { get; private set; }

    public async ValueTask InitializeAsync()
    {
        if (!DockerHelper.IsAvailable()) return;

        if (DockerHelper.ShouldReuseInfrastructure())
        {
            PostgresConnectionString = DockerHelper.LocalPostgreSqlConnectionString;
            SqlServerConnectionString = DockerHelper.LocalSqlServerConnectionString;
            OracleConnectionString = DockerHelper.LocalOracleConnectionString;
            OracleAdminConnectionString = DockerHelper.LocalOracleAdminConnectionString;
            await OracleSchemaHelper.ResetSchemaAsync(OracleAdminConnectionString);
            return;
        }

        var tasks = new List<Task>();

        // Postgres
        tasks.Add(Task.Run(async () =>
        {
            _postgres = new PostgreSqlBuilder("postgres:18-alpine")
                .Build();
            await _postgres.StartAsync();
            PostgresConnectionString = _postgres.GetConnectionString();
        }));

        // SQL Server — generic builder to avoid MsSqlBuilder's sqlcmd readiness check,
        // which is unavailable on azure-sql-edge (the only multi-arch MSSQL image).
        tasks.Add(Task.Run(async () =>
        {
            _sqlServer = new ContainerBuilder("mcr.microsoft.com/azure-sql-edge:latest")
                .WithPortBinding(1433, true)
                .WithEnvironment("ACCEPT_EULA", "Y")
                .WithEnvironment("MSSQL_SA_PASSWORD", SqlServerPassword)
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilInternalTcpPortIsAvailable(1433, x => x.WithTimeout(TimeSpan.FromMinutes(2))))
                .Build();
            await _sqlServer.StartAsync();
            var port = _sqlServer.GetMappedPublicPort(1433);
            SqlServerConnectionString = $"Server=localhost,{port};Database=master;User Id=sa;Password={SqlServerPassword};TrustServerCertificate=True";
        }));

        // Oracle — generic builder on oracle-free (multi-arch, no shm-size requirement).
        // OracleBuilder is hardcoded for oracle-xe (SERVICE_NAME=XE); oracle-free uses SERVICE_NAME=FREE.
        tasks.Add(Task.Run(async () =>
        {
            _oracle = new ContainerBuilder("gvenzl/oracle-free:slim")
                .WithPortBinding(1521, true)
                .WithEnvironment("ORACLE_PASSWORD", OraclePassword)
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilMessageIsLogged("DATABASE IS READY TO USE!", x => x.WithTimeout(TimeSpan.FromMinutes(5))))
                .Build();
            await _oracle.StartAsync();
            var port = _oracle.GetMappedPublicPort(1521);
            OracleConnectionString = $"User Id=system;Password={OraclePassword};Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT={port}))(CONNECT_DATA=(SERVICE_NAME=FREE)))";
        }));

        await Task.WhenAll(tasks);
    }

    public async ValueTask DisposeAsync()
    {
        var tasks = new List<Task>();

        if (_postgres != null) tasks.Add(_postgres.DisposeAsync().AsTask());
        if (_sqlServer != null) tasks.Add(_sqlServer.DisposeAsync().AsTask());
        if (_oracle != null) tasks.Add(_oracle.DisposeAsync().AsTask());

        await Task.WhenAll(tasks);
    }
}

namespace DtPipe.Coordinator.Server;

/// <summary>Registers the stores that keep the coordinator's state beyond the process, on the store the settings choose.</summary>
internal static class StoreRegistration
{
    /// <summary>
    /// The rights, the run journal, the armed deployments, the cursors, the hosts set aside, the session keys and the key issuer's signing
    /// keys, all in one database (<see cref="SqlDatabase"/>: a SQLite file, a PostgreSQL or a SQL Server database), opened when the first of
    /// them is asked for, which the server does before it serves. The signing keys' shelf is keyed (<see cref="IssuerKeys.Shelf"/>):
    /// DataProtection reads the one unkeyed repository, and would read any other element there as a key of its own.
    /// </summary>
    public static IServiceCollection AddCoordinatorStores(this IServiceCollection services, CoordinatorServerOptions server)
    {
        // A database lost stops the coordinator with an exit code that says it failed.
        services.AddSingleton(provider => SqlDatabase.Open(
            DialectOf(server), server.Initialize, server.StoreLockWait, provider.GetRequiredService<ILoggerFactory>().CreateLogger<SqlDatabase>(),
            () =>
            {
                Environment.ExitCode = 1;
                provider.GetRequiredService<IHostApplicationLifetime>().StopApplication();
            }));
        services.AddSingleton<IRightsStore, SqlRightsStore>();
        services.AddSingleton<IRunStore, SqlRunStore>();
        services.AddSingleton<IDeploymentStore, SqlDeploymentStore>();
        services.AddSingleton<ICursorStore, SqlCursorStore>();
        services.AddSingleton<IIgnoredHostStore, SqlIgnoredHostStore>();
        services.AddSingleton<Microsoft.AspNetCore.DataProtection.Repositories.IXmlRepository, SqlXmlRepository>();
        services.AddKeyedSingleton<Microsoft.AspNetCore.DataProtection.Repositories.IXmlRepository>(IssuerKeys.Shelf, (provider, _) =>
            new SqlXmlRepository(provider.GetRequiredService<SqlDatabase>(), "issuer_keys", "signing key"));
        return services;
    }

    private static StoreDialect DialectOf(CoordinatorServerOptions server) => server.Store switch
    {
        StoreKind.PostgreSql => new PostgreSqlDialect(server.ConnectionString!),
        StoreKind.SqlServer => new SqlServerDialect(server.ConnectionString!),
        _ => new SqliteDialect(server.SqliteDirectory ?? server.StateDir),
    };
}

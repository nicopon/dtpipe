namespace DtPipe.Coordinator.Server;

/// <summary>Registers the stores that keep the coordinator's state beyond the process, on the store the settings choose.</summary>
internal static class StoreRegistration
{
    /// <summary>
    /// The rights, the run journal, the armed deployments, the cursors, the hosts set aside, the session keys and the key issuer's signing
    /// keys, all on one store: a Redis connection or a database (<see cref="SqlDatabase"/>, a SQLite file), opened when the first of them is asked for, which the server does before
    /// it serves. The signing keys' shelf is keyed (<see cref="IssuerKeys.Shelf"/>): DataProtection reads the one unkeyed repository, and
    /// would read any other element there as a key of its own.
    /// </summary>
    public static IServiceCollection AddCoordinatorStores(this IServiceCollection services, CoordinatorServerOptions server)
    {
        if (server.Store == StoreKind.Redis)
        {
            services.AddSingleton(provider => RedisConnection.Connect(server.Redis, provider.GetRequiredService<ILoggerFactory>().CreateLogger<RedisConnection>()));
            services.AddSingleton<IRightsStore, RedisRightsStore>();
            services.AddSingleton<IRunStore, RedisRunStore>();
            services.AddSingleton<IDeploymentStore, RedisDeploymentStore>();
            services.AddSingleton<ICursorStore, RedisCursorStore>();
            services.AddSingleton<IIgnoredHostStore, RedisIgnoredHostStore>();
            services.AddSingleton<Microsoft.AspNetCore.DataProtection.Repositories.IXmlRepository, RedisXmlRepository>();
            services.AddKeyedSingleton<Microsoft.AspNetCore.DataProtection.Repositories.IXmlRepository>(IssuerKeys.Shelf, (provider, _) =>
                new RedisXmlRepository(provider.GetRequiredService<RedisConnection>(), "issuer-keys", "signing key"));
        }
        else
        {
            services.AddSingleton(provider => SqlDatabase.Open(
                new SqliteDialect(server.SqliteDirectory ?? server.StateDir), server.Initialize, provider.GetRequiredService<ILoggerFactory>().CreateLogger<SqlDatabase>()));
            services.AddSingleton<IRightsStore, SqlRightsStore>();
            services.AddSingleton<IRunStore, SqlRunStore>();
            services.AddSingleton<IDeploymentStore, SqlDeploymentStore>();
            services.AddSingleton<ICursorStore, SqlCursorStore>();
            services.AddSingleton<IIgnoredHostStore, SqlIgnoredHostStore>();
            services.AddSingleton<Microsoft.AspNetCore.DataProtection.Repositories.IXmlRepository, SqlXmlRepository>();
            services.AddKeyedSingleton<Microsoft.AspNetCore.DataProtection.Repositories.IXmlRepository>(IssuerKeys.Shelf, (provider, _) =>
                new SqlXmlRepository(provider.GetRequiredService<SqlDatabase>(), "issuer_keys", "signing key"));
        }
        return services;
    }
}

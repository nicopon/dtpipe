namespace DtPipe.Coordinator.Server;

/// <summary>Registers the stores that keep the coordinator's state beyond the process, on the store the settings choose.</summary>
internal static class StoreRegistration
{
    /// <summary>
    /// The rights, the run journal, the armed deployments, the hosts set aside and the session keys, all on one store: a Redis
    /// connection or a SQLite file, opened when the first of them is asked for, which the server does before it serves.
    /// </summary>
    public static IServiceCollection AddCoordinatorStores(this IServiceCollection services, CoordinatorServerOptions server)
    {
        if (server.Store == StoreKind.Redis)
        {
            services.AddSingleton(provider => RedisConnection.Connect(server.Redis, provider.GetRequiredService<ILoggerFactory>().CreateLogger<RedisConnection>()));
            services.AddSingleton<IRightsStore, RedisRightsStore>();
            services.AddSingleton<IRunStore, RedisRunStore>();
            services.AddSingleton<IDeploymentStore, RedisDeploymentStore>();
            services.AddSingleton<IIgnoredHostStore, RedisIgnoredHostStore>();
            services.AddSingleton<Microsoft.AspNetCore.DataProtection.Repositories.IXmlRepository, RedisXmlRepository>();
        }
        else
        {
            services.AddSingleton(provider => SqliteDatabase.Open(
                server.SqliteDirectory ?? server.StateDir, server.Initialize, provider.GetRequiredService<ILoggerFactory>().CreateLogger<SqliteDatabase>()));
            services.AddSingleton<IRightsStore, SqliteRightsStore>();
            services.AddSingleton<IRunStore, SqliteRunStore>();
            services.AddSingleton<IDeploymentStore, SqliteDeploymentStore>();
            services.AddSingleton<IIgnoredHostStore, SqliteIgnoredHostStore>();
            services.AddSingleton<Microsoft.AspNetCore.DataProtection.Repositories.IXmlRepository, SqliteXmlRepository>();
        }
        return services;
    }
}

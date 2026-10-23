using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// F10 — Core boundary deny-list. Concrete infrastructure types must NOT live in the
/// DtPipe.Core assembly: they belong to DtPipe.Adapters.Shared (SQL writers, dialects,
/// retry policy, cursor persistence) or, for Arrow bridges, outside Core entirely.
/// Each name must also be found in DtPipe.Adapters.Shared, or a stale name passes vacuously.
/// </summary>
public class CoreBoundaryTests
{
    [Theory]
    [InlineData("DtPipe.Adapters.Shared.Abstractions.BaseSqlDataWriter")]
    [InlineData("DtPipe.Adapters.Shared.Dialects.BaseSqlDialect")]
    [InlineData("DtPipe.Adapters.Shared.Dialects.PostgreSqlDialect")]
    [InlineData("DtPipe.Adapters.Shared.Dialects.OracleDialect")]
    [InlineData("DtPipe.Adapters.Shared.Dialects.SqlServerDialect")]
    [InlineData("DtPipe.Adapters.Shared.Dialects.SqliteDialect")]
    [InlineData("DtPipe.Adapters.Shared.Dialects.DuckDbDialect")]
    [InlineData("DtPipe.Adapters.Shared.Infrastructure.Retry.DatabaseRetryPolicy")]
    [InlineData("DtPipe.Adapters.Shared.Cursor.CursorStateStore")]
    public void Core_Assembly_Does_Not_Contain_Concrete_Infrastructure(string typeName)
    {
        var type = typeof(DtPipe.Core.Options.OptionsRegistry).Assembly.GetType(typeName);
        Assert.True(type is null, $"{typeName} must not be in the DtPipe.Core assembly.");

        var shared = typeof(DtPipe.Adapters.Shared.Dialects.BaseSqlDialect).Assembly.GetType(typeName);
        Assert.True(shared is not null, $"{typeName} must be in the DtPipe.Adapters.Shared assembly.");
    }

    [Fact]
    public void Core_Assembly_Keeps_Engine_Contracts()
    {
        // Engine contracts remain in Core (compile-time references double as the check).
        Assert.NotNull(typeof(DtPipe.Core.Abstractions.Dag.IDagOrchestrator));
        Assert.NotNull(typeof(DtPipe.Core.Abstractions.Dag.IMemoryChannelRegistry));
        Assert.NotNull(typeof(DtPipe.Core.Abstractions.IStreamReader));
        Assert.NotNull(typeof(DtPipe.Core.Abstractions.ISqlDialect)); // abstraction stays
        Assert.NotNull(typeof(DtPipe.Core.Pipelines.Dag.BranchDefinition));

        // …and they live in the Core assembly, not elsewhere.
        Assert.Equal(typeof(DtPipe.Core.Options.OptionsRegistry).Assembly, typeof(DtPipe.Core.Abstractions.Dag.IDagOrchestrator).Assembly);
        Assert.Equal(typeof(DtPipe.Core.Options.OptionsRegistry).Assembly, typeof(DtPipe.Core.Pipelines.Dag.BranchDefinition).Assembly);
    }
}

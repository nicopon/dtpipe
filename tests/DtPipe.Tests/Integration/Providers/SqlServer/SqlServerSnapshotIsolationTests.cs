using DtPipe.Adapters.SqlServer;
using DtPipe.Core.Options;
using AwesomeAssertions;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DtPipe.Tests.Integration.Providers.SqlServer;

/// <summary>
/// The reader asks for Snapshot isolation, which a database only grants once
/// ALLOW_SNAPSHOT_ISOLATION is ON — and a freshly created one has it OFF. This covers the state
/// the shared integration database cannot: it was created with snapshot already allowed, so every
/// other SQL Server test passes whatever the reader does here.
///
/// The query must read a real table. SQL Server raises error 3952 when a statement first touches a
/// user object, so <c>SELECT 1</c> succeeds under a snapshot transaction the database never
/// authorised, and a test written that way goes green without proving anything.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class SqlServerSnapshotIsolationTests : IAsyncLifetime
{
	private readonly Fixtures.GlobalDatabaseFixture _fixture;
	private readonly string _database = $"snapshot_off_{Guid.NewGuid():N}";
	private string? _masterConnection;

	public SqlServerSnapshotIsolationTests(Fixtures.GlobalDatabaseFixture fixture) => _fixture = fixture;

	private string DatabaseConnection =>
		new SqlConnectionStringBuilder(_masterConnection!) { InitialCatalog = _database }.ConnectionString;

	public async ValueTask InitializeAsync()
	{
		_masterConnection = _fixture.SqlServerConnectionString;
		if (_masterConnection == null) return;

		await using var connection = new SqlConnection(_masterConnection);
		await connection.OpenAsync();

		await using (var create = connection.CreateCommand())
		{
			create.CommandText = $"CREATE DATABASE [{_database}]";
			await create.ExecuteNonQueryAsync();
		}

		await using var seed = connection.CreateCommand();
		seed.CommandText =
			$"USE [{_database}]; CREATE TABLE snap_rows (id INT); INSERT INTO snap_rows VALUES (1),(2);";
		await seed.ExecuteNonQueryAsync();
	}

	public async ValueTask DisposeAsync()
	{
		if (_masterConnection == null) return;

		SqlConnection.ClearAllPools();
		await using var connection = new SqlConnection(_masterConnection);
		await connection.OpenAsync();
		await using var drop = connection.CreateCommand();
		drop.CommandText =
			$"ALTER DATABASE [{_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{_database}];";
		try { await drop.ExecuteNonQueryAsync(); } catch { }
	}

	[Fact]
	public async Task A_database_that_disallows_snapshot_is_still_readable()
	{
		if (_masterConnection == null) return;

		await using var check = new SqlConnection(DatabaseConnection);
		await check.OpenAsync();
		await using (var state = check.CreateCommand())
		{
			state.CommandText = "SELECT snapshot_isolation_state FROM sys.databases WHERE database_id = DB_ID()";
			var value = await state.ExecuteScalarAsync();
			((byte)value!).Should().Be(0, "the premise of this test is a database that refuses Snapshot");
		}

		var reader = new SqlServerReader(DatabaseConnection, "SELECT id FROM snap_rows", new SqlServerReaderOptions());
		await reader.OpenAsync();

		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
			rows.AddRange(batch.ToArray());

		rows.Select(r => Convert.ToInt32(r[0])).Should().BeEquivalentTo(new[] { 1, 2 });
		await reader.DisposeAsync();
	}
}

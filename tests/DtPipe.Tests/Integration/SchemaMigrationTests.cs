using DtPipe.Adapters.Sqlite;
using DtPipe.Configuration;
using DtPipe.Core.Models;
using DtPipe.Feedback;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Integration;

public class SchemaMigrationTests
{
    [Fact]
    public async Task AutoMigrate_AddsMissingColumn_In_Sqlite()
    {
        var dbPath = $"migration_test_{Guid.NewGuid()}.db";
        var connectionString = $"Data Source={dbPath}";

        try
        {
            // 1. Setup: Create table with 1 column
            var options = new SqliteWriterOptions
            {
                Table = "MIG_TEST",
                Strategy = SqliteWriteStrategy.Append
            };

            var writer = new SqliteDataWriter(connectionString, options, NullLogger<SqliteDataWriter>.Instance, SqliteTypeConverter.Instance);

            await writer.ExecuteCommandAsync("CREATE TABLE MIG_TEST (ID INTEGER PRIMARY KEY)", CancellationToken.None);

            // 2. Prepare Source: 2 columns (ID and NEW_COL)
            var sourceColumns = new List<PipeColumnInfo>
            {
                new PipeColumnInfo("ID", typeof(int), false),
                new PipeColumnInfo("NEW_COL", typeof(string), true)
            };

            // 3. Run Export logic (Manual simulation of ExportService step)
            var inspector = (DtPipe.Core.Abstractions.ISchemaInspector)writer;
            var targetSchema = await inspector.InspectTargetAsync(CancellationToken.None);
            var report = DtPipe.Core.Validation.SchemaCompatibilityAnalyzer.Analyze(sourceColumns, targetSchema, writer.Dialect);

            Assert.Contains(report.Columns, c => c.ColumnName == "NEW_COL" && c.Status == DtPipe.Core.Validation.CompatibilityStatus.MissingInTarget);

            var migrator = (DtPipe.Core.Abstractions.ISchemaMigrator)writer;

            // Initialize writer to resolve _quotedTargetTableName
            await writer.InitializeAsync(sourceColumns, CancellationToken.None);

            await migrator.MigrateSchemaAsync(report, CancellationToken.None);

            // 4. Verify: Re-inspect
            var updatedSchema = await inspector.InspectTargetAsync(CancellationToken.None);
            Assert.Contains(updatedSchema!.Columns, c => c.Name == "NEW_COL");
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }
}

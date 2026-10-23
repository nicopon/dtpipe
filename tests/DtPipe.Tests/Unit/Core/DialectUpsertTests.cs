using DtPipe.Core.Abstractions;
using DtPipe.Adapters.Shared.Dialects;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// F9 — dialect-owned staged-merge generation. Pins the exact SQL text per dialect ×
/// mode so refactors cannot silently shift generated SQL (snapshot-first discipline).
/// </summary>
public class DialectUpsertTests
{
    private static readonly PipeColumnInfo Id = new("Id", typeof(int), false);
    private static readonly PipeColumnInfo Val = new("Val", typeof(string), true);

    private static MergeSpec Spec(MergeMode mode, bool verified = true, string source = "[stage]") => new(
        QuotedTargetTable: "\"tgt\"",
        SourceTable: source,
        KeyColumns: new[] { "Id" },
        Columns: new PipeColumnInfo[] { Id, Val },
        Mode: mode,
        ConstraintVerified: verified);

    [Fact]
    public void PostgreSql_Upsert_OnConflict_DoUpdate_Excludes_Keys()
    {
        var sql = new PostgreSqlDialect().BuildStagingMerge(Spec(MergeMode.Upsert));

        Assert.Contains("INSERT INTO \"tgt\" (\"Id\", \"Val\") SELECT \"Id\", \"Val\" FROM", sql);
        Assert.Contains("ON CONFLICT (\"Id\") DO UPDATE SET", sql);
        Assert.Contains("\"Val\" = EXCLUDED.\"Val\"", sql);
        Assert.DoesNotContain("\"Id\" = EXCLUDED", sql); // key columns never updated
    }

    [Fact]
    public void PostgreSql_Ignore_OnConflict_DoNothing()
    {
        var sql = new PostgreSqlDialect().BuildStagingMerge(Spec(MergeMode.Ignore));
        Assert.Contains("ON CONFLICT (\"Id\") DO NOTHING", sql);
    }

    [Fact]
    public void DuckDb_VerifiedConstraint_Matches_Ansi_Shape()
    {
        var sql = new DuckDbDialect().BuildStagingMerge(Spec(MergeMode.Upsert, verified: true));
        Assert.Contains("INSERT INTO \"tgt\"", sql);
        Assert.Contains("ON CONFLICT (\"Id\") DO UPDATE SET \"Val\" = EXCLUDED.\"Val\"", sql);
    }

    [Fact]
    public void DuckDb_Unverified_Falls_Back_To_DeleteThenInsert()
    {
        var sql = new DuckDbDialect().BuildStagingMerge(Spec(MergeMode.Upsert, verified: false));
        var parts = sql.Split(';');
        Assert.Equal(2, parts.Length);
        Assert.Contains("DELETE FROM \"tgt\" USING", parts[0]);
        Assert.Contains("INSERT INTO \"tgt\" SELECT * FROM", parts[1]);
    }

    [Fact]
    public void DuckDb_Unverified_Ignore_Deletes_From_Staging()
    {
        var sql = new DuckDbDialect().BuildStagingMerge(Spec(MergeMode.Ignore, verified: false));
        var parts = sql.Split(';');
        Assert.Contains("DELETE FROM", parts[0]);
        Assert.Contains("USING \"tgt\"", parts[0]); // deletes staging rows already in target
        Assert.Contains("INSERT INTO \"tgt\"", parts[1]);
    }

    [Fact]
    public void SqlServer_Generates_Tsql_Merge()
    {
        var sql = new SqlServerDialect().BuildStagingMerge(Spec(MergeMode.Upsert, source: "stage"));

        Assert.Contains("MERGE \"tgt\" AS T USING [stage] AS S ON (T.Id = S.[Id])", sql);
        Assert.Contains("WHEN MATCHED THEN UPDATE SET T.Val = S.[Val]", sql);
        Assert.Contains("WHEN NOT MATCHED THEN INSERT (Id, Val) VALUES (S.[Id], S.[Val]);", sql);
    }

    [Fact]
    public void SqlServer_Ignore_Omits_WhenMatched_Clause()
    {
        var sql = new SqlServerDialect().BuildStagingMerge(Spec(MergeMode.Ignore, source: "stage"));

        Assert.Contains("MERGE \"tgt\" AS T USING [stage] AS S ON (T.Id = S.[Id])", sql);
        Assert.DoesNotContain("WHEN MATCHED", sql);          // existing keys are skipped
        Assert.Contains("WHEN NOT MATCHED THEN INSERT (Id, Val) VALUES (S.[Id], S.[Val]);", sql);
    }

    [Fact]
    public void Sqlite_Parameterized_Clause_Uses_Excluded()
    {
        var clause = new SqliteDialect().BuildParameterizedConflictClause(new[] { "Id" }, new[] { Id, Val });

        Assert.StartsWith("ON CONFLICT (\"Id\") DO UPDATE SET", clause);
        Assert.Contains("\"Val\" = excluded.\"Val\"", clause);
        Assert.DoesNotContain("\"Id\" =", clause);
    }

    [Fact]
    public void MySql_Upsert_Uses_OnDuplicateKeyUpdate_With_Derived_Alias()
    {
        var sql = new MySqlDialect().BuildStagingMerge(Spec(MergeMode.Upsert, source: "`stage`"));

        Assert.Contains("INSERT INTO \"tgt\" (`Id`, `Val`)", sql);
        // Derived-table alias, not VALUES(): MySQL deprecated VALUES() in 8.0.20.
        Assert.Contains("SELECT * FROM (SELECT `Id`, `Val` FROM `stage`) AS dtp_src", sql);
        Assert.Contains("ON DUPLICATE KEY UPDATE `Val` = dtp_src.`Val`", sql);
        Assert.DoesNotContain("VALUES(", sql);
        Assert.DoesNotContain("`Id` = dtp_src", sql); // key columns never updated
    }

    [Fact]
    public void MySql_Ignore_Assigns_Key_To_Itself()
    {
        var sql = new MySqlDialect().BuildStagingMerge(Spec(MergeMode.Ignore, source: "`stage`"));

        // A no-op assignment skips the row without INSERT IGNORE's blanket error suppression.
        Assert.Contains("ON DUPLICATE KEY UPDATE `Id` = \"tgt\".`Id`", sql);
        Assert.DoesNotContain("INSERT IGNORE", sql);
        Assert.DoesNotContain("dtp_src.`Val`", sql); // existing rows are left untouched
    }

    [Fact]
    public void MySql_Unverified_Falls_Back_To_DeleteThenInsert()
    {
        var sql = new MySqlDialect().BuildStagingMerge(Spec(MergeMode.Upsert, verified: false, source: "`stage`"));
        var parts = sql.Split(';');

        // Without a matching unique index, ON DUPLICATE KEY UPDATE would never fire and the
        // upsert would silently append duplicates — so it must not be emitted at all.
        Assert.Equal(2, parts.Length);
        Assert.Contains("DELETE t FROM \"tgt\" AS t JOIN `stage` AS s ON t.`Id` = s.`Id`", parts[0]);
        Assert.Contains("INSERT INTO \"tgt\" (`Id`, `Val`) SELECT `Id`, `Val` FROM `stage`", parts[1]);
        Assert.DoesNotContain("ON DUPLICATE KEY", sql);
    }

    [Fact]
    public void MySql_Unverified_Ignore_Deletes_From_Staging()
    {
        var sql = new MySqlDialect().BuildStagingMerge(Spec(MergeMode.Ignore, verified: false, source: "`stage`"));
        var parts = sql.Split(';');

        Assert.Contains("DELETE s FROM `stage` AS s JOIN \"tgt\" AS t", parts[0]);
        Assert.Contains("INSERT INTO \"tgt\"", parts[1]);
    }

    [Fact]
    public void MySql_Quote_Doubles_Embedded_Backtick()
    {
        Assert.Equal("`we``ird`", new MySqlDialect().Quote("we`ird"));
    }
}

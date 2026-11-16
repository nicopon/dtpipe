using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using DtPipe.Adapters.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Sqlite;

/// <summary>
/// SQLite types a value, not a column. A column with no declared type (an expression, a view, a
/// table made with a bare name) reports the type of the first row it meets, and a NULL there says
/// nothing of the rows after it. The reader must never turn those later rows into empty cells.
/// </summary>
public sealed class SqliteReaderUntypedColumnTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"dtpipe-sqlite-{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        if (File.Exists(_path)) File.Delete(_path);
    }

    private async Task<(IReadOnlyList<string> Types, List<string?> Cells)> ReadSecondColumn(string setupSql, string query)
    {
        var cs = $"Data Source={_path}";
        await using (var setup = new SqliteConnection(cs))
        {
            await setup.OpenAsync();
            var cmd = setup.CreateCommand();
            cmd.CommandText = setupSql;
            await cmd.ExecuteNonQueryAsync();
        }

        await using var reader = new SqliteReader(cs, query);
        await reader.OpenAsync();
        var types = reader.Columns!.Select(c => c.ClrType.Name).ToList();
        var cells = new List<string?>();
        await foreach (var batch in reader.ReadRecordBatchesAsync())
        {
            using (batch)
            {
                var column = batch.Column(1);
                for (var i = 0; i < batch.Length; i++)
                    cells.Add(column.IsNull(i) ? null : column is StringArray s ? s.GetString(i) : $"<{column.GetType().Name}>");
            }
        }
        return (types, cells);
    }

    [Fact]
    public async Task A_Null_First_Row_Of_An_Undeclared_Column_Does_Not_Empty_The_Rows_After_It()
    {
        var (types, cells) = await ReadSecondColumn(
            "create table t(id integer, note); insert into t values (1, NULL), (2, 'x'), (3, 'y');",
            "select * from t");

        Assert.Equal("String", types[1]);
        Assert.Equal(new string?[] { null, "x", "y" }, cells);
    }

    [Fact]
    public async Task A_Column_Declared_Text_Keeps_Its_Type_Under_A_Null_First_Row()
    {
        var (types, cells) = await ReadSecondColumn(
            "create table t(id integer, note text); insert into t values (1, NULL), (2, 'x');",
            "select * from t");

        Assert.Equal("String", types[1]);
        Assert.Equal(new string?[] { null, "x" }, cells);
    }

    [Fact]
    public async Task A_Column_Declared_Blob_Stays_Binary_Under_A_Null_First_Row()
    {
        var (types, _) = await ReadSecondColumn(
            "create table t(id integer, payload blob); insert into t values (1, NULL), (2, x'0102');",
            "select * from t");

        Assert.Equal("Byte[]", types[1]);
    }

    /// <summary>
    /// The first value past the probe's window is still never read as empty: a cell the column cannot
    /// hold stops the run and names the column.
    /// </summary>
    [Fact]
    public async Task A_Value_Past_The_Probe_Window_That_The_Column_Cannot_Hold_Is_Refused_By_Name()
    {
        var inserts = string.Join(" ", Enumerable.Range(0, 300).Select(i => $"insert into t values ({i}, NULL);"));
        var fault = await Assert.ThrowsAnyAsync<Exception>(() => ReadSecondColumn(
            $"create table t(id integer, note); {inserts} insert into t values (300, 'late');",
            "select * from t"));

        Assert.Contains("note", fault.Message);
    }
}

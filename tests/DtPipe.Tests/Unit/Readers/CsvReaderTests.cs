using DtPipe.Adapters.Csv;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Options;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Readers;

public class CsvReaderTests : IAsyncLifetime
{
    private string _testCsvPath = null!;

    public async ValueTask InitializeAsync()
    {
        _testCsvPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.csv");

        // Create test CSV file
        var content = """
            Id,Name,Score,Active
            1,Alice,95.5,true
            2,Bob,87.3,false
            3,Charlie,92.0,true
            """;
        await File.WriteAllTextAsync(_testCsvPath, content);
    }

    public ValueTask DisposeAsync()
    {
        if (File.Exists(_testCsvPath)) File.Delete(_testCsvPath);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task CsvReader_ShouldReadHeaderAndData()
    {
        // Arrange
        var options = new CsvReaderOptions { Separator = ",", HasHeader = true };
        var reader = new CsvStreamReader(_testCsvPath, options);

        // Act
        await reader.OpenAsync();
        var columns = reader.Columns;
        var rows = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(100))
        {
            foreach (var row in batch.ToArray())
            {
                rows.Add(row);
            }
        }
        await reader.DisposeAsync();

        // Assert
        columns.Should().HaveCount(4);
        columns![0].Name.Should().Be("Id");
        columns[1].Name.Should().Be("Name");
        columns[2].Name.Should().Be("Score");
        columns[3].Name.Should().Be("Active");

        rows.Should().HaveCount(3);
        rows[0][0].Should().Be("1");
        rows[0][1].Should().Be("Alice");
        rows[1][1].Should().Be("Bob");
        rows[2][1].Should().Be("Charlie");
    }

    [Fact]
    public async Task CsvReader_ShouldHandleCustomDelimiter()
    {
        // Arrange: Create semicolon-delimited file
        var semicolonPath = Path.Combine(Path.GetTempPath(), $"test_semicolon_{Guid.NewGuid()}.csv");
        await File.WriteAllTextAsync(semicolonPath, "A;B;C\n1;2;3\n4;5;6");

        try
        {
            var options = new CsvReaderOptions { Separator = ";", HasHeader = true };
            var reader = new CsvStreamReader(semicolonPath, options);

            // Act
            await reader.OpenAsync();
            var columns = reader.Columns;
            var rows = new List<object?[]>();
            await foreach (var batch in reader.ReadBatchesAsync(100))
            {
                foreach (var row in batch.ToArray())
                {
                    rows.Add(row);
                }
            }
            await reader.DisposeAsync();

            // Assert
            columns.Should().HaveCount(3);
            columns![0].Name.Should().Be("A");
            rows.Should().HaveCount(2);
            rows[0][0].Should().Be("1");
        }
        finally
        {
            if (File.Exists(semicolonPath)) File.Delete(semicolonPath);
        }
    }

    [Fact]
    public async Task CsvReader_ColumnTypes_ShouldParseUuidAndInt()
    {
        // Arrange: CSV with UUID-formatted Id and numeric Score
        var uuidPath = Path.Combine(Path.GetTempPath(), $"test_uuid_{Guid.NewGuid()}.csv");
        var guid1 = Guid.NewGuid();
        var guid2 = Guid.NewGuid();
        await File.WriteAllTextAsync(uuidPath, $"Id,Name,Score\n{guid1},Alice,42\n{guid2},Bob,99");

        try
        {
            var options = new CsvReaderOptions
            {
                Separator = ",",
                HasHeader = true,
                ColumnTypes = "Id:uuid,Score:int32"
            };
            var reader = new CsvStreamReader(uuidPath, options);

            // Act
            await reader.OpenAsync();
            var columns = reader.Columns!;
            var rows = new List<object?[]>();
            await foreach (var batch in reader.ReadBatchesAsync(100))
                foreach (var row in batch.ToArray())
                    rows.Add(row);
            await reader.DisposeAsync();

            // Assert: schema reflects declared types
            columns.Should().HaveCount(3);
            columns[0].ClrType.Should().Be(typeof(Guid));
            columns[1].ClrType.Should().Be(typeof(string));
            columns[2].ClrType.Should().Be(typeof(int));

            // Assert: row values are parsed
            rows.Should().HaveCount(2);
            rows[0][0].Should().BeOfType<Guid>().Which.Should().Be(guid1);
            rows[0][2].Should().BeOfType<int>().Which.Should().Be(42);
            rows[1][0].Should().BeOfType<Guid>().Which.Should().Be(guid2);
            rows[1][2].Should().BeOfType<int>().Which.Should().Be(99);
        }
        finally
        {
            if (File.Exists(uuidPath)) File.Delete(uuidPath);
        }
    }

    [Fact]
    public async Task CsvReader_InferColumnTypes_ShouldSuggestUuid()
    {
        // Arrange: CSV with standard UUID strings
        var uuidPath = Path.Combine(Path.GetTempPath(), $"test_infer_{Guid.NewGuid()}.csv");
        var lines = Enumerable.Range(0, 10).Select(i => $"{Guid.NewGuid()},value{i}");
        await File.WriteAllTextAsync(uuidPath, "Id,Name\n" + string.Join("\n", lines));

        try
        {
            var options = new CsvReaderOptions { Separator = ",", HasHeader = true };
            var reader = new CsvStreamReader(uuidPath, options);
            await reader.OpenAsync();

            // Act
            var suggestions = await reader.InferColumnTypesAsync(10);
            await reader.DisposeAsync();

            // Assert
            suggestions.Should().ContainKey("Id");
            suggestions["Id"].Should().Be("uuid");
        }
        finally
        {
            if (File.Exists(uuidPath)) File.Delete(uuidPath);
        }
    }

    [Fact]
    public void CsvReaderFactory_ShouldDetectCsvFiles()
    {
        var registry = new OptionsRegistry();
        registry.Register(new CsvReaderOptions());
        var factory = new CliStreamReaderFactory(new CsvReaderDescriptor(), registry, null!);

        factory.CanHandle("csv:data.csv").Should().BeTrue();
        factory.CanHandle("data.csv").Should().BeTrue();
        factory.CanHandle("path/to/file.CSV").Should().BeTrue();
        factory.CanHandle("data.parquet").Should().BeFalse();
        factory.CanHandle("sqlite:test.db").Should().BeFalse();
    }

    [Fact]
    public async Task CsvReader_MaxSample_LimitsInferenceRowSampleCount()
    {
        var csvPath = Path.Combine(Path.GetTempPath(), $"test_sample_{Guid.NewGuid()}.csv");
        var content = """
            Id,Score
            1,100
            2,200
            3,not_a_number
            4,400
            5,500
            """;
        await File.WriteAllTextAsync(csvPath, content);

        try
        {
            // Sample only the first 2 rows: both are numeric, so Score is inferred as int
            var optionsLimited = new CsvReaderOptions { AutoColumnTypes = true, MaxSample = 2 };
            await using (var reader = new CsvStreamReader(csvPath, optionsLimited))
            {
                await reader.OpenAsync();
                reader.Columns!.First(c => c.Name == "Score").ClrType.Should().Be(typeof(int));

                // Line 4 lies beyond the sample: the inferred type refuses it rather than reading null.
                var read = async () => { await foreach (var _ in reader.ReadBatchesAsync(100)) { } };
                var refusal = (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message;
                refusal.Should().Contain("CSV line 4").And.Contain("'Score'").And.Contain("int32")
                    .And.Contain("--max-sample").And.Contain("--column-types").And.NotContain("not_a_number");
            }

            // Sample 5 rows: row 3 is not_a_number, so Score falls back to string
            var optionsBroader = new CsvReaderOptions { AutoColumnTypes = true, MaxSample = 5 };
            await using (var reader = new CsvStreamReader(csvPath, optionsBroader))
            {
                await reader.OpenAsync();
                reader.Columns!.First(c => c.Name == "Score").ClrType.Should().Be(typeof(string));
            }
        }
        finally
        {
            if (File.Exists(csvPath)) File.Delete(csvPath);
        }
    }

    [Fact]
    public async Task CsvReader_MaxSample_Zero_DisablesAutoColumnTypesInference()
    {
        var csvPath = Path.Combine(Path.GetTempPath(), $"test_sample0_{Guid.NewGuid()}.csv");
        var content = """
            Id,Score
            1,100
            2,200
            """;
        await File.WriteAllTextAsync(csvPath, content);

        try
        {
            // MaxSample = 0 turns inference off: columns remain strings
            var optionsZero = new CsvReaderOptions { AutoColumnTypes = true, MaxSample = 0 };
            await using (var reader = new CsvStreamReader(csvPath, optionsZero))
            {
                await reader.OpenAsync();
                reader.Columns!.First(c => c.Name == "Score").ClrType.Should().Be(typeof(string));
                reader.AutoAppliedTypes.Should().BeNull();
            }

            // MaxSample <= 0 also turns inference off
            var optionsNegative = new CsvReaderOptions { AutoColumnTypes = true, MaxSample = -1 };
            await using (var reader = new CsvStreamReader(csvPath, optionsNegative))
            {
                await reader.OpenAsync();
                reader.Columns!.First(c => c.Name == "Score").ClrType.Should().Be(typeof(string));
                reader.AutoAppliedTypes.Should().BeNull();
            }

            // Explicit column types still apply when MaxSample = 0
            var optionsDeclared = new CsvReaderOptions { AutoColumnTypes = true, MaxSample = 0, ColumnTypes = "Score:int32" };
            await using (var reader = new CsvStreamReader(csvPath, optionsDeclared))
            {
                await reader.OpenAsync();
                reader.Columns!.First(c => c.Name == "Score").ClrType.Should().Be(typeof(int));
            }
        }
        finally
        {
            if (File.Exists(csvPath)) File.Delete(csvPath);
        }
    }

    [Fact]
    public async Task CsvReader_MaxSample_CliExecutionHonorsFlag()
    {
        var csvPath = Path.Combine(Path.GetTempPath(), $"test_cli_{Guid.NewGuid()}.csv");
        var outPath = Path.Combine(Path.GetTempPath(), $"test_cli_out_{Guid.NewGuid()}.csv");
        var content = """
            Id,Score
            1,100
            2,200
            3,300
            """;
        await File.WriteAllTextAsync(csvPath, content);

        try
        {
            var exit = await Program.Main(["-i", $"csv:{csvPath}", "--auto-column-types", "--max-sample", "2", "-o", $"csv:{outPath}", "--no-stats"]);
            exit.Should().Be(0);
            File.Exists(outPath).Should().BeTrue();
        }
        finally
        {
            if (File.Exists(csvPath)) File.Delete(csvPath);
            if (File.Exists(outPath)) File.Delete(outPath);
        }
    }

    [Theory]
    [InlineData("int", "abc", "int32")]
    [InlineData("long", "abc", "int64")]
    [InlineData("double", "abc", "double")]
    [InlineData("decimal", "abc", "decimal")]
    [InlineData("bool", "abc", "bool")]
    [InlineData("datetime", "abc", "datetime")]
    [InlineData("datetimeoffset", "abc", "datetimeoffset")]
    [InlineData("uuid", "abc", "uuid")]
    [InlineData("int", " ", "int32")]
    [InlineData("uuid", " ", "uuid")]
    public async Task CsvReader_ColumnTypes_Refuses_A_Cell_Its_Type_Does_Not_Read(string hint, string cell, string named)
    {
        var csvPath = Path.Combine(Path.GetTempPath(), $"test_unread_{Guid.NewGuid()}.csv");
        await File.WriteAllTextAsync(csvPath, $"id,n\n1,\n2,{cell}\n");
        try
        {
            await using var reader = new CsvStreamReader(csvPath, new CsvReaderOptions { ColumnTypes = $"n:{hint}" });
            await reader.OpenAsync();

            var read = async () => { await foreach (var _ in reader.ReadBatchesAsync(100)) { } };
            var refusal = (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message;
            refusal.Should().Contain("CSV line 3").And.Contain("'n'").And.Contain($"declared {named}");
            if (cell.Trim().Length > 0) refusal.Should().NotContain(cell);
        }
        finally
        {
            if (File.Exists(csvPath)) File.Delete(csvPath);
        }
    }

    [Fact]
    public async Task CsvReader_ColumnTypes_Reads_An_Empty_Cell_As_Null_And_Every_Spelling_It_Accepts()
    {
        var bytes = Guid.NewGuid();
        var csvPath = Path.Combine(Path.GetTempPath(), $"test_read_{Guid.NewGuid()}.csv");
        await File.WriteAllTextAsync(csvPath,
            $"n,b,g\n,,\n 42 ,yes,{Convert.ToBase64String(bytes.ToByteArray())}\n7,TRUE,{bytes}\n");
        try
        {
            await using var reader = new CsvStreamReader(csvPath, new CsvReaderOptions { ColumnTypes = "n:int,b:bool,g:uuid" });
            await reader.OpenAsync();
            var rows = new List<object?[]>();
            await foreach (var batch in reader.ReadBatchesAsync(100)) rows.AddRange(batch.ToArray());

            rows.Should().HaveCount(3);
            rows[0].Should().OnlyContain(v => v == null);
            rows[1].Should().Equal(42, true, bytes);
            rows[2].Should().Equal(7, true, bytes);
        }
        finally
        {
            if (File.Exists(csvPath)) File.Delete(csvPath);
        }
    }
}

using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Processors.DuckDB;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Threading.Channels;
using Xunit;

namespace DtPipe.Tests.Unit.Processors;

/// <summary>
/// End-to-end tests for DuckDBSqlProcessor: OpenAsync → ReadRecordBatchesAsync.
/// These tests exercise the real DuckDB native library (no mocking of the engine)
/// to catch P/Invoke entry point errors and schema/streaming bugs early.
/// </summary>
public class DuckDBSqlProcessorTests
{
    private static IMemoryChannelRegistry BuildRegistry(
        string alias, Schema schema, IEnumerable<RecordBatch> batches)
    {
        var channel = Channel.CreateUnbounded<RecordBatch>();
        foreach (var b in batches) channel.Writer.TryWrite(b);
        channel.Writer.Complete();

        var mock = new Mock<IMemoryChannelRegistry>();
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync(alias, It.IsAny<CancellationToken>()))
            .ReturnsAsync(schema);
        mock.Setup(r => r.GetArrowChannel(alias))
            .Returns((channel, schema));
        return mock.Object;
    }

    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_SimpleSelect_ReturnsBatches()
    {
        // Arrange: one integer column, two rows
        var field = new Field("val", Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);
        var arr = new Int32Array.Builder().Append(42).Append(99).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 2);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT val FROM src", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        // Act
        await processor.OpenAsync();

        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);

        await processor.DisposeAsync();

        // Assert
        Assert.NotEmpty(batches);
        var totalRows = batches.Sum(b => b.Length);
        Assert.Equal(2, totalRows);

        // Verify schema is correct
        Assert.NotNull(processor.Schema);
        Assert.Single(processor.Schema!.FieldsList);
        Assert.Equal("val", processor.Schema.FieldsList[0].Name);
    }

    [Fact]
    public async Task OpenAsync_JoinQueryWithRefTables_ExplainDoesNotThrow()
    {
        var mainField = new Field("GenerateIndex", Int64Type.Default, nullable: false);
        var mainSchema = new Schema(new[] { mainField }, null);
        var mainBatch = new RecordBatch(mainSchema, new IArrowArray[] { new Int64Array.Builder().Append(1).Build() }, 1);

        var refField = new Field("Id", Int64Type.Default, nullable: false);
        var refSchema = new Schema(new[] { refField }, null);
        var refBatch = new RecordBatch(refSchema, new IArrowArray[] { new Int64Array.Builder().Append(1).Build() }, 1);

        var channelMain = Channel.CreateUnbounded<RecordBatch>();
        channelMain.Writer.TryWrite(mainBatch);
        channelMain.Writer.Complete();

        var channelRef = Channel.CreateUnbounded<RecordBatch>();
        channelRef.Writer.TryWrite(refBatch);
        channelRef.Writer.Complete();

        var channelRef2 = Channel.CreateUnbounded<RecordBatch>();
        channelRef2.Writer.TryWrite(refBatch);
        channelRef2.Writer.Complete();

        var mock = new Mock<IMemoryChannelRegistry>();
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("main", It.IsAny<CancellationToken>())).ReturnsAsync(mainSchema);
        mock.Setup(r => r.GetArrowChannel("main")).Returns((channelMain, mainSchema));

        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("ref", It.IsAny<CancellationToken>())).ReturnsAsync(refSchema);
        mock.Setup(r => r.GetArrowChannel("ref")).Returns((channelRef, refSchema));

        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("ref2", It.IsAny<CancellationToken>())).ReturnsAsync(refSchema);
        mock.Setup(r => r.GetArrowChannel("ref2")).Returns((channelRef2, refSchema));

        var processor = new DuckDBSqlProcessor(
            mock.Object,
            "SELECT m.*, r.Id as ref1_id, r2.Id as ref2_id FROM main m LEFT JOIN ref r ON m.GenerateIndex = CAST(r.Id AS BIGINT) LEFT JOIN ref2 r2 ON m.GenerateIndex = CAST(r2.Id AS BIGINT)",
            "main", "main",
            refAliases: ["ref", "ref2"], refChannelAliases: ["ref", "ref2"],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);

        await processor.DisposeAsync();
        Assert.NotEmpty(batches);
    }

    /// <summary>
    /// Only the --from source streams: a join with a --ref builds its hash table on the --ref, so rows come out
    /// while the main is still producing, whichever side of the join the query names it on. A hash table built
    /// on the main holds the whole stream and gives no row before its end.
    /// </summary>
    [Theory]
    [InlineData("SELECT m.k, r.v FROM main m JOIN ref r ON m.k = r.k")]
    [InlineData("SELECT m.k, r.v FROM ref r JOIN main m ON m.k = r.k")]
    public async Task ReadRecordBatches_MainJoinedWithRef_StreamsBeforeMainEnds(string query)
    {
        var mainSchema = new Schema([new Field("k", Int64Type.Default, nullable: false)], null);
        var refSchema = new Schema(
            [new Field("k", Int64Type.Default, nullable: false), new Field("v", StringType.Default, nullable: false)], null);

        var refKeys = new Int64Array.Builder();
        var refValues = new StringArray.Builder();
        for (var i = 0; i < 1000; i++)
        {
            refKeys.Append(i);
            refValues.Append($"v{i}");
        }
        var channelRef = Channel.CreateUnbounded<RecordBatch>();
        channelRef.Writer.TryWrite(new RecordBatch(refSchema, [refKeys.Build(), refValues.Build()], 1000));
        channelRef.Writer.Complete();

        // The main produces until the test has its answer: a source that has not ended while the query runs.
        var channelMain = Channel.CreateUnbounded<RecordBatch>();
        using var stop = new CancellationTokenSource();
        var producer = Task.Run(async () =>
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    var keys = new Int64Array.Builder();
                    for (var i = 0; i < 100; i++) keys.Append(i);
                    await channelMain.Writer.WriteAsync(new RecordBatch(mainSchema, [keys.Build()], 100));
                    await Task.Delay(10, stop.Token);
                }
            }
            catch (OperationCanceledException) { }
            finally { channelMain.Writer.Complete(); }
        });

        var mock = new Mock<IMemoryChannelRegistry>();
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("main", It.IsAny<CancellationToken>())).ReturnsAsync(mainSchema);
        mock.Setup(r => r.GetArrowChannel("main")).Returns((channelMain, mainSchema));
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("ref", It.IsAny<CancellationToken>())).ReturnsAsync(refSchema);
        mock.Setup(r => r.GetArrowChannel("ref")).Returns((channelRef, refSchema));

        var processor = new DuckDBSqlProcessor(
            mock.Object, query, "main", "main",
            refAliases: ["ref"], refChannelAliases: ["ref"],
            NullLogger<DuckDBSqlProcessor>.Instance);
        try
        {
            await processor.OpenAsync();
            await using var batches = processor.ReadRecordBatchesAsync().GetAsyncEnumerator();
            var first = batches.MoveNextAsync().AsTask();
            var streamed = await Task.WhenAny(first, Task.Delay(TimeSpan.FromSeconds(15))) == first;

            stop.Cancel();
            await producer;
            if (await first)
            {
                batches.Current.Dispose();
                while (await batches.MoveNextAsync())
                    batches.Current.Dispose();
            }

            Assert.True(streamed, "The join gave no row while its --from source was producing: its hash table is built on the stream.");
        }
        finally
        {
            stop.Cancel();
            await processor.DisposeAsync();
        }
    }

    /// <summary>
    /// The --ref sources are drained together and registered one after the other: a registration writes the
    /// maps of the sources and uses the one connection, neither of which takes two threads. The channels end
    /// at the same instant while OpenAsync waits on them, which is when registrations would overlap and one
    /// would lose an entry of the maps. A round is a few milliseconds; the race shows in about one round in
    /// eight, so the rounds together leave it no chance to hide.
    /// </summary>
    [Fact]
    public async Task OpenAsync_RefsEndingTogether_AreRegisteredOneAtATime()
    {
        const int refs = 16;
        const int rounds = 40;

        for (var round = 0; round < rounds; round++)
            await OpenWithRefsEndingTogetherAsync(refs);
    }

    private static async Task OpenWithRefsEndingTogetherAsync(int refs)
    {
        var schema = new Schema(new[] { new Field("v", Int64Type.Default, nullable: false) }, null);
        RecordBatch BatchOf(long value) => new(schema, new IArrowArray[] { new Int64Array.Builder().Append(value).Build() }, 1);

        var mainChannel = Channel.CreateUnbounded<RecordBatch>();
        mainChannel.Writer.TryWrite(BatchOf(0));
        mainChannel.Writer.Complete();

        var registry = new Mock<IMemoryChannelRegistry>();
        registry.Setup(r => r.WaitForArrowChannelSchemaAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(schema);
        registry.Setup(r => r.GetArrowChannel("main")).Returns((mainChannel, schema));

        var aliases = Enumerable.Range(0, refs).Select(i => $"r{i}").ToArray();
        var channels = new Channel<RecordBatch>[refs];
        var draining = new TaskCompletionSource[refs];
        for (var i = 0; i < refs; i++)
        {
            var channel = channels[i] = Channel.CreateUnbounded<RecordBatch>();
            var reached = draining[i] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var alias = aliases[i];
            registry.Setup(r => r.GetArrowChannel(alias)).Returns(() =>
            {
                reached.TrySetResult();
                return (channel, schema);
            });
        }

        var query = $"SELECT main.v + {string.Join(" + ", aliases.Select(a => $"{a}.v"))} AS total FROM main, {string.Join(", ", aliases)}";
        var processor = new DuckDBSqlProcessor(
            registry.Object, query, "main", "main", aliases, aliases, NullLogger<DuckDBSqlProcessor>.Instance);

        using var cts = new CancellationTokenSource();
        var open = processor.OpenAsync(cts.Token);
        var threads = new List<Thread>();
        try
        {
            // Every drain is waiting on its channel before any of them ends: without it the drains would
            // not overlap, and the round would pass without having exercised the race.
            var allDraining = Task.WhenAll(draining.Select(d => d.Task));
            if (await Task.WhenAny(allDraining, open, Task.Delay(TimeSpan.FromSeconds(10))) != allDraining)
            {
                if (open.IsCompleted) await open;
                throw new TimeoutException($"The {refs} --ref sources were not all being drained within 10 s.");
            }

            var barrier = new Barrier(refs);
            for (var i = 0; i < refs; i++)
            {
                var (channel, value) = (channels[i], i + 1);
                var thread = new Thread(() =>
                {
                    barrier.SignalAndWait();
                    channel.Writer.TryWrite(BatchOf(value));
                    channel.Writer.Complete();
                })
                { IsBackground = true };
                threads.Add(thread);
                thread.Start();
            }

            await open.WaitAsync(TimeSpan.FromSeconds(30));

            long total = 0;
            await foreach (var batch in processor.ReadRecordBatchesAsync(cts.Token))
                using (batch)
                    total += ((Int64Array)batch.Column(0)).GetValue(0)!.Value;
            Assert.Equal(Enumerable.Range(1, refs).Sum(v => (long)v), total);
        }
        catch
        {
            cts.Cancel();
            try { await processor.DisposeAsync(); } catch { /* the fault above is the one to report */ }
            throw;
        }
        finally
        {
            foreach (var thread in threads) thread.Join();
        }

        await processor.DisposeAsync();
    }

    /// <summary>
    /// A --ref batch no scan read is disposed by the processor's dispose. A table holds r1's name, so its view
    /// is refused: r0 is registered, r1 is a scan with no view, r2 is drained and never registered, and no
    /// scan reads any of them. Each source counts on its own pool, read with no collection before it: a
    /// finalizer that takes a batch back lowers the count by itself.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_RefBatchesNoScanRead_AreDisposed()
    {
        var schema = new Schema(new[] { new Field("v", Int64Type.Default, nullable: false) }, null);
        var registry = new Mock<IMemoryChannelRegistry>();
        registry.Setup(r => r.WaitForArrowChannelSchemaAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(schema);

        var aliases = new[] { "r0", "r1", "r2" };
        var pools = new Dictionary<string, TrackingMemoryPool>();
        foreach (var alias in aliases.Prepend("main"))
        {
            var pool = pools[alias] = new TrackingMemoryPool();
            var channel = Channel.CreateUnbounded<RecordBatch>();
            for (var b = 0; b < 3; b++)
                channel.Writer.TryWrite(new RecordBatch(schema, new IArrowArray[] { new Int64Array.Builder().Append(b).Build(pool) }, 1));
            channel.Writer.Complete();
            registry.Setup(r => r.GetArrowChannel(alias)).Returns((channel, schema));
        }

        var processor = new DuckDBSqlProcessor(
            registry.Object, "SELECT 1 FROM main, r0, r1, r2", "main", "main", aliases, aliases,
            NullLogger<DuckDBSqlProcessor>.Instance, initSql: "CREATE TABLE r1 (x INT)");

        await Assert.ThrowsAnyAsync<Exception>(() => processor.OpenAsync());
        await processor.DisposeAsync();

        Assert.All(aliases, alias => Assert.Equal(0, pools[alias].ActiveAllocations));
    }

    /// <summary>
    /// Two sources that carry a column of the same name are scanned with their own projections: the ref
    /// is read for <c>Name</c> alone, and is never handed the <c>GenerateIndex</c> the main side reads.
    /// A scan given a column the plan did not ask for takes it as the next one, here an integer as text,
    /// and DuckDB faults natively.
    /// </summary>
    [Fact]
    public async Task OpenAsync_SourcesSharingAColumnName_EachScanReadsItsOwnProjection()
    {
        var mainSchema = new Schema(new[]
        {
            new Field("GenerateIndex", Int64Type.Default, nullable: false),
            new Field("Product", StringType.Default, nullable: false),
        }, null);
        var mainBatch = new RecordBatch(mainSchema, new IArrowArray[]
        {
            new Int64Array.Builder().Append(0).Append(1).Build(),
            new StringArray.Builder().Append("a product name past inline").Append("another product name past inline").Build(),
        }, 2);

        var refSchema = new Schema(new[]
        {
            new Field("GenerateIndex", Int64Type.Default, nullable: false),
            new Field("Name", StringType.Default, nullable: false),
        }, null);
        var refBatch = new RecordBatch(refSchema, new IArrowArray[]
        {
            new Int64Array.Builder().Append(7).Append(8).Build(),
            new StringArray.Builder().Append("a customer name past inline").Append("one more customer name past inline").Build(),
        }, 2);

        var channelMain = Channel.CreateUnbounded<RecordBatch>();
        channelMain.Writer.TryWrite(mainBatch);
        channelMain.Writer.Complete();
        var channelRef = Channel.CreateUnbounded<RecordBatch>();
        channelRef.Writer.TryWrite(refBatch);
        channelRef.Writer.Complete();

        var mock = new Mock<IMemoryChannelRegistry>();
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("main", It.IsAny<CancellationToken>())).ReturnsAsync(mainSchema);
        mock.Setup(r => r.GetArrowChannel("main")).Returns((channelMain, mainSchema));
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("ref", It.IsAny<CancellationToken>())).ReturnsAsync(refSchema);
        mock.Setup(r => r.GetArrowChannel("ref")).Returns((channelRef, refSchema));

        await using var processor = new DuckDBSqlProcessor(
            mock.Object,
            "SELECT m.*, r.Name FROM main m JOIN ref r ON true",
            "main", "main",
            refAliases: ["ref"], refChannelAliases: ["ref"],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var names = new List<string>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
        {
            using (b)
            {
                var column = (StringArray)b.Column("Name");
                for (var i = 0; i < b.Length; i++) names.Add(column.GetString(i));
            }
        }

        Assert.Equal(
            ["a customer name past inline", "a customer name past inline", "one more customer name past inline", "one more customer name past inline"],
            names.Order(StringComparer.Ordinal).ToArray());
    }

    // A main of two rows (GenerateIndex) and a --ref "ref" of three customer names.
    private static DuckDBSqlProcessor TwoSourceProcessor(string query)
    {
        var mainSchema = new Schema(new[] { new Field("GenerateIndex", Int64Type.Default, nullable: false) }, null);
        var mainBatch = new RecordBatch(mainSchema, new IArrowArray[] { new Int64Array.Builder().Append(0).Append(1).Build() }, 2);
        var refSchema = new Schema(new[] { new Field("Name", StringType.Default, nullable: false) }, null);
        var refBatch = new RecordBatch(refSchema, new IArrowArray[]
        {
            new StringArray.Builder().Append("a customer name past inline").Append("one more customer name past inline").Append("a third one").Build(),
        }, 3);

        var channelMain = Channel.CreateUnbounded<RecordBatch>();
        channelMain.Writer.TryWrite(mainBatch);
        channelMain.Writer.Complete();
        var channelRef = Channel.CreateUnbounded<RecordBatch>();
        channelRef.Writer.TryWrite(refBatch);
        channelRef.Writer.Complete();

        var mock = new Mock<IMemoryChannelRegistry>();
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("main", It.IsAny<CancellationToken>())).ReturnsAsync(mainSchema);
        mock.Setup(r => r.GetArrowChannel("main")).Returns((channelMain, mainSchema));
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("ref", It.IsAny<CancellationToken>())).ReturnsAsync(refSchema);
        mock.Setup(r => r.GetArrowChannel("ref")).Returns((channelRef, refSchema));

        return new DuckDBSqlProcessor(
            mock.Object, query, "main", "main",
            refAliases: ["ref"], refChannelAliases: ["ref"],
            NullLogger<DuckDBSqlProcessor>.Instance);
    }

    private static async Task<int> CountRowsAsync(DuckDBSqlProcessor processor)
    {
        var rows = 0;
        await foreach (var b in processor.ReadRecordBatchesAsync())
            using (b) rows += b.Length;
        return rows;
    }

    /// <summary>
    /// A stream is read to its end by one scan: a --ref the plan scans twice is read once through a CTE put in
    /// front of the query, and the query's own columns come out as written.
    /// </summary>
    [Fact]
    public async Task OpenAsync_RefScannedTwice_IsReadOnceAndServesEveryScan()
    {
        await using var processor = TwoSourceProcessor(
            "SELECT s.GenerateIndex, a.Name, b.Name FROM main s JOIN ref a ON true JOIN ref b ON true");

        await processor.OpenAsync();

        Assert.Equal(["GenerateIndex", "Name", "Name"], processor.Schema!.FieldsList.Select(f => f.Name).ToArray());
        Assert.Equal(2 * 3 * 3, await CountRowsAsync(processor));
    }

    /// <summary>The CTE joins a WITH list the query opens itself, past a leading comment.</summary>
    [Fact]
    public async Task OpenAsync_RefScannedTwiceInsideTheQuerysOwnWith_IsReadOnce()
    {
        await using var processor = TwoSourceProcessor(
            "-- pairs\nWITH pairs AS (SELECT a.Name AS A, b.Name AS B FROM ref a JOIN ref b ON a.Name < b.Name) " +
            "SELECT s.GenerateIndex, p.A, p.B FROM main s JOIN pairs p ON true ORDER BY 1, 2, 3");

        await processor.OpenAsync();

        Assert.Equal(2 * 3, await CountRowsAsync(processor));
    }

    /// <summary>
    /// The recursive part of a recursive CTE runs again at each iteration, so a --ref it joins is a source
    /// scanned more than once: read once, the recursion goes to its end (from 0: 1+3+9+27, from 1: 1+3+9).
    /// </summary>
    [Fact]
    public async Task OpenAsync_RefJoinedInARecursivePart_IsReadOnce()
    {
        await using var processor = TwoSourceProcessor(
            "WITH RECURSIVE t AS (SELECT GenerateIndex AS n FROM main UNION ALL " +
            "SELECT t.n + 1 FROM t JOIN ref r ON r.Name IS NOT NULL WHERE t.n < 3) SELECT count(*) AS c FROM t");

        await processor.OpenAsync();
        long count = 0;
        await foreach (var b in processor.ReadRecordBatchesAsync())
            using (b) count = ((Int64Array)b.Column(0)).GetValue(0)!.Value;

        Assert.Equal(53, count);
    }

    /// <summary>The main is a stream, never held whole: a plan that scans it twice is refused, naming it.</summary>
    [Fact]
    public async Task OpenAsync_MainScannedTwice_IsRefusedNamingIt()
    {
        await using var processor = TwoSourceProcessor(
            "SELECT a.GenerateIndex, b.GenerateIndex FROM main a JOIN main b ON true");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => processor.OpenAsync());

        Assert.Contains("'main' more than once", ex.Message);
    }

    /// <summary>
    /// A --ref is held in memory, so a scan reading no column (count(*)) alongside another scan
    /// succeeds and sees the full row count.
    /// </summary>
    [Fact]
    public async Task OpenAsync_ScanReadingNoColumnOfRefScannedTwice_Succeeds()
    {
        await using var processor = TwoSourceProcessor(
            "SELECT (SELECT count(*) FROM ref) AS n, r.Name FROM main s JOIN ref r ON true");

        await processor.OpenAsync();
        var counts = new List<long>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
        {
            using (b)
            {
                var col = (Int64Array)b.Column(0);
                for (var i = 0; i < b.Length; i++) counts.Add(col.GetValue(i)!.Value);
            }
        }

        Assert.Equal(6, counts.Count);
        Assert.All(counts, n => Assert.Equal(3L, n));
    }

    /// <summary>A second scan on --from that reads no column is still refused naming it.</summary>
    [Fact]
    public async Task OpenAsync_ScanReadingNoColumnOfMainScannedTwice_IsRefusedNamingIt()
    {
        await using var processor = TwoSourceProcessor(
            "SELECT (SELECT count(*) FROM main) AS n, s.GenerateIndex FROM main s");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => processor.OpenAsync());

        Assert.Contains("'main' more than once", ex.Message);
    }

    /// <summary>
    /// A recursive CTE referencing --from in its recursive step binds once in the AST but opens a new
    /// scan on each iteration; the second Init refuses it naming the alias.
    /// </summary>
    [Fact]
    public async Task OpenAsync_MainScannedInRecursiveCte_IsRefusedNamingIt()
    {
        await using var processor = TwoSourceProcessor(
            "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT t.n + 1 FROM t JOIN main m ON m.GenerateIndex >= 0 WHERE t.n < 3) " +
            "SELECT count(*) AS c FROM t");

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await processor.OpenAsync();
            await CountRowsAsync(processor);
        });

        Assert.Contains("'main' more than once", ex.Message);
    }

    /// <summary>
    /// Regression test: WHERE clauses on CDI streaming sources (duckdb_arrow_scan) must be applied.
    ///
    /// Root cause: duckdb_arrow_scan declares filter_pushdown=true. DuckDB removes the Filter
    /// operator from the plan assuming the scan will apply it. But the C API wrapper (FactoryGetNext)
    /// ignores ArrowStreamParameters — the filter was silently dropped, returning all rows.
    /// Fix: SET disabled_optimizers='filter_pushdown' forces DuckDB to keep Filter operators.
    /// </summary>
    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_WhereFilter_FiltersRowsCorrectly()
    {
        var field = new Field("n", Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);
        // 10 rows: 0..9
        var arr = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 10)).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 10);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT n FROM src WHERE n < 5", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        await processor.DisposeAsync();

        // Must be 5 rows (0,1,2,3,4), not 10
        Assert.Equal(5, batches.Sum(b => b.Length));
    }

    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_UuidColumn_PreservesArrowUuidExtension()
    {
        // Arrange: UUID column — verifies arrow_lossless_conversion is active
        // and duckdb_to_arrow_schema + duckdb_data_chunk_to_arrow preserve arrow.uuid
        var uuidField = ArrowTypeMapper.GetField("id", typeof(Guid));
        var schema = new Schema(new[] { uuidField }, null);

        var uuidBuilder = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(16);
        uuidBuilder.Append(ArrowTypeMapper.ToArrowUuidBytes(Guid.Parse("550e8400-e29b-41d4-a716-446655440000")));
        var uuidArr = uuidBuilder.Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { uuidArr }, 1);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT id FROM src", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        // Act
        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // Assert: schema from prepared statement has arrow.uuid
        Assert.NotNull(processor.Schema);
        var idField = Assert.Single(processor.Schema!.FieldsList);
        Assert.Equal("id", idField.Name);
        string? ext = null;
        processor.Schema.FieldsList[0].Metadata?.TryGetValue("ARROW:extension:name", out ext);
        Assert.Equal("arrow.uuid", ext, StringComparer.OrdinalIgnoreCase);

        // Assert: data round-trips correctly
        Assert.NotEmpty(resultBatches);
        Assert.Equal(1, resultBatches.Sum(b => b.Length));
    }

    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_Aggregation_ReturnsAggregatedResult()
    {
        // Verifies that the streaming path handles SQL aggregation correctly
        var field = new Field("n", Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);

        var arr = new Int32Array.Builder()
            .AppendRange(Enumerable.Range(1, 100))
            .Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 100);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT COUNT(*) AS cnt FROM src", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        Assert.NotEmpty(resultBatches);
        Assert.Equal(1, resultBatches.Sum(b => b.Length));

        var cntField = processor.Schema!.FieldsList.Single(f => f.Name == "cnt");
        Assert.NotNull(cntField);
    }

    // ── Arrow FFI Projection Regression Suite ──────────────────────────────────────────────

    /// <summary>
    /// Aggregate on a String column when the schema has fixed-width columns
    /// before it (FixedSizeBinary + String + Timestamp). Without the pre-flight EXPLAIN projection,
    /// DuckDB's arrow_scan reads children[0] (the fixed-width column) as a String, causing a
    /// SIGSEGV / SIGBUS in SetVectorString. With the projection, DuckDB receives children[0] = String.
    /// </summary>
    [Fact]
    public async Task AggregateOnString_MixedSchema_DoesNotCrash()
    {
        // Schema: UUID-like (FixedSizeBinary) + String + Timestamp
        var uuidField = new Field("id", new FixedSizeBinaryType(16), nullable: false);
        var nameField = new Field("username", StringType.Default, nullable: false);
        var tsField = new Field("last_login", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: false);
        var schema = new Schema(new[] { uuidField, nameField, tsField }, null);

        var uuidBuilder = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(16);
        for (int i = 0; i < 100; i++) uuidBuilder.Append(Enumerable.Repeat((byte)i, 16).ToArray());
        var uuidArr = uuidBuilder.Build();

        var nameArr = new StringArray.Builder()
            .AppendRange(Enumerable.Range(0, 100).Select(i => $"user_{i:D3}"))
            .Build();

        var epoch = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var tsArr = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"))
            .AppendRange(Enumerable.Range(0, 100).Select(i => epoch.AddSeconds(i)))
            .Build();

        var batch = new RecordBatch(schema, new IArrowArray[] { uuidArr, nameArr, tsArr }, 100);

        const string alias = "db";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry,
            "SELECT count(*) AS total, avg(length(username)) AS avg_len FROM db",
            alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // One row of aggregation results
        Assert.Equal(1, resultBatches.Sum(b => b.Length));

        // Verify total = 100
        var totalField = processor.Schema!.FieldsList.Single(f => f.Name == "total");
        Assert.NotNull(totalField);
    }

    /// <summary>
    /// Aggregate with a string expression in the aggregate
    /// ('xx' || username). Verifies that string concatenation in aggregates also works.
    /// </summary>
    [Fact]
    public async Task AggregateWithStringConcat_MixedSchema_DoesNotCrash()
    {
        var uuidField = new Field("id", new FixedSizeBinaryType(16), nullable: false);
        var nameField = new Field("username", StringType.Default, nullable: false);
        var schema = new Schema(new[] { uuidField, nameField }, null);

        var uuidBuilder = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(16);
        for (int i = 0; i < 50; i++) uuidBuilder.Append(Enumerable.Repeat((byte)i, 16).ToArray());
        var uuidArr = uuidBuilder.Build();

        var nameArr = new StringArray.Builder()
            .AppendRange(Enumerable.Range(0, 50).Select(i => $"u{i}"))
            .Build();

        var batch = new RecordBatch(schema, new IArrowArray[] { uuidArr, nameArr }, 50);

        const string alias = "db";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry,
            "SELECT count(*) AS total, avg(length('xx' || username)) AS avg_len FROM db",
            alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        Assert.Equal(1, resultBatches.Sum(b => b.Length));
    }

    /// <summary>
    /// Projection order test: when a query selects columns in non-schema order, the output
    /// must reflect the query order — not the schema order.
    ///
    /// Schema: [num (Int32), text (String)]
    /// Query:  SELECT text, num FROM src       ← text first, num second
    ///
    /// Without the projection order fix, the stream returns [num_data, text_data] (schema order).
    /// DuckDB reads children[0] as "text" (Int32 data) → type mismatch → wrong output or crash.
    /// With the fix, the stream returns [text_data, num_data] → output is correct.
    /// </summary>
    [Fact]
    public async Task Projection_MultiColumn_OutputOrderMatchesQueryOrder_NotSchemaOrder()
    {
        // Schema order: num first, text second
        var numField = new Field("num", Int32Type.Default, nullable: false);
        var textField = new Field("text", StringType.Default, nullable: false);
        var schema = new Schema(new[] { numField, textField }, null);

        var numArr = new Int32Array.Builder().AppendRange(new[] { 10, 20, 30 }).Build();
        var textArr = new StringArray.Builder().AppendRange(new[] { "alpha", "beta", "gamma" }).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { numArr, textArr }, 3);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        // Query selects text FIRST, num SECOND — opposite of schema order
        var processor = new DuckDBSqlProcessor(
            registry, "SELECT text, num FROM src ORDER BY num", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // Output schema must have text first, num second
        Assert.Equal(2, processor.Schema!.FieldsList.Count);
        Assert.Equal("text", processor.Schema.FieldsList[0].Name);
        Assert.Equal("num", processor.Schema.FieldsList[1].Name);

        // Data: text column must contain strings ("alpha"/"beta"/"gamma"), not integers
        Assert.Equal(3, resultBatches.Sum(b => b.Length));
        var firstBatch = resultBatches[0];
        var textColumn = Assert.IsType<StringArray>(firstBatch.Column(0));
        var numColumn = Assert.IsType<Int32Array>(firstBatch.Column(1));

        // Verify text values are the original strings (not integers cast to strings)
        var textValues = Enumerable.Range(0, textColumn.Length).Select(i => textColumn.GetString(i)).ToList();
        Assert.All(textValues, v => Assert.True(v == "alpha" || v == "beta" || v == "gamma",
            $"Expected a string value but got '{v}'"));

        // Verify num values are the original integers
        var numValues = Enumerable.Range(0, numColumn.Length).Select(i => numColumn.GetValue(i)).ToList();
        Assert.All(numValues, v => Assert.True(v == 10 || v == 20 || v == 30,
            $"Expected 10/20/30 but got '{v}'"));
    }

    /// <summary>
    /// Projection order test with three columns: schema order A→B→C, query selects C→A.
    /// Verifies that a two-column subset projection in non-schema order is correctly mapped.
    /// </summary>
    [Fact]
    public async Task Projection_PartialAndReordered_CorrectOutput()
    {
        var aField = new Field("a", Int32Type.Default, nullable: false);
        var bField = new Field("b", StringType.Default, nullable: false);
        var cField = new Field("c", Int64Type.Default, nullable: false);
        var schema = new Schema(new[] { aField, bField, cField }, null);

        var aArr = new Int32Array.Builder().AppendRange(new[] { 1, 2, 3 }).Build();
        var bArr = new StringArray.Builder().AppendRange(new[] { "x", "y", "z" }).Build();
        var cArr = new Int64Array.Builder().AppendRange(new[] { 100L, 200L, 300L }).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { aArr, bArr, cArr }, 3);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        // Select c first, then a — skipping b entirely
        var processor = new DuckDBSqlProcessor(
            registry, "SELECT c, a FROM src ORDER BY a", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // Output schema: c first, a second
        Assert.Equal(2, processor.Schema!.FieldsList.Count);
        Assert.Equal("c", processor.Schema.FieldsList[0].Name);
        Assert.Equal("a", processor.Schema.FieldsList[1].Name);

        Assert.Equal(3, resultBatches.Sum(b => b.Length));
        var firstBatch = resultBatches[0];

        // c column (Int64): values should be 100, 200, 300
        var cColumn = Assert.IsType<Int64Array>(firstBatch.Column(0));
        var cValues = Enumerable.Range(0, cColumn.Length).Select(i => cColumn.GetValue(i)).ToList();
        Assert.All(cValues, v => Assert.True(v == 100 || v == 200 || v == 300,
            $"c column has wrong value '{v}'"));

        // a column (Int32): values should be 1, 2, 3
        var aColumn = Assert.IsType<Int32Array>(firstBatch.Column(1));
        var aValues = Enumerable.Range(0, aColumn.Length).Select(i => aColumn.GetValue(i)).ToList();
        Assert.All(aValues, v => Assert.True(v == 1 || v == 2 || v == 3,
            $"a column has wrong value '{v}'"));
    }

    /// <summary>
    /// Sentinel test: verifies that duckdb_execute_prepared_streaming is available in the
    /// bundled DuckDB native library and that the full streaming pipeline works end-to-end.
    ///
    /// If this test fails with EntryPointNotFoundException, the DuckDB native library was
    /// upgraded to a version that removed this deprecated function.
    ///
    /// ACTION REQUIRED — do NOT add a silent fallback to materialized execution:
    ///   1. Find the new non-deprecated streaming API that DuckDB introduced.
    ///   2. Migrate DuckDBSqlProcessor.ExecuteStreamingQuery and DuckDBArrowNativeMethods
    ///      to use it — single code path, no parallel logic.
    ///   3. If no streaming API is available yet, revert DuckDB.NET.Data.Full to the last
    ///      working version until one is available.
    /// </summary>
    [Fact]
    public async Task DuckDB_StreamingAPI_IsAvailable()
    {
        var field = new Field("n", Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);
        var arr = new Int32Array.Builder().AppendRange(Enumerable.Range(1, 10)).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 10);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT n FROM src", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        // EntryPointNotFoundException here = duckdb_execute_prepared_streaming was removed.
        // See the ACTION REQUIRED comment above — do not catch this exception.
        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        await processor.DisposeAsync();

        Assert.NotEmpty(batches);
        Assert.Equal(10, batches.Sum(b => b.Length));
    }

    // ── --duck-init Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that initSql runs before the main query on the same in-memory connection.
    /// Creates a TEMP TABLE in initSql and SELECTs from it in the main query — if initSql
    /// did not run, the query would fail with "table not found".
    /// No streaming source is required: the query is a plain table scan.
    /// </summary>
    [Fact]
    public async Task InitSql_CreatesObject_ObjectIsVisibleInQuery()
    {
        var emptyRegistry = new Mock<IMemoryChannelRegistry>().Object;

        var processor = new DuckDBSqlProcessor(
            emptyRegistry,
            "SELECT val FROM _init_check",
            mainAlias: "", mainChannelAlias: "",
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance,
            initSql: "CREATE TEMP TABLE _init_check (val VARCHAR); INSERT INTO _init_check VALUES ('duck_init_test')");

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        processor.Dispose();

        Assert.Equal(1, batches.Sum(b => b.Length));
        var col = Assert.IsType<Apache.Arrow.StringArray>(batches[0].Column(0));
        Assert.Equal("duck_init_test", col.GetString(0));
    }

    /// <summary>
    /// Verifies that a macro defined in initSql is callable in the main query.
    /// Uses only built-in DuckDB SQL — no extension download required.
    /// </summary>
    [Fact]
    public async Task InitSql_CreateMacro_MacroIsUsableInQuery()
    {
        var field = new Field("val", Apache.Arrow.Types.Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);
        var arr = new Int32Array.Builder().AppendRange(new[] { 1, 2, 3 }).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 3);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry,
            "SELECT triple(val) AS result FROM src ORDER BY val",
            alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance,
            initSql: "CREATE MACRO triple(x) AS x * 3");

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        processor.Dispose();

        Assert.Equal(3, batches.Sum(b => b.Length));
        var resultCol = Assert.IsType<Int32Array>(batches[0].Column(0));
        Assert.Equal(3, resultCol.GetValue(0));
        Assert.Equal(6, resultCol.GetValue(1));
        Assert.Equal(9, resultCol.GetValue(2));
    }

    /// <summary>
    /// Verifies that LOAD json (bundled in DuckDB.NET.Data.Full — no network required)
    /// via initSql enables JSON functions in the subsequent query.
    /// json is auto-loaded in DuckDB 1.5.x, so LOAD json is a no-op if already active;
    /// this test confirms it does not break normal processor operation.
    /// </summary>
    [Fact]
    public async Task InitSql_LoadJsonExtension_JsonFunctionsWorkInQuery()
    {
        var emptyRegistry = new Mock<IMemoryChannelRegistry>().Object;

        var processor = new DuckDBSqlProcessor(
            emptyRegistry,
            "SELECT json_array_length('[10, 20, 30]') AS cnt",
            mainAlias: "", mainChannelAlias: "",
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance,
            initSql: "LOAD json");

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        processor.Dispose();

        Assert.Equal(1, batches.Sum(b => b.Length));
        // json_array_length returns UBIGINT in DuckDB → UInt64Array in Arrow
        var col = batches[0].Column(0);
        Assert.NotNull(col);
        Assert.Equal(1, col.Length);
    }

    /// <summary>
    /// Verifies that the @file prefix causes initSql to be read from disk, not
    /// interpreted as inline SQL. The file creates a temp table; the query
    /// selects from it to confirm the file content was executed.
    /// </summary>
    [Fact]
    public async Task InitSql_AtFilePrefix_LoadsSqlFromFile()
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(tempFile,
                "CREATE TEMP TABLE _file_check (val VARCHAR); INSERT INTO _file_check VALUES ('from_file_test')");

            var emptyRegistry = new Mock<IMemoryChannelRegistry>().Object;
            var processor = new DuckDBSqlProcessor(
                emptyRegistry,
                "SELECT val FROM _file_check",
                mainAlias: "", mainChannelAlias: "",
                refAliases: [], refChannelAliases: [],
                NullLogger<DuckDBSqlProcessor>.Instance,
                initSql: $"@{tempFile}");

            await processor.OpenAsync();
            var batches = new List<RecordBatch>();
            await foreach (var b in processor.ReadRecordBatchesAsync())
                batches.Add(b);
            processor.Dispose();

            Assert.Equal(1, batches.Sum(b => b.Length));
            var col = Assert.IsType<Apache.Arrow.StringArray>(batches[0].Column(0));
            Assert.Equal("from_file_test", col.GetString(0));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    /// <summary>
    /// Verifies that a syntax error in initSql causes OpenAsync to throw before any
    /// query execution — the error must propagate and not be silently swallowed.
    /// </summary>
    [Fact]
    public async Task InitSql_InvalidSql_ThrowsOnOpenAsync()
    {
        var emptyRegistry = new Mock<IMemoryChannelRegistry>().Object;

        var processor = new DuckDBSqlProcessor(
            emptyRegistry,
            "SELECT 1 AS n",
            mainAlias: "", mainChannelAlias: "",
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance,
            initSql: "THIS IS NOT VALID SQL");

        await Assert.ThrowsAnyAsync<Exception>(() => processor.OpenAsync());
        processor.Dispose();
    }

    /// <summary>
    /// Verifies that DuckDB's projection pushdown correctly handles nested Arrow Struct fields.
    /// When querying a sub-property like `User.Role`, the processor should pass the entire `User`
    /// root column to DuckDB to prevent native crashes, and the output should contain only `Role`.
    /// </summary>
    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_NestedStructProjection_ReturnsNativeExtraction()
    {
        // Schema: User = Struct { Id: Int32, Role: String }
        var idField = new Field("Id", Int32Type.Default, nullable: false);
        var roleField = new Field("Role", StringType.Default, nullable: false);
        var userStructType = new StructType(new[] { idField, roleField });

        var userField = new Field("User", userStructType, nullable: false);
        var schema = new Schema(new[] { userField }, null);

        // Data arrays
        var idArr = new Int32Array.Builder().AppendRange(new[] { 1, 2 }).Build();
        var roleArr = new StringArray.Builder().AppendRange(new[] { "Admin", "Guest" }).Build();

        // Valid buffer for the struct array (2 items, both valid)
        var validBuf = new ArrowBuffer.Builder<byte>(1).Append(0xFF).Build();
        var structArr = new StructArray(userStructType, 2, new IArrowArray[] { idArr, roleArr }, validBuf, 0);

        var batch = new RecordBatch(schema, new IArrowArray[] { structArr }, 2);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT User.Role FROM src ORDER BY User.Id", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // Output schema must only have 'Role'
        Assert.Single(processor.Schema!.FieldsList);
        Assert.Equal("Role", processor.Schema.FieldsList[0].Name);

        // Data verification
        Assert.Single(resultBatches);
        var firstBatch = resultBatches[0];
        Assert.Equal(2, firstBatch.Length);

        var roleColumn = Assert.IsType<StringArray>(firstBatch.Column(0));
        Assert.Equal("Admin", roleColumn.GetString(0));
        Assert.Equal("Guest", roleColumn.GetString(1));
    }

    [Fact]
    public async Task ReadRecordBatches_QueryFailsMidStream_FailsTheRead()
    {
        const string alias = "src";
        var schema = new Schema(new[] { new Field("n", Int32Type.Default, nullable: false) }, null);
        var registry = BuildRegistry(alias, schema, []);

        await using var processor = new DuckDBSqlProcessor(
            registry,
            "SELECT CASE WHEN i < 200000 THEN i ELSE error('boom') END AS v FROM range(400000) t(i)",
            alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);
        await processor.OpenAsync();

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var batch in processor.ReadRecordBatchesAsync()) batch.Dispose();
        });

        // Not the query's own words: with several tasks running, DuckDB may report one it interrupted instead.
        Assert.StartsWith("DuckDB failed while streaming the result", failure.Message);
    }

    [Fact]
    public unsafe void DuckDB_ErrorDataMessage_ReturnsDuckDbErrorWithoutEntryPointNotFound()
    {
        Apache.Arrow.C.CArrowSchema ffiSchema = default;
        var errData = DtPipe.Adapters.Shared.Infrastructure.DuckDb.DuckDbArrowNative.DuckDBToArrowSchema(
            IntPtr.Zero, null, null, 0, &ffiSchema);

        Assert.NotEqual(IntPtr.Zero, errData);
        try
        {
            var msg = System.Runtime.InteropServices.Marshal.PtrToStringUTF8(
                DtPipe.Adapters.Shared.Infrastructure.DuckDb.DuckDbArrowNative.DuckDBErrorDataMessage(errData));
            Assert.False(string.IsNullOrWhiteSpace(msg));
        }
        finally
        {
            DtPipe.Adapters.Shared.Infrastructure.DuckDb.DuckDbArrowNative.DuckDBDestroyErrorData(ref errData);
        }
        Assert.Equal(IntPtr.Zero, errData);
    }

    /// <summary>
    /// Verifies 2048-row slicing across wide types (primitives, strings, decimal, timestamp, uuid, bool8,
    /// struct, list) and deterministic release of pooled --from buffers even through an ORDER BY query.
    /// </summary>
    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_LargeBatchExceeding2048Rows_SlicesCleanlyAcrossTypes()
    {
        const int rows = 5000;
        var pool = new TrackingMemoryPool();

        var uuidField = ArrowTypeMapper.GetField("uid", typeof(Guid));
        var bool8Field = new Field("flag8", Int8Type.Default, nullable: true,
            [KeyValuePair.Create("ARROW:extension:name", "arrow.bool8")]);
        var structType = new StructType([new Field("k", Int32Type.Default, nullable: false)]);
        var listType = new ListType(Int32Type.Default);
        var decType = new Decimal128Type(18, 2);
        var tsType = new TimestampType(TimeUnit.Microsecond, "UTC");

        var schema = new Schema(
        [
            new Field("id", Int32Type.Default, nullable: false),
            new Field("txt", StringType.Default, nullable: true),
            new Field("amt", decType, nullable: true),
            new Field("ts", tsType, nullable: true),
            uuidField,
            bool8Field,
            new Field("st", structType, nullable: false),
            new Field("lst", listType, nullable: false),
        ], null);

        var idB = new Int32Array.Builder();
        var txtB = new StringArray.Builder();
        var amtB = new Decimal128Array.Builder(decType);
        var tsB = new TimestampArray.Builder(tsType);
        var uidB = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(16);
        var b8B = new Int8Array.Builder();
        var stKB = new Int32Array.Builder();
        var lstB = new ListArray.Builder(Int32Type.Default);
        var lstValB = (Int32Array.Builder)lstB.ValueBuilder;

        var epoch = new DateTimeOffset(2024, 6, 1, 0, 0, 0, TimeSpan.Zero);
        for (var i = 0; i < rows; i++)
        {
            idB.Append(i);
            if (i % 7 == 0) txtB.AppendNull();
            else txtB.Append($"row_payload_exceeding_inline_limit_{i:D5}");
            amtB.Append(i * 1.25m);
            tsB.Append(epoch.AddSeconds(i));
            uidB.Append(ArrowTypeMapper.ToArrowUuidBytes(Guid.NewGuid()));
            if (i % 5 == 0) b8B.AppendNull();
            else b8B.Append((sbyte)(i % 2));
            stKB.Append(i * 10);
            lstB.Append();
            lstValB.Append(i);
        }

        var stValid = new ArrowBuffer.BitmapBuilder(rows);
        for (var i = 0; i < rows; i++) stValid.Append(true);

        var batch = new RecordBatch(schema,
        [
            idB.Build(pool),
            txtB.Build(pool),
            amtB.Build(pool),
            tsB.Build(pool),
            uidB.Build(),
            b8B.Build(pool),
            new StructArray(structType, rows, [stKB.Build(pool)], stValid.Build(pool)),
            lstB.Build(pool),
        ], rows);

        const string alias = "big";
        var registry = BuildRegistry(alias, schema, [batch]);

        await using (var processor = new DuckDBSqlProcessor(
            registry,
            "SELECT id, txt, amt, ts, uid, flag8, st.k AS sk, lst[1] AS first_elem FROM big ORDER BY id DESC",
            alias, alias, refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance))
        {
            await processor.OpenAsync();
            var readRows = 0;
            int? firstId = null;
            int? lastId = null;
            await foreach (var b in processor.ReadRecordBatchesAsync())
            {
                using (b)
                {
                    var ids = (Int32Array)b.Column(0);
                    if (b.Length > 0)
                    {
                        firstId ??= ids.GetValue(0);
                        lastId = ids.GetValue(b.Length - 1);
                    }
                    readRows += b.Length;
                }
            }
            Assert.Equal(rows, readRows);
            Assert.Equal(rows - 1, firstId);
            Assert.Equal(0, lastId);
        }

        Assert.Equal(0, pool.ActiveAllocations);
    }

    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_EmptyBatchesInterleaved_DoesNotTruncateStream()
    {
        var schema = new Schema([new Field("n", Int32Type.Default, nullable: false)], null);
        static RecordBatch MakeBatch(Schema s, params int[] values) =>
            new(s, [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        var registry = BuildRegistry("src", schema,
        [
            MakeBatch(schema),
            MakeBatch(schema, 1, 2, 3),
            MakeBatch(schema),
            MakeBatch(schema, 4, 5),
            MakeBatch(schema),
        ]);

        await using var processor = new DuckDBSqlProcessor(
            registry, "SELECT n FROM src ORDER BY n", "src", "src",
            refAliases: [], refChannelAliases: [], NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var values = new List<int>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
        {
            using (b)
            {
                var col = (Int32Array)b.Column(0);
                for (var i = 0; i < b.Length; i++) values.Add(col.GetValue(i)!.Value);
            }
        }
        Assert.Equal([1, 2, 3, 4, 5], values);

        // 0-row stream still yields schema and 0 rows.
        var emptyReg = BuildRegistry("src", schema, [MakeBatch(schema)]);
        await using var emptyProc = new DuckDBSqlProcessor(
            emptyReg, "SELECT n FROM src", "src", "src",
            refAliases: [], refChannelAliases: [], NullLogger<DuckDBSqlProcessor>.Instance);
        await emptyProc.OpenAsync();
        Assert.Single(emptyProc.Schema!.FieldsList);
        Assert.Equal(0, await CountRowsAsync(emptyProc));
    }

    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_ArrowBool8AndJsonExtensions_RoundTripUnderEdgesOnly()
    {
        var bool8Field = new Field("b", Int8Type.Default, nullable: true,
            [KeyValuePair.Create("ARROW:extension:name", "arrow.bool8")]);
        var jsonField = new Field("j", StringType.Default, nullable: true,
            [KeyValuePair.Create("ARROW:extension:name", "arrow.json")]);
        var schema = new Schema([bool8Field, jsonField], null);

        var batch = new RecordBatch(schema,
        [
            new Int8Array.Builder().Append(1).Append(0).AppendNull().Build(),
            new StringArray.Builder().Append("{\"a\":1}").Append("{\"a\":2}").AppendNull().Build(),
        ], 3);

        var registry = BuildRegistry("src", schema, [batch]);
        await using var processor = new DuckDBSqlProcessor(
            registry, "SELECT b, j, typeof(b) AS tb, typeof(j) AS tj FROM src WHERE b = true", "src", "src",
            refAliases: [], refChannelAliases: [], NullLogger<DuckDBSqlProcessor>.Instance,
            edgesOnly: true);

        await processor.OpenAsync();
        var bField = processor.Schema!.FieldsList[0];
        Assert.IsType<Int8Type>(bField.DataType);
        Assert.Equal("arrow.bool8", bField.Metadata["ARROW:extension:name"]);
        Assert.Equal(typeof(bool), ArrowTypeMapper.GetClrTypeFromField(bField));

        var jField = processor.Schema.FieldsList[1];
        Assert.IsType<StringType>(jField.DataType);
        Assert.Equal("arrow.json", jField.Metadata["ARROW:extension:name"]);

        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);

        Assert.Equal(1, batches.Sum(b => b.Length));
        var rowBatch = batches[0];
        Assert.Equal(true, ArrowTypeMapper.GetValueForField(rowBatch.Column(0), bField, 0));
        Assert.Equal("{\"a\":1}", ((StringArray)rowBatch.Column(1)).GetString(0));
        Assert.Equal("BOOLEAN", ((StringArray)rowBatch.Column(2)).GetString(0));
        Assert.Equal("JSON", ((StringArray)rowBatch.Column(3)).GetString(0));
        foreach (var b in batches) b.Dispose();
    }

    /// <summary>
    /// Joining --from with read_parquet triggers a post-prepare rebind in DuckDB during streaming
    /// execution. Prepare-scoped bind tracking must allow that rebind rather than rejecting it as a
    /// duplicate scan.
    /// </summary>
    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_MainJoinedWithReadParquet_RebindsAndFiltersCleanly()
    {
        var tempParquet = Path.Combine(Path.GetTempPath(), $"dtpipe-ext-{Guid.NewGuid():N}.parquet");
        try
        {
            await using (var seedConn = new global::DuckDB.NET.Data.DuckDBConnection("DataSource=:memory:"))
            {
                await seedConn.OpenAsync();
                await using var cmd = seedConn.CreateCommand();
                cmd.CommandText = $"COPY (SELECT i::BIGINT AS id, (i // 100)::INT AS bucket, 'v_' || i::VARCHAR AS val FROM range(0, 500) t(i)) TO '{tempParquet}' (FORMAT PARQUET)";
                await cmd.ExecuteNonQueryAsync();
            }

            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
            var batch = new RecordBatch(schema, [new Int64Array.Builder().AppendRange(Enumerable.Range(0, 500).Select(i => (long)i)).Build()], 500);
            var registry = BuildRegistry("main", schema, [batch]);

            await using var processor = new DuckDBSqlProcessor(
                registry,
                $"SELECT m.id, e.val FROM main m JOIN read_parquet('{tempParquet}') e ON m.id = e.id WHERE e.bucket = 2 ORDER BY m.id",
                "main", "main", refAliases: [], refChannelAliases: [],
                NullLogger<DuckDBSqlProcessor>.Instance);

            await processor.OpenAsync();
            var ids = new List<long>();
            await foreach (var b in processor.ReadRecordBatchesAsync())
            {
                using (b)
                {
                    var col = (Int64Array)b.Column(0);
                    for (var i = 0; i < b.Length; i++) ids.Add(col.GetValue(i)!.Value);
                }
            }

            Assert.Equal(100, ids.Count);
            Assert.Equal(200L, ids[0]);
            Assert.Equal(299L, ids[^1]);
        }
        finally
        {
            if (File.Exists(tempParquet)) File.Delete(tempParquet);
        }
    }
}

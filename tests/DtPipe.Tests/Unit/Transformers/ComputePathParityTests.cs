using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using DtPipe.Core.Models;
using DtPipe.Tests.Unit.Conformance;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Services;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// A batch under 1024 rows (what <c>--dry-run 10</c> reads) and a batch over it (what a real run reads, in
/// parallel) give the same cells, or the same error, for every Arrow type: sample mode must show what the run
/// writes. The large batch is run twice, on a provider that gives each thread an engine and on one that does
/// not, so every row of the parallel path is held against the sequential one.
/// </summary>
public class ComputePathParityTests : IDisposable
{
    private const int Small = 10;
    private const int Large = 2048;

    private readonly JsEngineProvider _js = new();
    private readonly SingleEngineProvider _single;

    public ComputePathParityTests() => _single = new SingleEngineProvider(_js);

    public void Dispose() => _js.Dispose();

    /// <summary>A provider that lends one engine, so the transformer keeps to the sequential path whatever the batch holds.</summary>
    private sealed class SingleEngineProvider(IJsEngineProvider inner) : IJsEngineProvider
    {
        public Jint.Engine GetEngine() => inner.GetEngine();

        public void Dispose() { }
    }

    /// <summary>Catalogue cases a compute stage cannot hold as a column, and why: they stand for no CLR type it can declare.</summary>
    private static readonly IReadOnlyDictionary<string, string> NotComputable = new Dictionary<string, string>
    {
        ["struct"] = "a struct has no CLR type a column can declare",
        ["map"] = "a map has no CLR type a column can declare",
        ["list<struct>"] = "a struct has no CLR type a column can declare",
    };

    public static TheoryData<string> CatalogueCases()
    {
        var names = new TheoryData<string>();
        foreach (var @case in ArrowTypeCases.All)
            if (!NotComputable.ContainsKey(@case.Name)) names.Add(@case.Name);
        return names;
    }

    /// <summary>The script hands a column's own cell back, so the output column is built from every value the type holds.</summary>
    [Theory]
    [MemberData(nameof(CatalogueCases))]
    public async Task A_Column_Of_Each_Arrow_Type_Is_Rewritten_The_Same_On_Both_Paths(string name)
    {
        var @case = ArrowTypeCases.Named(name);
        var clr = ArrowTypeMap.GetClrTypeFromField(@case.Field);
        var column = new PipeColumnInfo("v", clr, true);

        await AssertSameOnBothPaths("v:row.v", column, n => Tile(@case.Build(), n), @case.Field);
    }

    public static TheoryData<string, string> ScriptCases()
    {
        // Values a script may return, tried under each declared output type: a number, a text, a boolean, a
        // date, a null, and values the type refuses.
        string[] hints = ["int32", "int64", "double", "float32", "decimal", "string", "bool", "datetime", "datetimeoffset", "uuid"];
        string[] scripts =
        [
            "row.Id * 7 + 0.5",
            "row.Id % 3 === 0 ? null : row.Id + 1",
            "row.Id / 3",
            "row.Id % 2 === 0 ? true : false",
            "row.Id % 3 === 0 ? null : '2.5'",
            "'é' + row.Id",
            "new Date(Date.UTC(2024, 0, 1) + row.Id * 86400000 + 123)",
            "'2024-01-0' + (1 + row.Id % 9) + 'T01:02:03'",
            "'6f9619ff-8b86-d011-b42d-00c04fc964ff'",
            "'abc'",
            "3e10",
            "1e30",
            "{ a: 1 }",
        ];

        var cases = new TheoryData<string, string>();
        foreach (var hint in hints)
            foreach (var script in scripts)
                cases.Add(hint, script);
        return cases;
    }

    [Theory]
    [MemberData(nameof(ScriptCases))]
    public async Task A_Script_Result_Becomes_The_Same_Cell_On_Both_Paths(string hint, string script)
    {
        var id = new PipeColumnInfo("Id", typeof(int), false);

        await AssertSameOnBothPaths($"v:{hint}:{script}", id, IdColumn, field: null);
    }

    private async Task AssertSameOnBothPaths(string script, PipeColumnInfo column, Func<int, IArrowArray> input, Field? field)
    {
        var small = await Run(_js, script, column, input(Small), field);
        var sequential = await Run(_single, script, column, input(Large), field);
        var parallel = await Run(_js, script, column, input(Large), field);

        Assert.Equal(small, sequential.Take(Small).ToArray());
        Assert.Equal(sequential, parallel);
    }

    /// <summary>
    /// The cells of the output column as text, one per row, or the error that stopped the batch. A cell is read
    /// back through the generic value reader, so what is compared is what a writer would be handed.
    /// </summary>
    private static async Task<string[]> Run(IJsEngineProvider provider, string script, PipeColumnInfo column, IArrowArray input, Field? field)
    {
        var transformer = new ComputeDataTransformer(new ComputeOptions { Compute = [script] }, provider);
        await transformer.InitializeAsync([column]);

        var schemaField = field ?? new Field(column.Name, input.Data.DataType, true);
        using var batch = new RecordBatch(new Schema([schemaField], null), [input], input.Length);
        try
        {
            using var output = await transformer.TransformBatchAsync(batch);
            var outColumn = output!.Column(output.Schema.GetFieldIndex("v"));
            var outField = output.Schema.GetFieldByName("v");
            var read = ArrowTypeMap.CompileValueReader(outColumn, outField);
            return Enumerable.Range(0, outColumn.Length)
                .Select(i => $"{outField.DataType.Name}: {Describe(read(i))}")
                .ToArray();
        }
        catch (Exception e)
        {
            return [DescribeError(e)];
        }
        finally
        {
            input.Dispose();
        }
    }

    private static IArrowArray IdColumn(int length)
    {
        var builder = new Int32Array.Builder();
        for (var i = 0; i < length; i++) builder.Append(i);
        return builder.Build();
    }

    /// <summary>A column of <paramref name="length"/> rows made of a catalogue case's rows, repeated.</summary>
    private static IArrowArray Tile(IArrowArray one, int length)
    {
        // Apache.Arrow joins neither a null column nor a dictionary one.
        if (one is NullArray) return new NullArray(length);
        if (one is DictionaryArray dictionary)
            return new DictionaryArray((DictionaryType)dictionary.Data.DataType, Tile(dictionary.Indices, length), dictionary.Dictionary);

        var parts = new List<IArrowArray>();
        for (var rows = 0; rows < length; rows += one.Length)
            parts.Add(rows + one.Length <= length ? one : ArrowArrayFactory.Slice(one, 0, length - rows));
        return ArrowArrayConcatenator.Concatenate(parts)!;
    }

    /// <summary>
    /// An error by what does not depend on the row that raised it: the parallel path reports whichever failing
    /// row's error comes first, and a format error quotes its row's value.
    /// </summary>
    private static string DescribeError(Exception error)
    {
        const string Evaluating = "Error evaluating compute script for column";
        var chain = new List<string>();
        for (var e = error; e is not null; e = e.InnerException)
        {
            var message = e.Message.StartsWith(Evaluating, StringComparison.Ordinal)
                ? e.Message[..(e.Message.IndexOf("': ", StringComparison.Ordinal) + 1)]
                : e.InnerException is null ? "" : e.Message;
            chain.Add($"{e.GetType().Name} {message}".TrimEnd());
        }
        return "throws " + string.Join(" -> ", chain);
    }

    private static string Describe(object? value) => value switch
    {
        null => "null",
        string s => $"string {s}",
        byte[] bytes => $"bytes {Convert.ToHexString(bytes)}",
        System.Collections.IDictionary map => $"map {{{string.Join(", ", map.Keys.Cast<object>().Select(k => $"{Describe(k)}: {Describe(map[k])}"))}}}",
        System.Collections.IEnumerable list => $"list [{string.Join(", ", list.Cast<object?>().Select(Describe))}]",
        IFormattable f => $"{value.GetType().Name} {f.ToString(null, CultureInfo.InvariantCulture)}",
        _ => $"{value.GetType().Name} {Convert.ToString(value, CultureInfo.InvariantCulture)}",
    };
}

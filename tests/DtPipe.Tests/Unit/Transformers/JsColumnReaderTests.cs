using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using DtPipe.Tests.Unit.Conformance;
using DtPipe.Transformers.Services;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// A column a script reads through the Arrow row view gives, cell by cell, what the generic value reader
/// gives once converted for JavaScript: the same JavaScript value of the same type, null for a null, the
/// same exception where it throws. The conformance table's catalogue holds one case per Arrow type, so a
/// type added there is read here too, and a branch of the view's typed reader that drifts from the
/// generic one fails here.
/// </summary>
public class JsColumnReaderTests : IDisposable
{
    private readonly JsEngineProvider _js = new();

    public void Dispose() => _js.Dispose();

    public static TheoryData<string> CatalogueCases()
    {
        var names = new TheoryData<string>();
        foreach (var @case in ArrowTypeCases.All) names.Add(@case.Name);
        return names;
    }

    [Theory]
    [MemberData(nameof(CatalogueCases))]
    public void A_Column_Reads_As_The_Generic_Reader_Reads_It(string name)
    {
        var @case = ArrowTypeCases.Named(name);
        AssertSameReading(@case.Build(), @case.Field);
    }

    /// <summary>A column without a null takes its own branch of the typed reader.</summary>
    [Theory]
    [MemberData(nameof(CatalogueCases))]
    public void A_Column_Without_A_Null_Reads_As_The_Generic_Reader_Reads_It(string name)
    {
        var @case = ArrowTypeCases.Named(name);
        var column = @case.Build();
        if (column.NullCount == 0) return;

        var withoutNull = ArrowArrayFactory.Slice(column, 0, 1);
        if (withoutNull.NullCount != 0) return;

        AssertSameReading(withoutNull, @case.Field);
    }

    private void AssertSameReading(IArrowArray array, Field field)
    {
        var engine = _js.GetEngine();
        for (var i = 0; i < array.Length; i++)
        {
            var index = i;
            var expected = Describe(() => JsValueConversion.ClrToJsValue(engine, ArrowTypeMap.CompileValueReader(array, field)(index)));
            var actual = Describe(() => ArrowRowObjectInstance.CompileJsReader(engine, array, field)(index));
            Assert.Equal(expected, actual);
        }
    }

    private static string Describe(Func<JsValue> read)
    {
        try { return Describe(read()); }
        catch (Exception e) { return $"throws {e.GetType().Name}: {e.Message}"; }
    }

    /// <summary>A value as text that tells apart what <c>==</c> does not: a number's bits, a type, an object's keys in order.</summary>
    private static string Describe(JsValue value) => value switch
    {
        _ when value.IsNull() => "null",
        _ when value.IsUndefined() => "undefined",
        _ when value.IsBoolean() => $"boolean {value.AsBoolean()}",
        _ when value.IsNumber() => $"number {BitConverter.DoubleToInt64Bits(value.AsNumber()):X}",
        _ when value.IsString() => $"string {value.AsString()}",
        _ when value.IsBigInt() => $"bigint {value}",
        ArrayInstance array => $"array [{string.Join(", ", Enumerable.Range(0, (int)array.Get("length").AsNumber()).Select(i => Describe(array.Get(i))))}]",
        ObjectInstance obj => $"{obj.GetType().Name} {{{string.Join(", ", obj.GetOwnPropertyKeys().Select(k => $"{k}: {Describe(obj.Get(k))}"))}}} {Convert.ToString(obj.ToObject(), CultureInfo.InvariantCulture)}",
        _ => $"{value.Type} {value}"
    };
}

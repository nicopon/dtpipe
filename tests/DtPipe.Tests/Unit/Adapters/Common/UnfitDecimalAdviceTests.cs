using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using DtPipe.Adapters.Common;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Common;

/// <summary>
/// The refusal of a decimal that does not fit names the column and its width and says where to cast;
/// it never carries a value.
/// </summary>
public class UnfitDecimalAdviceTests
{
    [Fact]
    public void A_Column_That_Declared_No_Width_Says_So()
    {
        var field = ArrowTypeMap.GetField("DENSITY", ArrowTypeMap.DefaultDecimal);

        var message = UnfitDecimalAdvice.Compose(field);

        Assert.Contains("Column 'DENSITY' (no declared precision, read as decimal(38, 18))", message);
        Assert.Contains("Cast it in the source query", message);
    }

    [Fact]
    public void A_Column_That_Declared_Its_Width_Does_Not_Claim_It_Declared_None()
    {
        var field = new Field("TOTAL", new Decimal128Type(10, 2), true);

        var message = UnfitDecimalAdvice.Compose(field);

        Assert.Contains("Column 'TOTAL' (declared decimal(10, 2))", message);
        Assert.DoesNotContain("no declared precision", message);
    }

    [Fact]
    public void The_Refusal_Keeps_The_Fault_It_Explains()
    {
        var schema = new Schema.Builder()
            .Field(new Field("ID", Int64Type.Default, false))
            .Field(ArrowTypeMap.GetField("DENSITY", ArrowTypeMap.DefaultDecimal))
            .Build();
        var fault = new DecimalOutOfRangeException(1, ArrowTypeMap.DefaultDecimalType, new OverflowException());

        var refusal = UnfitDecimalAdvice.Refuse(fault, schema);

        Assert.Contains("'DENSITY'", refusal.Message);
        Assert.Same(fault, refusal.InnerException);
    }
}

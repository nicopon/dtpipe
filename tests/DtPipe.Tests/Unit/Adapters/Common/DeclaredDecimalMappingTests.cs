using DtPipe.Adapters.DuckDB;
using DtPipe.Adapters.MySql;
using DtPipe.Adapters.Oracle;
using DtPipe.Adapters.PostgreSQL;
using DtPipe.Adapters.SqlServer;
using DtPipe.Adapters.Sqlite;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Common;

/// <summary>
/// One rule for every SQL dialect: a decimal the source declares a precision and scale for is
/// created at that width within what the engine accepts; anything else takes the engine's default
/// for the CLR type. SQLite has no fixed-point type to honour a declaration with.
/// </summary>
public class DeclaredDecimalMappingTests
{
    public static TheoryData<string, ITypeMapper, bool> Dialects => new()
    {
        { "postgresql", PostgreSqlTypeConverter.Instance, true },
        { "mysql", MySqlTypeConverter.Instance, true },
        { "sqlserver", SqlServerTypeConverter.Instance, true },
        { "oracle", OracleTypeConverter.Instance, true },
        { "duckdb", DuckDbTypeConverter.Instance, true },
        { "sqlite", SqliteTypeConverter.Instance, false },
    };

    [Theory]
    [MemberData(nameof(Dialects))]
    public void A_Declared_Decimal_Is_Created_At_Its_Width(string dialect, ITypeMapper mapper, bool honoursDeclaration)
    {
        var type = mapper.MapToProviderType(Column(typeof(decimal), 18, 9));

        if (honoursDeclaration)
            Assert.Contains("(18,9)", type.Replace(" ", ""));
        else
            Assert.Equal(mapper.MapToProviderType(typeof(decimal)), type);
    }

    [Theory]
    [MemberData(nameof(Dialects))]
    public void A_Nullable_Declared_Decimal_Is_Treated_Like_A_Decimal(string dialect, ITypeMapper mapper, bool honoursDeclaration)
    {
        var type = mapper.MapToProviderType(Column(typeof(decimal?), 10, 2));

        Assert.Equal(honoursDeclaration, type.Replace(" ", "").Contains("(10,2)"));
    }

    [Theory]
    [MemberData(nameof(Dialects))]
    public void An_Undeclared_Decimal_Takes_The_Engine_Default(string dialect, ITypeMapper mapper, bool honoursDeclaration)
        => Assert.Equal(mapper.MapToProviderType(typeof(decimal)), mapper.MapToProviderType(Column(typeof(decimal), null, null)));

    [Theory]
    [MemberData(nameof(Dialects))]
    public void A_Declaration_Out_Of_Range_Takes_The_Engine_Default(string dialect, ITypeMapper mapper, bool honoursDeclaration)
    {
        var fallback = mapper.MapToProviderType(typeof(decimal));

        Assert.Equal(fallback, mapper.MapToProviderType(Column(typeof(decimal), 0, 0)));
        Assert.Equal(fallback, mapper.MapToProviderType(Column(typeof(decimal), 10, 11)));
        Assert.Equal(fallback, mapper.MapToProviderType(Column(typeof(decimal), 10, -1)));
        Assert.Equal(fallback, mapper.MapToProviderType(Column(typeof(decimal), 10, null)));
    }

    [Fact]
    public void A_Width_Beyond_The_Engine_Takes_The_Default_Where_Another_Engine_Accepts_It()
    {
        // 60 digits: PostgreSQL's NUMERIC and MySQL's DECIMAL hold it, the 38-digit engines do not.
        Assert.Contains("(60,2)", Map(PostgreSqlTypeConverter.Instance, 60, 2));
        Assert.Contains("(60,2)", Map(MySqlTypeConverter.Instance, 60, 2));
        foreach (var narrow in new ITypeMapper[] { SqlServerTypeConverter.Instance, OracleTypeConverter.Instance, DuckDbTypeConverter.Instance })
            Assert.Equal(narrow.MapToProviderType(typeof(decimal)), Map(narrow, 60, 2));

        // MySQL stops at 30 decimals whatever the precision.
        Assert.Equal(MySqlTypeConverter.Instance.MapToProviderType(typeof(decimal)), Map(MySqlTypeConverter.Instance, 40, 31));
    }

    private static string Map(ITypeMapper mapper, int precision, int scale)
        => mapper.MapToProviderType(Column(typeof(decimal), precision, scale));

    [Theory]
    [MemberData(nameof(Dialects))]
    public void A_Declaration_On_Another_Type_Changes_Nothing(string dialect, ITypeMapper mapper, bool honoursDeclaration)
        => Assert.Equal(mapper.MapToProviderType(typeof(long)), mapper.MapToProviderType(Column(typeof(long), 18, 9)));

    private static PipeColumnInfo Column(Type clr, int? precision, int? scale)
        => new("c", clr, true, Precision: precision, Scale: scale);
}

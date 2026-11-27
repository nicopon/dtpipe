using DtPipe.Adapters.Oracle;
using Oracle.ManagedDataAccess.Types;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Oracle;

public sealed class OracleSchemaInspectorTests
{
    [Fact]
    public void ReadOutputString_Handles_OracleString_Null_And_DBNull()
    {
        Assert.Equal("", OracleSchemaInspector.ReadOutputString(null));
        Assert.Equal("", OracleSchemaInspector.ReadOutputString(DBNull.Value));
        Assert.Equal("", OracleSchemaInspector.ReadOutputString(OracleString.Null));
        Assert.Equal("HR", OracleSchemaInspector.ReadOutputString(new OracleString("HR")));
        Assert.Equal("USERS", OracleSchemaInspector.ReadOutputString("USERS"));
    }
}

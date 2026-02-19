namespace DtPipe.Core.Helpers;

public static class TypeHelper
{
    public static Type? ParseTypeHint(string typeHint)
    {
        return typeHint.ToLowerInvariant() switch
        {
            "string" => typeof(string),
            "int" => typeof(int),
            "long" => typeof(long),
            "double" => typeof(double),
            "decimal" => typeof(decimal),
            "bool" => typeof(bool),
            "datetime" => typeof(DateTime),
            "guid" => typeof(Guid),
            _ => null
        };
    }
}

namespace DtPipe.Jobs;

/// <summary>
/// The keys of a branch that say where it reads and writes and which provider settings it carries.
/// The distributed family and the CLI name them here; the CLI's own model derives the same names
/// from its property names, and <c>JobKeysTests</c> compares the two.
/// </summary>
public static class JobKeys
{
    public const string Input = "input";
    public const string Output = "output";
    public const string From = "from";
    public const string Ref = "ref";
    public const string ProviderOptions = "provider-options";
}

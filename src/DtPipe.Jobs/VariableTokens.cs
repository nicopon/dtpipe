using System.Text.RegularExpressions;

namespace DtPipe.Jobs;

/// <summary>A <c>${{…}}</c> of a text: where it stands, and what is inside it, trimmed.</summary>
public readonly record struct VariableToken(int Index, int Length, string Expression);

/// <summary>
/// The grammar of <c>${{…}}</c>, the one every reader of a job shares: the CLI that resolves it, the
/// studio that lists it, the coordinator that places it. A token is whatever stands between the braces,
/// trimmed. Its expression is either a reference, which names a scheme (<c>scheme://…</c>) and belongs
/// to the component that owns the scheme, or a variable name, an identifier the environment and a
/// node's datasets carry. Any other expression is refused: a copy of this grammar that read it
/// differently let the CLI run what the coordinator never placed.
/// </summary>
public static partial class VariableTokens
{
    /// <summary>Every token of <paramref name="text"/>, in order, each occurrence.</summary>
    public static IEnumerable<VariableToken> Of(string text) =>
        TokenPattern().Matches(text).Select(m => new VariableToken(m.Index, m.Length, m.Groups[1].Value.Trim()));

    /// <summary>The expressions of <paramref name="text"/>, each once, in order of first appearance.</summary>
    public static IReadOnlyList<string> Expressions(string text) =>
        Of(text).Select(t => t.Expression).Distinct(StringComparer.Ordinal).ToList();

    /// <summary>A reference: it names a scheme, so the component that owns the scheme resolves it.</summary>
    public static bool IsReference(string expression) => expression.IndexOf("://", StringComparison.Ordinal) > 0;

    /// <summary>A variable name: letters, digits and underscores, not starting with a digit.</summary>
    public static bool IsName(string expression) => NamePattern().IsMatch(expression);

    /// <summary>Why <paramref name="expression"/> is neither a reference nor a variable name; <c>null</c> when it is one.</summary>
    public static string? Refusal(string expression) =>
        IsReference(expression) || IsName(expression)
            ? null
            : $"${{{{{expression}}}}} is neither a variable name (letters, digits and underscores, not starting with a digit) nor a reference (scheme://…).";

    /// <summary>The refusals of every expression of <paramref name="text"/>, none when the text holds only references and names.</summary>
    public static IReadOnlyList<string> Refusals(string text) =>
        Expressions(text).Select(Refusal).OfType<string>().ToList();

    [GeneratedRegex(@"\$\{\{([^}]+)\}\}")]
    private static partial Regex TokenPattern();

    [GeneratedRegex(@"^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex NamePattern();
}

using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// A writer that says, from the Arrow schema alone, which columns its target format cannot carry.
/// A writer writes natively every type its format carries and refuses the rest; it never turns a
/// type it cannot write into one it can. The engine asks before the writer touches its target, in
/// the real run and in a sample alike, and turns the answer into advice; it never converts.
/// </summary>
public interface IColumnTypeGate
{
    /// <summary>
    /// The columns of <paramref name="schema"/> this writer refuses, empty when it writes them all.
    /// Reads the schema only: it never opens, reads or changes the target.
    /// </summary>
    IReadOnlyList<ColumnTypeRefusal> RefuseColumns(Schema schema);
}

/// <summary>
/// One column a writer cannot write: the Arrow field it received, and the types it would accept
/// in its place, most faithful first. <paramref name="Accepted"/> may be empty when no type keeps
/// the values.
/// </summary>
public sealed record ColumnTypeRefusal(Field Received, IReadOnlyList<IArrowType> Accepted);

/// <summary>
/// Every column a writer refused, raised before its target is touched. <see cref="Exception.Message"/>
/// is plain text the host formats.
/// </summary>
public sealed class ColumnTypeRefusedException(string writer, IReadOnlyList<ColumnTypeRefusal> refusals, string message)
    : Exception(message)
{
    public string Writer { get; } = writer;
    public IReadOnlyList<ColumnTypeRefusal> Refusals { get; } = refusals;
}

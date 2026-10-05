namespace DtPipe.Processors.Sql;

/// <summary>
/// The query reads the streaming <c>--from</c> source more than once. A stream hands its rows to the
/// first read and keeps none for the next, so letting the query run would return wrong results
/// without any error.
/// </summary>
public sealed class StreamedSourceReadRepeatedlyException : InvalidOperationException
{
    public StreamedSourceReadRepeatedlyException(string alias, int reads)
        : base(BuildMessage(alias, reads))
    {
        Alias = alias;
        Reads = reads;
    }

    public string Alias { get; }
    public int Reads { get; }

    private static string BuildMessage(string alias, int reads) =>
        $"The query reads the streaming source '{alias}' (--from {alias}) {reads} times, and a stream can only " +
        "be read once.\n" +
        "DuckDB consumes the rows as they arrive and keeps none, so every read after the first would find " +
        "nothing: the query would run and return wrong results with no error. dtpipe refuses it up front.\n" +
        "Ways out:\n" +
        $"  1. Read '{alias}' once and reuse the result through a materialized CTE:\n" +
        $"       WITH once AS MATERIALIZED (SELECT * FROM {alias}) SELECT ... FROM once a JOIN once b ON ...\n" +
        $"  2. Give '{alias}' to --ref instead of --from. A --ref is loaded in full into a table that can be " +
        "read as often as the query wants, and that table spills to disk when it outgrows memory. " +
        "A --sql branch needs no --from when all of its inputs are --ref.";
}

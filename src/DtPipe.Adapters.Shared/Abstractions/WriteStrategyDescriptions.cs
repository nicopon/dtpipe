namespace DtPipe.Adapters.Shared.Abstractions;

/// <summary>
/// What each write strategy does to the target, worded once for the writer options of every SQL
/// adapter: each strategy enum puts these on its members as <c>[Description]</c>, and the help reads
/// them by reflection. The behaviour is <see cref="BaseSqlDataWriter"/>'s, so the words sit beside it.
/// </summary>
public static class WriteStrategyDescriptions
{
    public const string Append =
        "Insert into the target, keeping its rows. The table is created from the source schema when it does not exist.";

    public const string Truncate =
        "Empty the target with TRUNCATE, then insert. The table is created when it does not exist; an existing one keeps its schema, indexes and constraints.";

    public const string DeleteThenInsert =
        "DELETE every row of the target, then insert: for a target where TRUNCATE is not allowed. The table is created when it does not exist; an existing one is kept.";

    public const string Recreate =
        "DROP the target if it exists, recreate it from the source schema, then insert. Its indexes, constraints and triggers are lost; a warning names a column or primary key the new table lacks.";

    public const string Upsert =
        "Update the rows whose key matches, insert the others. Needs a key: the target's primary key, or the key option.";

    public const string Ignore =
        "Insert only the rows whose key is absent from the target, leaving the others untouched. Needs a key, as Upsert does.";
}

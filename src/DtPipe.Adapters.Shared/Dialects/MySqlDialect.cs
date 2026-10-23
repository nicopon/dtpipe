using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.Shared.Dialects;

public class MySqlDialect : BaseSqlDialect
{
    /// <summary>MySQL 5.6+ enforces this for the next transaction.</summary>
    public override string? ReadOnlySessionSql => "SET TRANSACTION READ ONLY";

    private static readonly HashSet<string> ReservedKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN", "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DENSE_RANK", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE", "ELSEIF", "EMPTY", "ENCLOSED", "ESCAPED", "EXCEPT", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FIRST_VALUE", "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "FUNCTION", "GENERATED", "GET", "GRANT", "GROUP", "GROUPING", "GROUPS", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IO_AFTER_GTIDS", "IO_BEFORE_GTIDS", "IS", "ITERATE", "JOIN", "JSON_TABLE", "KEY", "KEYS", "KILL", "LAG", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MANUAL", "MASTER_BIND", "MASTER_SSL_VERIFY_SERVER_CERT", "MATCH", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NTH_VALUE", "NTILE", "NULL", "NUMERIC", "OF", "ON", "OPTIMIZE", "OPTIMIZER_COSTS", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "OVER", "PARALLEL", "PARTITION", "PERCENT_RANK", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE", "QUALIFY", "RANGE", "RANK", "READ", "READS", "READ_WRITE", "REAL", "RECURSIVE", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESIGNAL", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "ROW", "ROWS", "ROW_NUMBER", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SIGNAL", "SMALLINT", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STORED", "STRAIGHT_JOIN", "SYSTEM", "TABLE", "TABLESAMPLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "VIRTUAL", "WHEN", "WHERE", "WHILE", "WINDOW", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL"
    };

    /// <summary>
    /// The row alias bound to the staging SELECT in <see cref="BuildStagingMerge"/>. MySQL requires
    /// the alias to differ from the target table name, so it carries a prefix no user table will hold.
    /// </summary>
    private const string SourceAlias = "dtp_src";

    public override string Normalize(string identifier)
    {
        // Identity, deliberately. MySQL does not case-fold identifiers: column names are compared
        // case-insensitively but stored as written, and TABLE name case sensitivity depends on the
        // server's lower_case_table_names setting — a server property no dialect can know statically.
        // Folding here would rename columns the user asked for; leaving them alone matches either
        // server configuration, because the comparison is case-insensitive on both.
        return identifier;
    }

    public override string Quote(string identifier)
    {
        // MySQL escapes an embedded backtick by doubling it. ANSI_QUOTES mode would accept
        // double quotes instead, but backticks work under every sql_mode, so they are the safe form.
        return $"`{identifier.Replace("`", "``")}`";
    }

    protected override bool IsReservedKeyword(string identifier)
    {
        return ReservedKeywords.Contains(identifier);
    }

    protected override bool IsCaseMismatch(string identifier)
    {
        // Unquoted identifiers keep their case; nothing to preserve by quoting.
        return false;
    }

    public override string? TableDiscoveryQuery => "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name";

    /// <summary>
    /// F9 — MySQL staged merge via INSERT … SELECT … ON DUPLICATE KEY UPDATE.
    /// <para>
    /// Two MySQL-specific constraints shape this SQL. First, ON DUPLICATE KEY UPDATE fires on
    /// <em>any</em> PRIMARY KEY or UNIQUE index, never on a caller-named conflict target — so when the
    /// writer cannot verify that such an index exists (<see cref="MergeSpec.ConstraintVerified"/> false)
    /// the clause degenerates to a plain INSERT and duplicates land silently. That failure mode is why
    /// the unverified path falls back to an explicit DELETE+INSERT, exactly as DuckDB does.
    /// </para>
    /// <para>
    /// Second, the source row is referenced through a derived-table alias rather than the
    /// <c>VALUES()</c> function, which MySQL deprecated in 8.0.20; the derived-table form is the
    /// documented replacement for the INSERT … SELECT shape and emits no deprecation warning.
    /// </para>
    /// </summary>
    public override string BuildStagingMerge(MergeSpec spec)
    {
        var cols = spec.Columns.Select(c => Quote(c.Name)).ToList();
        var colList = string.Join(", ", cols);
        var nonKeys = spec.Columns
            .Where(c => !spec.KeyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase))
            .ToList();

        if (!spec.ConstraintVerified || spec.KeyColumns.Count == 0)
        {
            return BuildDeleteThenInsertFallback(spec, colList);
        }

        var insert = $"INSERT INTO {spec.QuotedTargetTable} ({colList}) " +
                     $"SELECT * FROM (SELECT {colList} FROM {spec.SourceTable}) AS {SourceAlias}";

        return spec.Mode switch
        {
            // A no-op assignment is what "leave the existing row alone" looks like here. INSERT IGNORE
            // would also skip the row, but it downgrades unrelated errors (truncation, bad values) to
            // warnings as well — too broad a silence for a strategy that only asked to skip conflicts.
            MergeMode.Ignore => $"{insert} ON DUPLICATE KEY UPDATE {NoOpAssignment(spec)}",
            MergeMode.Upsert when nonKeys.Count == 0 => $"{insert} ON DUPLICATE KEY UPDATE {NoOpAssignment(spec)}",
            MergeMode.Upsert => $"{insert} ON DUPLICATE KEY UPDATE " + string.Join(", ", nonKeys
                .Select(c => $"{Quote(c.Name)} = {SourceAlias}.{Quote(c.Name)}")),
            _ => insert,
        };
    }

    /// <summary>
    /// Assigns a key column to itself: syntactically an update, semantically a skip. Used for Ignore,
    /// and for an Upsert whose target is all-key (nothing left to update).
    /// </summary>
    private string NoOpAssignment(MergeSpec spec)
    {
        var key = Quote(spec.KeyColumns[0]);
        return $"{key} = {spec.QuotedTargetTable}.{key}";
    }

    /// <summary>
    /// Fallback for a target without a PK/UNIQUE index covering the keys. Multi-statement script,
    /// ';'-separated — the writer splits and executes the steps one at a time.
    /// </summary>
    private string BuildDeleteThenInsertFallback(MergeSpec spec, string colList)
    {
        var steps = new List<string>();

        if (spec.KeyColumns.Count > 0)
        {
            var join = string.Join(" AND ", spec.KeyColumns.Select(k =>
            {
                var safe = Quote(k);
                return $"t.{safe} = s.{safe}";
            }));

            // MySQL's multi-table DELETE names the table to purge before FROM.
            if (spec.Mode == MergeMode.Upsert)
                steps.Add($"DELETE t FROM {spec.QuotedTargetTable} AS t JOIN {spec.SourceTable} AS s ON {join}");
            else if (spec.Mode == MergeMode.Ignore)
                steps.Add($"DELETE s FROM {spec.SourceTable} AS s JOIN {spec.QuotedTargetTable} AS t ON {join}");
        }

        steps.Add($"INSERT INTO {spec.QuotedTargetTable} ({colList}) SELECT {colList} FROM {spec.SourceTable}");
        return string.Join(";", steps);
    }
}

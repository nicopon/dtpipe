using System.Data;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;

namespace DtPipe.Adapters.Oracle;

public sealed class OracleSchemaInspector : ISchemaInspector
{
	private readonly string _connectionString;
	private readonly string _tableName;
	private readonly ILogger _logger;

	public OracleSchemaInspector(string connectionString, string tableName, ILogger logger)
	{
		_connectionString = connectionString;
		_tableName = tableName;
		_logger = logger;
	}

	public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		if (_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug("Starting target schema inspection for table {Table}", _tableName);
		await using var connection = new OracleConnection(_connectionString);
		await connection.OpenAsync(ct);

		// Parse optional schema prefix (e.g., "MYSCHEMA.MyTable" → owner=MYSCHEMA, table=MyTable)
		string? inputOwner = null;
		string inputTable = _tableName;

		// Strip quotes if present for catalog lookup
		static string Unquote(string s) => s.StartsWith('"') && s.EndsWith('"') ? s[1..^1] : s;

		var dotIndex = _tableName.IndexOf('.');
		if (dotIndex > 0)
		{
			inputOwner = Unquote(_tableName[..dotIndex]);
			inputTable = Unquote(_tableName[(dotIndex + 1)..]);
		}
		else
		{
			inputTable = Unquote(_tableName);
		}

		bool hasOwner = !string.IsNullOrEmpty(inputOwner);
		string GetView(string viewSuffix) => hasOwner ? $"ALL_{viewSuffix}" : $"USER_{viewSuffix}";

		// Case-insensitive catalog lookup to find the actual table name
		var tablesView = GetView("TABLES");
		var findSql = hasOwner
			? $"SELECT table_name, num_rows FROM {tablesView} WHERE UPPER(owner) = UPPER(:p_owner) AND UPPER(table_name) = UPPER(:p_table)"
			: $"SELECT table_name, num_rows FROM {tablesView} WHERE UPPER(table_name) = UPPER(:p_table)";

		using var findCmd = connection.CreateCommand();
		findCmd.BindByName = true;
		findCmd.CommandText = findSql;
		if (hasOwner) findCmd.Parameters.Add(new OracleParameter("p_owner", inputOwner));
		findCmd.Parameters.Add(new OracleParameter("p_table", inputTable));

		if (_logger.IsEnabled(LogLevel.Debug))
			_logger.LogDebug("Case-insensitive table lookup: {Sql} with table={Table}", findSql, inputTable);

		var matches = new List<(string TableName, long? RowCount)>();
		using (var findReader = await findCmd.ExecuteReaderAsync(ct))
		{
			while (await findReader.ReadAsync(ct))
			{
				var catName = findReader.GetString(0);
				var rows = findReader.IsDBNull(1) ? (long?)null : Convert.ToInt64(findReader.GetValue(1));
				matches.Add((catName, rows));
			}
		}

		if (matches.Count == 0)
		{
			if (_logger.IsEnabled(LogLevel.Debug))
				_logger.LogDebug("Table {Table} not found in catalog", inputTable);
			return new TargetSchemaInfo([], false, null, null, null);
		}

		// Resolve ambiguity: if multiple tables match case-insensitively, prefer exact match
		string tableName;
		long? rowCount;

		if (matches.Count == 1)
		{
			tableName = matches[0].TableName;
			rowCount = matches[0].RowCount;
		}
		else
		{
			// Multiple matches (e.g., "MaTable" and "MATABLE" both exist)
			var exactMatch = matches.FirstOrDefault(m => m.TableName == inputTable);
			if (exactMatch.TableName != null)
			{
				tableName = exactMatch.TableName;
				rowCount = exactMatch.RowCount;
			}
			else
			{
				// No exact match — use uppercase version (Oracle default)
				var upperMatch = matches.FirstOrDefault(m => m.TableName == inputTable.ToUpperInvariant());
				if (upperMatch.TableName != null)
				{
					tableName = upperMatch.TableName;
					rowCount = upperMatch.RowCount;
				}
				else
				{
					tableName = matches[0].TableName;
					rowCount = matches[0].RowCount;
				}

				if (_logger.IsEnabled(LogLevel.Warning))
					_logger.LogWarning("Ambiguous table name '{Input}': found {Count} matches ({Names}). Using '{Resolved}'.",
						inputTable, matches.Count, string.Join(", ", matches.Select(m => m.TableName)), tableName);
			}
		}

		string owner = inputOwner ?? "";

		void AddOwnerParam(OracleCommand cmd)
		{
			if (hasOwner) cmd.Parameters.Add(new OracleParameter("p_owner", owner));
		}


		var colView = GetView("TAB_COLUMNS");
		var columnsSql = hasOwner
			? $@"SELECT
                column_name, data_type, nullable, data_length, data_precision, data_scale, char_length
            FROM {colView}
            WHERE owner = :p_owner AND table_name = :p_table
            ORDER BY column_id"
			: $@"SELECT
                column_name, data_type, nullable, data_length, data_precision, data_scale, char_length
            FROM {colView}
            WHERE table_name = :p_table
            ORDER BY column_id";

		using var columnsCmd = connection.CreateCommand();
		columnsCmd.BindByName = true;
		columnsCmd.CommandText = columnsSql;
		AddOwnerParam(columnsCmd);
		columnsCmd.Parameters.Add(new OracleParameter("p_table", tableName));

		var pkColumns = await GetConstraintColumnsAsync(connection, GetView("CONSTRAINTS"), GetView("CONS_COLUMNS"), "P", hasOwner, owner, tableName, ct);

		var uniqueColumns = await GetConstraintColumnsAsync(connection, GetView("CONSTRAINTS"), GetView("CONS_COLUMNS"), "U", hasOwner, owner, tableName, ct);

		long? sizeBytes = await GetTableSizeAsync(connection, GetView("SEGMENTS"), hasOwner, owner, tableName, ct);

		var columns = new List<TargetColumnInfo>();
		if (_logger.IsEnabled(LogLevel.Debug))
			_logger.LogDebug("Retrieving columns with SQL: {Sql}", columnsSql);
		using var reader = await columnsCmd.ExecuteReaderAsync(ct);
		while (await reader.ReadAsync(ct))
		{
			var colName = reader.GetString(0);
			var dataType = reader.GetString(1);
			var isNullable = reader.GetString(2) == "Y";
			var dataLength = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3);
			var precision = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4);
			var scale = reader.IsDBNull(5) ? (int?)null : reader.GetInt32(5);
			var charLength = reader.IsDBNull(6) ? (int?)null : reader.GetInt32(6);

			var nativeType = OracleTypeConverter.Instance.BuildNativeType(dataType, dataLength, precision, scale, charLength);
			var maxLength = charLength ?? dataLength;

			columns.Add(new TargetColumnInfo(
				colName,
				nativeType,
				OracleTypeConverter.Instance.MapFromProviderType(dataType),
				isNullable,
				pkColumns.Contains(colName),
				uniqueColumns.Contains(colName),
				maxLength,
				precision,
				scale,
				IsCaseSensitive: colName != colName.ToUpperInvariant()
			));
		}

		return new TargetSchemaInfo(
			columns,
			true,
			rowCount,
			sizeBytes,
			pkColumns.Count > 0 ? pkColumns.ToList() : null,
			uniqueColumns.Count > 0 ? uniqueColumns.ToList() : null,
			IsRowCountEstimate: true
		);
	}

	private async Task<HashSet<string>> GetConstraintColumnsAsync(OracleConnection connection, string consView, string consColView, string constraintType, bool hasOwner, string owner, string tableName, CancellationToken ct)
	{
		string sql;
		if (hasOwner)
		{
			sql = $@"
            SELECT cols.column_name
            FROM {consView} cons
            JOIN {consColView} cols ON cons.constraint_name = cols.constraint_name
                AND cons.owner = cols.owner
            WHERE cons.constraint_type = :p_type
              AND cons.owner = :p_owner
              AND cons.table_name = :p_table";
		}
		else
		{
			sql = $@"
            SELECT cols.column_name
            FROM {consView} cons
            JOIN {consColView} cols ON cons.constraint_name = cols.constraint_name
            WHERE cons.constraint_type = :p_type
              AND cons.table_name = :p_table";
		}

		var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		using var cmd = connection.CreateCommand();
		cmd.BindByName = true;
		cmd.CommandText = sql;
		cmd.Parameters.Add(new OracleParameter("p_type", constraintType));
		if (hasOwner) cmd.Parameters.Add(new OracleParameter("p_owner", owner));
		cmd.Parameters.Add(new OracleParameter("p_table", tableName));

		using var reader = await cmd.ExecuteReaderAsync(ct);
		while (await reader.ReadAsync(ct))
		{
			columns.Add(reader.GetString(0));
		}
		return columns;
	}

	private async Task<long?> GetTableSizeAsync(OracleConnection connection, string segmentsView, bool hasOwner, string owner, string tableName, CancellationToken ct)
	{
		try
		{
			var sql = hasOwner
				? $"SELECT bytes FROM {segmentsView} WHERE owner = :p_owner AND segment_name = :p_table AND segment_type = 'TABLE'"
				: $"SELECT bytes FROM {segmentsView} WHERE segment_name = :p_table AND segment_type = 'TABLE'";

			using var cmd = connection.CreateCommand();
			cmd.BindByName = true;
			cmd.CommandText = sql;
			if (hasOwner) cmd.Parameters.Add(new OracleParameter("p_owner", owner));
			cmd.Parameters.Add(new OracleParameter("p_table", tableName));

			var result = await cmd.ExecuteScalarAsync(ct);
			return result == null || result == DBNull.Value ? null : Convert.ToInt64(result);
		}
		catch
		{
			return null;
		}
	}

	/// <summary>
	/// Resolves a table name using DBMS_UTILITY.NAME_RESOLVE.
	/// This handles synonyms, public synonyms, and schema-qualified names, returning the actual owner and table name.
	/// </summary>
	public static async Task<(string Schema, string Table)> ResolveTargetTableAsync(OracleConnection connection, string inputName, CancellationToken ct)
	{
		using var cmd = connection.CreateCommand();
		cmd.CommandText = @"
            DECLARE
              v_schema VARCHAR2(30);
              v_part1  VARCHAR2(30);
              v_part2  VARCHAR2(30);
              v_dblink VARCHAR2(30);
              v_part1_type NUMBER;
              v_object_number NUMBER;
            BEGIN
              DBMS_UTILITY.NAME_RESOLVE(
                name => :name,
                context => 2,
                schema => v_schema,
                part1 => v_part1,
                part2 => v_part2,
                dblink => v_dblink,
                part1_type => v_part1_type,
                object_number => v_object_number
              );
              :out_schema := v_schema;
              :out_table := v_part1;
            END;";

		cmd.Parameters.Add(new OracleParameter("name", inputName));

		var pSchema = new OracleParameter("out_schema", OracleDbType.Varchar2, 100) { Direction = ParameterDirection.Output };
		var pTable = new OracleParameter("out_table", OracleDbType.Varchar2, 100) { Direction = ParameterDirection.Output };

		cmd.Parameters.Add(pSchema);
		cmd.Parameters.Add(pTable);

		await cmd.ExecuteNonQueryAsync(ct);

		string schema = pSchema.Value.ToString() ?? "";
		string table = pTable.Value.ToString() ?? "";

		return (schema, table);
	}

	/// <summary>
	/// Quotes an identifier only if necessary (i.e. contains lowercase, spaces, or special characters).
	/// Oracle defaults to UPPERCASE for unquoted identifiers.
	/// </summary>
	public static string GetSmartQuotedIdentifier(string identifier)
	{
		if (string.IsNullOrEmpty(identifier)) return "";

		bool isAllUpper = true;
		bool isValidChars = true;

		if (!char.IsLetter(identifier[0])) isValidChars = false;

		foreach (var c in identifier)
		{
			if (char.IsLower(c)) isAllUpper = false;
			if (!char.IsLetterOrDigit(c) && c != '_' && c != '$' && c != '#') isValidChars = false;
		}

		if (isAllUpper && isValidChars) return identifier;

		return $"\"{identifier}\"";
	}
}

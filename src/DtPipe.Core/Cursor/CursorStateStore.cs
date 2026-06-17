namespace DtPipe.Core.Cursor;

using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// Reads and writes cursor state files (JSON).
/// Thread-safe for sequential calls (not concurrent).
/// </summary>
public static class CursorStateStore
{
    private class StateFileDto
    {
        [JsonPropertyName("version")]
        public int Version { get; set; } = 1;

        [JsonPropertyName("cursor")]
        public CursorDto? Cursor { get; set; }

        [JsonPropertyName("last_run")]
        public LastRunDto? LastRun { get; set; }
    }

    private class CursorDto
    {
        [JsonPropertyName("column")]
        public string Column { get; set; } = "";

        [JsonPropertyName("value")]
        public string Value { get; set; } = "";

        [JsonPropertyName("type")]
        public string Type { get; set; } = "";
    }

    private class LastRunDto
    {
        [JsonPropertyName("started_at")]
        public DateTime StartedAt { get; set; }

        [JsonPropertyName("completed_at")]
        public DateTime CompletedAt { get; set; }

        [JsonPropertyName("rows_transferred")]
        public long RowsTransferred { get; set; }

        [JsonPropertyName("status")]
        public string Status { get; set; } = "";
    }

    private static CursorType ParseType(string typeStr)
    {
        return typeStr?.ToLowerInvariant() switch
        {
            "datetime" => CursorType.DateTime,
            "integer" => CursorType.Integer,
            _ => CursorType.String
        };
    }

    private static string FormatType(CursorType type)
    {
        return type switch
        {
            CursorType.DateTime => "datetime",
            CursorType.Integer => "integer",
            _ => "string"
        };
    }

    /// <summary>Reads the cursor value from a state file, or null if the file does not exist or is invalid.</summary>
    public static CursorValue? Read(string statePath)
    {
        if (string.IsNullOrEmpty(statePath) || !File.Exists(statePath))
        {
            return null;
        }

        try
        {
            var json = File.ReadAllText(statePath);
            var dto = JsonSerializer.Deserialize<StateFileDto>(json);
            if (dto?.Cursor == null)
            {
                return null;
            }

            return new CursorValue(
                dto.Cursor.Column,
                dto.Cursor.Value,
                ParseType(dto.Cursor.Type)
            );
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>Writes the cursor value and run metadata to a state file. Creates or overwrites.</summary>
    public static void Save(string statePath, CursorValue cursor, CursorRunMetadata metadata)
    {
        if (string.IsNullOrEmpty(statePath))
        {
            throw new ArgumentException("State path cannot be null or empty.", nameof(statePath));
        }

        var dir = Path.GetDirectoryName(statePath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }

        var dto = new StateFileDto
        {
            Version = 1,
            Cursor = new CursorDto
            {
                Column = cursor.Column,
                Value = cursor.Value,
                Type = FormatType(cursor.Type)
            },
            LastRun = new LastRunDto
            {
                StartedAt = metadata.StartedAt,
                CompletedAt = metadata.CompletedAt,
                RowsTransferred = metadata.RowsTransferred,
                Status = metadata.Status
            }
        };

        var options = new JsonSerializerOptions
        {
            WriteIndented = true
        };

        var json = JsonSerializer.Serialize(dto, options);
        File.WriteAllText(statePath, json);
    }
}

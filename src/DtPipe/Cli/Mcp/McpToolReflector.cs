using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using DtPipe.Cli.Agent;

namespace DtPipe.Cli.Mcp;

public static class McpToolReflector
{
    private record MethodToolMapping(MethodInfo Method, ParameterInfo[] Parameters);

    /// <summary>
    /// Generates Ollama-compatible tool definitions by reflecting on methods decorated with [McpServerTool]
    /// and parameter [Description] attributes.
    /// </summary>
    public static List<ToolDefinition> BuildToolDefinitions(Type targetType)
    {
        var tools = new List<ToolDefinition>();

        var methods = targetType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() != null);

        foreach (var method in methods)
        {
            var toolAttr = method.GetCustomAttribute<McpServerToolAttribute>()!;
            var toolName = !string.IsNullOrEmpty(toolAttr.Name) ? toolAttr.Name : method.Name.ToLowerInvariant();

            var descAttr = method.GetCustomAttribute<DescriptionAttribute>();
            var description = descAttr?.Description ?? "";

            var parametersObj = new JsonObject
            {
                ["type"] = "object"
            };

            var propertiesObj = new JsonObject();
            var requiredList = new JsonArray();

            foreach (var p in method.GetParameters())
            {
                if (p.ParameterType == typeof(CancellationToken))
                    continue;

                var paramName = p.Name ?? "param";
                var paramDescAttr = p.GetCustomAttribute<DescriptionAttribute>();
                var paramDesc = paramDescAttr?.Description ?? "";

                var paramTypeStr = GetJsonTypeString(p.ParameterType);

                var propSchema = new JsonObject
                {
                    ["type"] = paramTypeStr,
                    ["description"] = paramDesc
                };

                propertiesObj[paramName] = propSchema;

                // Mark required if not nullable / no default value
                bool isOptional = p.IsOptional || Nullable.GetUnderlyingType(p.ParameterType) != null || p.DefaultValue != DBNull.Value;
                if (!isOptional)
                {
                    requiredList.Add(paramName);
                }
            }

            parametersObj["properties"] = propertiesObj;
            if (requiredList.Count > 0)
            {
                parametersObj["required"] = requiredList;
            }

            using var doc = JsonDocument.Parse(parametersObj.ToJsonString());
            tools.Add(new ToolDefinition(toolName, description, doc.RootElement.Clone()));
        }

        return tools;
    }

    /// <summary>The tool name a method is exposed under, or null when it is not a tool.</summary>
    private static string? ToolNameOf(MethodInfo method)
    {
        var attr = method.GetCustomAttribute<McpServerToolAttribute>();
        if (attr is null) return null;
        return !string.IsNullOrEmpty(attr.Name) ? attr.Name : method.Name;
    }

    /// <summary>A tool name reduced to what carries meaning: its letters, in one case.</summary>
    private static string Normalise(string name) =>
        name.Replace("-", string.Empty).Replace("_", string.Empty).ToLowerInvariant();

    /// <summary>
    /// Dynamically invokes an [McpServerTool] method on the target instance by tool name,
    /// deserializing JsonElement arguments to parameter types.
    /// </summary>
    public static async Task<string> InvokeToolAsync(object instance, string toolName, JsonElement args, CancellationToken ct)
    {
        var targetType = instance.GetType();
        var candidates = targetType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Select(m => (Method: m, Name: ToolNameOf(m)))
            .Where(x => x.Name is not null)
            .ToList();

        // A model writes preview_data as readily as preview-data, and the separator carries no
        // meaning — matching on it turns a spelling into a dead end. The same normalisation already
        // binds CLI option keys, and the ask-user terminator already tolerated the underscore on its
        // own; this is that tolerance made general instead of granted to one name.
        var method = candidates
            .FirstOrDefault(x => string.Equals(Normalise(x.Name!), Normalise(toolName), StringComparison.Ordinal))
            .Method;

        if (method == null)
        {
            // Naming what does exist is the difference between one wasted call and several: a model
            // told only that its guess was wrong has nothing to go on but another guess.
            var available = candidates.Select(x => x.Name!).OrderBy(n => n, StringComparer.Ordinal).ToArray();
            return JsonSerializer.Serialize(new
            {
                error = $"Unknown tool '{toolName}'.",
                availableTools = available,
            });
        }

        var parameters = method.GetParameters();
        var paramValues = new object?[parameters.Length];

        for (int i = 0; i < parameters.Length; i++)
        {
            var p = parameters[i];
            if (p.ParameterType == typeof(CancellationToken))
            {
                paramValues[i] = ct;
                continue;
            }

            var paramName = p.Name ?? "";
            JsonElement propElement = default;
            bool found = args.ValueKind == JsonValueKind.Object && (args.TryGetProperty(paramName, out propElement) || args.TryGetProperty(paramName.ToLowerInvariant(), out propElement));

            if (found && propElement.ValueKind != JsonValueKind.Null && propElement.ValueKind != JsonValueKind.Undefined)
            {
                paramValues[i] = ConvertJsonElement(propElement, p.ParameterType);
            }
            else if (p.HasDefaultValue)
            {
                paramValues[i] = p.DefaultValue;
            }
            else if (Nullable.GetUnderlyingType(p.ParameterType) != null || !p.ParameterType.IsValueType)
            {
                paramValues[i] = null;
            }
            else
            {
                return JsonSerializer.Serialize(new { error = $"Missing required parameter '{paramName}' for tool '{toolName}'." });
            }
        }

        try
        {
            var result = method.Invoke(instance, paramValues);
            if (result is Task<string> taskString)
            {
                return await taskString;
            }
            if (result is Task task)
            {
                await task;
                var resultProp = task.GetType().GetProperty("Result");
                return resultProp?.GetValue(task)?.ToString() ?? "{}";
            }
            return result?.ToString() ?? "{}";
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            return JsonSerializer.Serialize(new { error = ex.InnerException.Message });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new { error = ex.Message });
        }
    }

    private static string GetJsonTypeString(Type type)
    {
        var targetType = Nullable.GetUnderlyingType(type) ?? type;
        if (targetType == typeof(int) || targetType == typeof(long) || targetType == typeof(short)) return "integer";
        if (targetType == typeof(double) || targetType == typeof(float) || targetType == typeof(decimal)) return "number";
        if (targetType == typeof(bool)) return "boolean";
        return "string";
    }

    private static object? ConvertJsonElement(JsonElement el, Type targetType)
    {
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlying == typeof(string))
            return el.ValueKind == JsonValueKind.String ? el.GetString() : el.ToString();

        if (underlying == typeof(int))
        {
            if (el.ValueKind == JsonValueKind.Number && el.TryGetInt32(out var val)) return val;
            if (el.ValueKind == JsonValueKind.String && int.TryParse(el.GetString(), out var parsed)) return parsed;
            return 0;
        }

        if (underlying == typeof(bool))
        {
            if (el.ValueKind == JsonValueKind.True) return true;
            if (el.ValueKind == JsonValueKind.False) return false;
            if (el.ValueKind == JsonValueKind.String && bool.TryParse(el.GetString(), out var parsed)) return parsed;
            return false;
        }

        return JsonSerializer.Deserialize(el.GetRawText(), targetType);
    }
}

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Apache.Arrow.Serialization.Dynamic;

namespace Apache.Arrow.Serialization.Accessors;

internal class DynamicStructTypeAccessor : ArrowSerializer.StructTypeAccessorBase
{
    private readonly List<string> _fieldNames;
    public override List<ArrowSerializer.TypeAccessor> ChildAccessors { get; }
    private readonly Dictionary<string, int> _fieldMapping;

    public DynamicStructTypeAccessor(Schema schema)
    {
        _fieldNames = schema.FieldsList.Select(f => f.Name).ToList();
        _fieldMapping = _fieldNames.Select((name, index) => (name, index)).ToDictionary(x => x.name, x => x.index);
        ChildAccessors = schema.FieldsList.Select(f => ArrowSerializer.TypeAccessor.CreateFromArrowType(f.DataType)).ToList();
    }

    public DynamicStructTypeAccessor(StructType structType)
    {
        _fieldNames = structType.Fields.Select(f => f.Name).ToList();
        _fieldMapping = _fieldNames.Select((name, index) => (name, index)).ToDictionary(x => x.name, x => x.index);
        ChildAccessors = structType.Fields.Select(f => ArrowSerializer.TypeAccessor.CreateFromArrowType(f.DataType)).ToList();
    }

    public override IArrowArrayBuilder CreateBuilder(MemoryAllocator? allocator, ArrowSerializer.CapacityInfo? capacity)
    {
        var builders = new List<IArrowArrayBuilder>();
        for (int i = 0; i < ChildAccessors.Count; i++)
        {
            var name = _fieldNames[i];
            ArrowSerializer.CapacityInfo? childCap = null;
            capacity?.Children.TryGetValue(name, out childCap);
            builders.Add(ChildAccessors[i].CreateBuilder(allocator, childCap));
        }
        return new ArrowSerializer.StructArrayManualBuilder(new StructType(_fieldNames.Select((n, i) => new Field(n, ChildAccessors[i].Build(ChildAccessors[i].CreateBuilder(null, null)).Data.DataType, true)).ToList()), ChildAccessors, builders, null!, capacity?.Count ?? 0);
    }

    public override List<IArrowArrayBuilder> CreateChildBuilders(MemoryAllocator? allocator, ArrowSerializer.CapacityInfo? capacity)
    {
        var builders = new List<IArrowArrayBuilder>();
        for (int i = 0; i < ChildAccessors.Count; i++)
        {
            ArrowSerializer.CapacityInfo? childCap = null;
            capacity?.Children.TryGetValue(_fieldNames[i], out childCap);
            builders.Add(ChildAccessors[i].CreateBuilder(allocator, childCap));
        }
        return builders;
    }

    public override void AppendChildren(object value, List<IArrowArrayBuilder> builders)
    {
        IDictionary? dict = value as IDictionary;
        if (dict == null)
        {
            if (value is System.Text.Json.Nodes.JsonObject jo) dict = new JsonObjectWrapper(jo);
            else if (value is IDictionary<string, object?> gd) dict = new DictionaryWrapper(gd);
        }

        if (dict == null) return;

        var processedIndices = new HashSet<int>();

        foreach (DictionaryEntry entry in dict)
        {
            var key = entry.Key.ToString()!;
            if (_fieldMapping.TryGetValue(key, out int index))
            {
                ChildAccessors[index].Append(entry.Value, builders[index]);
                processedIndices.Add(index);
            }
            else
            {
                throw new InvalidOperationException($"Dynamic object contains unknown key '{key}' not present in the first item's schema.");
            }
        }

        for (int i = 0; i < ChildAccessors.Count; i++)
        {
            if (!processedIndices.Contains(i))
            {
                ArrowSerializer.AppendNull(builders[i]);
            }
        }
    }

    public override void Append(object? value, IArrowArrayBuilder builder)
    {
        ((ArrowSerializer.StructArrayManualBuilder)builder).AppendValue(value);
    }

    public override IArrowArray Build(IArrowArrayBuilder builder) => ((ArrowSerializer.StructArrayManualBuilder)builder).Build();
    
    public override void CollectCapacity(object? value, ArrowSerializer.CapacityInfo info)
    {
        if (value == null) return;
        info.Count++;
        IDictionary? dict = value as IDictionary;
        if (dict == null)
        {
            if (value is System.Text.Json.Nodes.JsonObject jo) dict = new JsonObjectWrapper(jo);
            else if (value is IDictionary<string, object?> gd) dict = new DictionaryWrapper(gd);
        }
        
        if (dict == null) return;
        foreach (DictionaryEntry entry in dict)
        {
            var key = entry.Key.ToString()!;
            if (_fieldMapping.TryGetValue(key, out int index))
            {
                var childInfo = ArrowSerializer.StructTypeAccessor.GetOrAddChildInfo(info, key);
                ChildAccessors[index].CollectCapacity(entry.Value, childInfo);
            }
        }
    }
}

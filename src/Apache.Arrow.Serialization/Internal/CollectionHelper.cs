using Apache.Arrow;
using Apache.Arrow.Arrays;
using System;
using System.Collections.Generic;

namespace Apache.Arrow.Serialization.Internal;

public static class CollectionHelper
{
    public static List<T>? BuildList<T>(IArrowArray array, int index, Func<IArrowArray, int, T> elementGetter)
    {
        if (array.IsNull(index)) return null;
        var listArray = (ListArray)array;
        int start = listArray.ValueOffsets[index];
        int count = listArray.ValueOffsets[index + 1] - start;
        var values = listArray.Values;
        
        var list = new List<T>(count);
        for (int i = 0; i < count; i++)
        {
            list.Add(elementGetter(values, start + i));
        }
        return list;
    }

    public static T[]? BuildArray<T>(IArrowArray array, int index, Func<IArrowArray, int, T> elementGetter)
    {
        if (array.IsNull(index)) return null;
        var listArray = (ListArray)array;
        int start = listArray.ValueOffsets[index];
        int count = listArray.ValueOffsets[index + 1] - start;
        var values = listArray.Values;
        
        var res = new T[count];
        for (int i = 0; i < count; i++)
        {
            res[i] = elementGetter(values, start + i);
        }
        return res;
    }

    public static HashSet<T>? BuildHashSet<T>(IArrowArray array, int index, Func<IArrowArray, int, T> elementGetter)
    {
        if (array.IsNull(index)) return null;
        var listArray = (ListArray)array;
        int start = listArray.ValueOffsets[index];
        int count = listArray.ValueOffsets[index + 1] - start;
        var values = listArray.Values;
        
        var set = new HashSet<T>(count);
        for (int i = 0; i < count; i++)
        {
            set.Add(elementGetter(values, start + i));
        }
        return set;
    }

    public static Dictionary<K, V>? BuildMap<K, V>(IArrowArray array, int index, Func<IArrowArray, int, K> keyGetter, Func<IArrowArray, int, V> valueGetter) where K : notnull
    {
        if (array.IsNull(index)) return null;
        var mapArray = (MapArray)array;
        int start = mapArray.ValueOffsets[index];
        int count = mapArray.ValueOffsets[index + 1] - start;
        
        var dict = new Dictionary<K, V>(count);
        var keys = mapArray.Keys;
        var values = mapArray.Values;
        
        for (int i = 0; i < count; i++)
        {
            var k = keyGetter(keys, start + i);
            var v = valueGetter(values, start + i);
            if (k != null) dict[k] = v;
        }
        return dict;
    }
}

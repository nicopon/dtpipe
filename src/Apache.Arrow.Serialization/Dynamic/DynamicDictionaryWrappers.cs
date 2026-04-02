using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow.Serialization.Dynamic;

internal class DictionaryWrapper : IDictionary
{
    private readonly IDictionary<string, object?> _inner;
    public DictionaryWrapper(IDictionary<string, object?> inner) => _inner = inner;

    public object? this[object key] { get => _inner[key.ToString()!]; set => throw new NotSupportedException(); }
    public bool IsFixedSize => true;
    public bool IsReadOnly => true;
    public ICollection Keys => (ICollection)_inner.Keys;
    public ICollection Values => (ICollection)_inner.Values;
    public int Count => _inner.Count;
    public bool IsSynchronized => false;
    public object SyncRoot => this;
    public void Add(object key, object? value) => throw new NotSupportedException();
    public void Clear() => throw new NotSupportedException();
    public bool Contains(object key) => _inner.ContainsKey(key.ToString()!);
    public IDictionaryEnumerator GetEnumerator() => new DictionaryWrapperEnumerator(_inner.GetEnumerator());
    public void Remove(object key) => throw new NotSupportedException();
    void ICollection.CopyTo(System.Array array, int index)
    {
        int i = 0;
        foreach (var kvp in _inner)
        {
            array.SetValue(new DictionaryEntry(kvp.Key, kvp.Value), index + i++);
        }
    }
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private class DictionaryWrapperEnumerator : IDictionaryEnumerator
    {
        private readonly IEnumerator<KeyValuePair<string, object?>> _inner;
        public DictionaryWrapperEnumerator(IEnumerator<KeyValuePair<string, object?>> inner) => _inner = inner;
        public object Key => _inner.Current.Key;
        public object? Value => _inner.Current.Value;
        public DictionaryEntry Entry => new DictionaryEntry(Key, Value);
        public object Current => Entry;
        public bool MoveNext() => _inner.MoveNext();
        public void Reset() => _inner.Reset();
    }
}

internal class JsonObjectWrapper : IDictionary
{
    private readonly System.Text.Json.Nodes.JsonObject _json;
    public JsonObjectWrapper(System.Text.Json.Nodes.JsonObject json) => _json = json;

    public object? this[object key] { get => _json[key.ToString()!]; set => throw new NotSupportedException(); }
    public bool IsFixedSize => true;
    public bool IsReadOnly => true;
    public ICollection Keys => _json.Select(kvp => kvp.Key).ToList();
    public ICollection Values => _json.Select(kvp => kvp.Value).ToList();
    public int Count => _json.Count;
    public bool IsSynchronized => false;
    public object SyncRoot => this;
    public void Add(object key, object? value) => throw new NotSupportedException();
    public void Clear() => throw new NotSupportedException();
    public bool Contains(object key) => _json.ContainsKey(key.ToString()!);
    public IDictionaryEnumerator GetEnumerator() => new JsonObjectEnumerator(_json.GetEnumerator(), this);
    public void Remove(object key) => throw new NotSupportedException();
    void ICollection.CopyTo(System.Array array, int index)
    {
        var pairs = _json.ToList();
        for (int i = 0; i < pairs.Count; i++)
        {
            array.SetValue(new DictionaryEntry(pairs[i].Key, Unwrap(pairs[i].Value)), index + i);
        }
    }
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private static object? Unwrap(System.Text.Json.Nodes.JsonNode? node)
    {
        if (node == null) return null;
        if (node is System.Text.Json.Nodes.JsonValue val)
        {
            if (val.TryGetValue<string>(out var s)) return s;
            if (val.TryGetValue<int>(out var i)) return i;
            if (val.TryGetValue<long>(out var l)) return l;
            if (val.TryGetValue<double>(out var d)) return d;
            if (val.TryGetValue<bool>(out var b)) return b;
            if (val.TryGetValue<decimal>(out var dec)) return dec;
        }
        return node;
    }

    private class JsonObjectEnumerator : IDictionaryEnumerator
    {
        private readonly IEnumerator<KeyValuePair<string, System.Text.Json.Nodes.JsonNode?>> _inner;
        private readonly JsonObjectWrapper _parent;
        public JsonObjectEnumerator(IEnumerator<KeyValuePair<string, System.Text.Json.Nodes.JsonNode?>> inner, JsonObjectWrapper parent) { _inner = inner; _parent = parent; }
        public object Key => _inner.Current.Key;
        public object? Value => Unwrap(_inner.Current.Value);
        public DictionaryEntry Entry => new DictionaryEntry(Key, Value);
        public object Current => Entry;
        public bool MoveNext() => _inner.MoveNext();
        public void Reset() => _inner.Reset();
    }
}

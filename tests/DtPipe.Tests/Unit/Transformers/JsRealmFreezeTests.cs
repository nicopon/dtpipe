using System.Dynamic;
using Apache.Arrow;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Row.Window;
using DtPipe.Transformers.Services;
using Jint;
using Jint.Native;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// What a script can keep from one call to the next: its engine's realm is frozen when the engine is
/// made (<see cref="JsRealmFreeze"/>), and each transformer hands it one view it moves from row to row.
/// Each channel, and what an attempt through it gives on three rows (<c>A = 1, 2, 4</c>). A channel the
/// freeze leaves open is stated as one that persists.
/// </summary>
public class JsRealmFreezeTests : IDisposable
{
    private const string ExtensionsRefused = "*Cannot prevent extensions of a row (Object.preventExtensions, seal or freeze)*";

    private static readonly IReadOnlyList<PipeColumnInfo> Columns = [new("A", typeof(double), false)];
    private static readonly double[] Values = [1, 2, 4];
    private static readonly string[] Kinds = ["compute", "filter", "expand", "window"];

    private readonly JsEngineProvider _js = new();

    public void Dispose() => _js.Dispose();

    [Fact]
    public void Every_Engine_The_Provider_Makes_Has_A_Frozen_Realm()
    {
        _js.GetEngine().Evaluate("Object.isFrozen(globalThis) && Object.isFrozen(Object.prototype)").AsBoolean().Should().BeTrue();

        var onAnotherThread = false;
        var thread = new Thread(() => onAnotherThread = _js.GetEngine().Evaluate("Object.isFrozen(globalThis)").AsBoolean());
        thread.Start();
        thread.Join();
        onAnotherThread.Should().BeTrue();
    }

    // ── The realm, walked ────────────────────────────────────────────────────

    /// <summary>
    /// Lists every place reachable from <c>globalThis</c>, or only through syntax, that a script could
    /// still change: an extensible object, a writable or configurable property, an accessor with a setter.
    /// </summary>
    private const string AuditScript = """
        (function () {
            'use strict';
            const roots = [
                ['globalThis', globalThis],
                ['%GeneratorFunction.prototype%', Object.getPrototypeOf(function* () {})],
                ['%AsyncFunction.prototype%', Object.getPrototypeOf(async function () {})],
                ['%AsyncGeneratorFunction.prototype%', Object.getPrototypeOf(async function* () {})],
                ['%GeneratorPrototype%', Object.getPrototypeOf(Object.getPrototypeOf((function* () {})()))],
                ['%AsyncGeneratorPrototype%', Object.getPrototypeOf(Object.getPrototypeOf((async function* () {})()))],
                ['%ArrayIteratorPrototype%', Object.getPrototypeOf([][Symbol.iterator]())],
                ['%StringIteratorPrototype%', Object.getPrototypeOf(''[Symbol.iterator]())],
                ['%MapIteratorPrototype%', Object.getPrototypeOf(new Map()[Symbol.iterator]())],
                ['%SetIteratorPrototype%', Object.getPrototypeOf(new Set()[Symbol.iterator]())],
                ['%RegExpStringIteratorPrototype%', Object.getPrototypeOf(/a/[Symbol.matchAll]('a'))],
                ['%IteratorHelperPrototype%', Object.getPrototypeOf([].values().map(x => x))],
                ['%WrapForValidIteratorPrototype%', Object.getPrototypeOf(Iterator.from({ next() { return { done: true }; } }))],
                ['%ThrowTypeError%', Object.getOwnPropertyDescriptor(arguments, 'callee').get],
            ];
            const seen = new Set();
            const out = [];
            const queue = roots.slice();
            for (let i = 0; i < queue.length; i++) {
                const [path, o] = queue[i];
                if (o === null || (typeof o !== 'object' && typeof o !== 'function') || seen.has(o)) continue;
                seen.add(o);
                if (Object.isExtensible(o)) out.push(path + ' : extensible');
                for (const key of Reflect.ownKeys(o)) {
                    const d = Reflect.getOwnPropertyDescriptor(o, key);
                    const name = typeof key === 'symbol' ? '[' + String(key) + ']' : key;
                    if (d.configurable) out.push(path + '.' + name + ' : configurable');
                    if ('value' in d) {
                        if (d.writable) out.push(path + '.' + name + ' : writable');
                        queue.push([path + '.' + name, d.value]);
                    } else {
                        if (d.set) out.push(path + '.' + name + ' : setter');
                        queue.push([path + '.' + name + '<get>', d.get]);
                        queue.push([path + '.' + name + '<set>', d.set]);
                    }
                }
                queue.push([path + '.[[Prototype]]', Reflect.getPrototypeOf(o)]);
            }
            return out.join('\n');
        })()
        """;

    /// <summary>
    /// Nothing stays extensible, writable or configurable. Eight setters stay, none of which stores on the
    /// realm: <c>__proto__</c> sets its receiver's prototype; <c>arguments</c> and <c>caller</c> are
    /// <c>%ThrowTypeError%</c>; the iterator prototypes' <c>constructor</c> and <c>@@toStringTag</c>, and
    /// <c>Error.prototype.stack</c>, refuse the prototype itself and write onto an object that inherits
    /// from it (the channel table plays <c>__proto__</c>, <c>Iterator.prototype.constructor</c> and
    /// <c>Error.prototype.stack</c>). Getters are not listed: each reads its receiver or returns it, and
    /// the <c>RegExp</c> statics, which read state of the engine's own, are removed.
    /// </summary>
    [Fact]
    public void Nothing_Reachable_In_The_Realm_Stays_Changeable_But_Eight_Setters_That_Store_Nothing_There()
    {
        var found = _js.GetEngine().Evaluate(AuditScript).AsString().Split('\n', StringSplitOptions.RemoveEmptyEntries);

        found.Should().BeEquivalentTo(
            "globalThis.[[Prototype]].__proto__ : setter",
            "%GeneratorFunction.prototype%.[[Prototype]].arguments : setter",
            "%GeneratorFunction.prototype%.[[Prototype]].caller : setter",
            "%GeneratorPrototype%.[[Prototype]].constructor : setter",
            "%GeneratorPrototype%.[[Prototype]].[Symbol(Symbol.toStringTag)] : setter",
            "%AsyncGeneratorPrototype%.[[Prototype]].constructor : setter",
            "%AsyncGeneratorPrototype%.[[Prototype]].[Symbol(Symbol.toStringTag)] : setter",
            "globalThis.Error.prototype.stack : setter");
    }

    // ── (i) to (iv): the realm, through a script ─────────────────────────────

    /// <summary>
    /// A <c>--compute</c> script played on three rows of one engine: <c>throws: …</c> when every call
    /// fails with that message, otherwise the three results, the second and third showing what an earlier
    /// call left.
    /// </summary>
    public static TheoryData<string, string> Channels => new()
    {
        // (i) The global object.
        { "const p = globalThis.__p; globalThis.__p = row.A; return String(p);", "throws: Cannot assign to read only property '__p' of Object" },
        { "globalThis.n = (globalThis.n || 0) + 1; return String(globalThis.n);", "throws: Cannot assign to read only property 'n' of Object" },
        { "n = 1; return 'x';", "throws: n is not defined" },
        { "return Reflect.set(globalThis, 'k', row.A) + ',' + globalThis.k;", "false,undefined|false,undefined|false,undefined" },
        { "Object.defineProperty(globalThis, 'k', { value: row.A, writable: true, configurable: true }); return 'x';", "throws: Cannot redefine property: k" },
        { "Object.assign(globalThis, { k: row.A }); return 'x';", "throws: Cannot assign to read only property 'k' of object '#<Object>'" },
        { "delete globalThis.Math; return 'x';", "throws: Cannot delete property 'Math' of Object" },
        { "Object.setPrototypeOf(globalThis, { k: row.A }); return 'x';", "throws: #<Object> is not extensible" },
        { "return String(Reflect.setPrototypeOf(globalThis, { k: row.A }));", "false|false|false" },
        // (ii) Prototypes, constructors and namespaces.
        { "Object.prototype.__p = row.A; return 'x';", "throws: Cannot assign to read only property '__p' of Object" },
        { "Array.prototype.__p = row.A; return 'x';", "throws: Cannot assign to read only property '__p' of Object" },
        { "String.prototype.__p = row.A; return 'x';", "throws: Cannot assign to read only property '__p' of Object" },
        { "Math.__p = row.A; return 'x';", "throws: Cannot assign to read only property '__p' of Object" },
        { "JSON.__p = row.A; return 'x';", "throws: Cannot assign to read only property '__p' of Object" },
        { "Math.max = () => row.A; return 'x';", "throws: Cannot assign to read only property 'max' of Object" },
        { "Object.defineProperty = function () {}; return 'x';", "throws: Cannot assign to read only property 'defineProperty' of Object" },
        { "Object.setPrototypeOf = function () {}; return 'x';", "throws: Cannot assign to read only property 'setPrototypeOf' of Object" },
        { "Object.defineProperty(Object.prototype, 'k', { value: row.A, configurable: true }); return 'x';", "throws: Cannot redefine property: k" },
        { "Object.setPrototypeOf(Array.prototype, { k: row.A }); return 'x';", "throws: #<Object> is not extensible" },
        { "Object.getPrototypeOf([][Symbol.iterator]()).__p = row.A; return 'x';", "throws: Cannot assign to read only property '__p' of Object" },
        { "Object.getPrototypeOf(function* () {}).__p = row.A; return 'x';", "throws: Cannot assign to read only property '__p' of Object" },
        { "Iterator.prototype.constructor = row.A; return 'x';", "throws: Cannot set property on Iterator prototype directly" },
        { "const o = Object.create(Iterator.prototype); o.constructor = row.A; return String(o.constructor);", "1|2|4" },
        { "Object.getPrototypeOf([1].values().map(x => x)).__p = row.A; return 'x';", "throws: Cannot assign to read only property '__p' of Object" },
{ "Object.getPrototypeOf(Iterator.from({ next() { return { done: true }; } })).__p = row.A; return 'x';", "throws: Cannot assign to read only property '__p' of Object" },
{ "Error.prototype.stack = String(row.A); return 'x';", "throws: Cannot set the \"stack\" property directly on %Error.prototype%" },
        { "Error.stackTraceLimit = row.A; return 'x';", "throws: Cannot assign to read only property 'stackTraceLimit' of Object" },
        { "Object.prototype.__proto__ = { k: row.A }; return 'x';", "throws: Invalid prototype" },
        // (iii) Declarations by indirect eval, direct eval and Function: strict code, so each is local or refused.
        { "(0, eval)('var __v = ' + row.A); return 'x';", "throws: Cannot define global variable '__v'" },
        { "(0, eval)('function __f() {}'); return 'x';", "throws: Cannot define global function '__f'" },
        { "const seen = (0, eval)('typeof __l'); (0, eval)('let __l = ' + row.A); return seen;", "undefined|undefined|undefined" },
        { "const seen = typeof __w; eval('var __w = ' + row.A); return seen;", "undefined|undefined|undefined" },
        { "Function('globalThis.k = ' + row.A)(); return 'x';", "throws: Cannot assign to read only property 'k' of Object" },
        { "return String(Function('return this')());", "undefined|undefined|undefined" },
        // (iv) Registries and state the engine keeps outside any property.
        { "const p = RegExp.$1; /(\\d+)/.exec(String(row.A)); return String(p);", "undefined|undefined|undefined" },
        { "const p = RegExp.lastMatch; /\\d+/.exec(String(row.A)); return String(p);", "undefined|undefined|undefined" },
        { "RegExp.input = String(row.A); return 'x';", "throws: Cannot assign to read only property 'input' of Object" },
        { "return Symbol.keyFor(Symbol.for('k' + row.A));", "k1|k2|k4" },
    };

    [Theory]
    [MemberData(nameof(Channels))]
    public async Task A_Channel_Through_The_Realm_Is_Closed(string body, string expected)
    {
        var compute = await ComputeAsync(body);

        string.Join("|", PlayRows(compute)).Should().Be(expected.StartsWith("throws: ") ? string.Join("|", Enumerable.Repeat(expected, 3)) : expected);
    }

    /// <summary>
    /// A binding declared at the top level of a script the engine evaluates lives in the global lexical
    /// environment, which freezing the global object does not reach. A script reaches it by closing the
    /// function a transformer wraps it in: the engine evaluates that text whole.
    /// </summary>
    [Fact]
    public async Task Persists_A_Top_Level_Binding_Declared_By_Closing_The_Wrapper()
    {
        var compute = await ComputeAsync("0); }); let __n = 0; (function (row) { return (String(++__n)");

        PlayRows(compute).Should().Equal("1", "2", "3");
    }

    /// <summary>
    /// A promise reaction a call queues does not run when the call returns: the engine keeps it, and what
    /// it closes over, until its next evaluation, which runs it.
    /// </summary>
    [Fact]
    public void Persists_A_Promise_Reaction_Until_The_Engine_Evaluates_Again()
    {
        var engine = _js.GetEngine();
        var call = engine.Evaluate("(function (box) { Promise.resolve().then(() => { box.ran = true; }); return 0; })");
        var box = new JsObject(engine);

        engine.Call(call, JsValue.Undefined, [box]);
        box.Get("ran").IsUndefined().Should().BeTrue();

        engine.Evaluate("0");
        box.Get("ran").AsBoolean().Should().BeTrue();
    }

    // ── (v) The objects the transformer hands the script ─────────────────────

    /// <summary>
    /// A change to the row, looked for on the next row: <c>probe</c> reads what the row before left,
    /// <c>change</c> leaves that row's <c>A</c> on the view. The view is one object from row to row: a
    /// write under a name or a number is cleared when it moves, one under a symbol or by
    /// <c>Object.defineProperty</c> is never read back, and its prototype is <c>Object.prototype</c> again.
    /// </summary>
    public static TheoryData<string, string, string> ViewChanges()
    {
        (string Probe, string Change)[] changes =
        [
            ("'k' in row ? row.k : undefined", "row.k = row.A"),
            ("0 in row ? row[0] : undefined", "row[0] = row.A"),
            ("row[Symbol.for('k')]", "row[Symbol.for('k')] = row.A"),
            ("(Object.getOwnPropertyDescriptor(row, 'k') || {}).value", "Object.defineProperty(row, 'k', { value: row.A, configurable: true })"),
            ("Object.getPrototypeOf(row).prevA", "Object.setPrototypeOf(row, { prevA: row.A })"),
            ("Reflect.getPrototypeOf(row).prevA", "Reflect.setPrototypeOf(row, { prevA: row.A })"),
        ];
        var data = new TheoryData<string, string, string>();
        foreach (var kind in Kinds)
        {
            foreach (var (probe, change) in changes)
                data.Add(kind, probe, change);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(ViewChanges))]
    public async Task A_Change_To_The_Row_Does_Not_Reach_The_Next_Row_On_Both_Paths(string kind, string probe, string change)
    {
        var script = ViewScript(kind, probe, change);
        object?[] expected = kind == "filter" ? [1d, 2d, 4d] : [-1d, -1d, -1d];

        RunRows(await CreateAsync(kind, script), Values).Should().Equal(expected);
        (await RunBatchesAsync(await CreateAsync(kind, script), column: 0)).Should().Equal(expected);
    }

    public static TheoryData<string, string> ExtensionChanges()
    {
        var data = new TheoryData<string, string>();
        foreach (var kind in Kinds)
        {
            foreach (var call in new[] { "Object.preventExtensions(row)", "Reflect.preventExtensions(row)", "Object.seal(row)", "Object.freeze(row)" })
                data.Add(kind, call);
        }
        return data;
    }

    /// <summary>
    /// Extensibility taken away would stay on the view for the next row, so both views refuse it loudly,
    /// <c>Reflect.preventExtensions</c> included, rather than answering false.
    /// </summary>
    [Theory]
    [MemberData(nameof(ExtensionChanges))]
    public async Task Taking_Away_A_Rows_Extensibility_Fails_Naming_The_Operation_On_Both_Paths(string kind, string call)
    {
        var script = ViewScript(kind, "undefined", call);

        var onRows = async () => RunRows(await CreateAsync(kind, script), Values);
        (await onRows.Should().ThrowAsync<InvalidOperationException>()).WithMessage(ExtensionsRefused);

        var onBatches = async () => await RunBatchesAsync(await CreateAsync(kind, script), column: 0);
        (await onBatches.Should().ThrowAsync<InvalidOperationException>()).WithMessage(ExtensionsRefused);
    }

    /// <summary>
    /// A prototype set on the row is not refused: the script reads through it for that row. The next row
    /// reads whole again, a null prototype set before it notwithstanding: its keys, a spread, <c>in</c>,
    /// an inherited method, its extensibility.
    /// </summary>
    [Fact]
    public async Task A_Prototype_Set_On_A_Row_Serves_That_Row_Only_On_Both_Paths()
    {
        const string body = "const seen = [Object.keys(row).join(','), Object.entries({ ...row }).join(','), 'A' in row, "
            + "row.hasOwnProperty('A'), Object.isExtensible(row)].join(';'); "
            + "Object.setPrototypeOf(row, { tag: 't' + row.A }); const tag = row.tag; Object.setPrototypeOf(row, null); "
            + "return seen + ';' + tag;";
        string[] expected = ["A;A,1;true;true;true;t1", "A;A,2;true;true;true;t2", "A;A,4;true;true;true;t4"];

        PlayRows(await ComputeAsync(body)).Should().Equal(expected);
        (await RunBatchesAsync(await ComputeAsync(body), column: 1)).Should().Equal(expected);
    }

    /// <summary>
    /// Each window gets an array of its own over the views the transformer reuses: a property set on it,
    /// or the order a sort in place leaves, does not reach the next window, and the sort itself stays the
    /// script's business.
    /// </summary>
    [Fact]
    public async Task A_Window_Gets_An_Array_Of_Its_Own_On_Both_Paths()
    {
        const string keeps = "const p = rows.k; rows.k = rows[0].A; return [{ A: p === undefined ? -1 : p }];";
        RunRows(await WindowAsync(keeps), Values).Should().Equal(-1d, -1d, -1d);
        (await RunBatchesAsync(await WindowAsync(keeps), column: 0)).Should().Equal(-1d, -1d, -1d);

        const string reorders = "const seen = rows.map(r => ({ A: r.A })); rows.sort((a, b) => b.A - a.A); return seen;";
        RunRows(await WindowAsync(reorders, count: 2), [1, 2, 4, 8]).Should().Equal(1d, 2d, 4d, 8d);
        (await RunBatchesAsync(await WindowAsync(reorders, count: 2), column: 0, [1, 2, 4, 8])).Should().Equal(1d, 2d, 4d, 8d);

        const string sorts = "rows.sort((a, b) => b.A - a.A); return rows;";
        RunRows(await WindowAsync(sorts, count: 2), [1, 2, 4, 8]).Should().Equal(2d, 1d, 8d, 4d);
        (await RunBatchesAsync(await WindowAsync(sorts, count: 2), column: 0, [1, 2, 4, 8])).Should().Equal(2d, 1d, 8d, 4d);
    }

    // ── (vi) A result that keeps the row ─────────────────────────────────────

    /// <summary>
    /// A result that reads the row later (a getter) is read while the view is still on its row, for each
    /// transformer and each path: no result is read once the transformer has moved.
    /// </summary>
    [Fact]
    public async Task A_Result_Keeping_The_Row_Is_Read_Before_The_Cursor_Moves()
    {
        // Compute, row path: the result becomes a .NET object at the call, its getter read then.
        var computeRows = await ComputeAsync("return { get v() { return row.A; } };");
        Values.Select(a => ((IDictionary<string, object?>)(ExpandoObject)computeRows.Transform([a])![1]!)["v"]).Should().Equal(1d, 2d, 4d);

        // Compute, Arrow path: the cell is converted at the call; an object becomes the .NET type name.
        var computeBatches = await ComputeAsync("return { get v() { return row.A; } };");
        (await RunBatchesAsync(computeBatches, column: 1)).Should().Equal(Enumerable.Repeat<object?>("System.Dynamic.ExpandoObject", 3));

        // Filter: only a boolean result is a match, read at the call.
        var filter = await FilterAsync("return { valueOf() { return row.A > 1; } };");
        Values.Where(a => filter.Transform([a]) is not null).Should().BeEmpty();
        (await RunBatchesAsync(filter, column: 0)).Should().BeEmpty();

        // Expand and window: the transformer reads each element's columns before the next call.
        const string expandScript = "return [{ get A() { return row.A; } }];";
        var expandRows = await ExpandAsync(expandScript);
        Values.SelectMany(a => expandRows.TransformMany([a])).Select(r => r[0]).Should().Equal(1d, 2d, 4d);
        var expandBatches = await ExpandAsync(expandScript);
        (await RunBatchesAsync(expandBatches, column: 0)).Should().Equal(1d, 2d, 4d);

        const string windowScript = "return rows.map(r => ({ get A() { return r.A; } }));";
        var windowRows = await WindowAsync(windowScript);
        Values.SelectMany(a => windowRows.TransformMany([a])).Select(r => r[0]).Should().Equal(1d, 2d, 4d);
        var windowBatches = await WindowAsync(windowScript);
        (await RunBatchesAsync(windowBatches, column: 0)).Should().Equal(1d, 2d, 4d);
    }

    // ── What a script that keeps nothing loses ───────────────────────────────

    /// <summary>
    /// A write to an object of the script's own fails when the property it names is inherited from a frozen
    /// prototype and read-only there; an object literal, a class or <c>Object.defineProperty</c> defines it.
    /// </summary>
    public static TheoryData<string, string> OwnObjects => new()
    {
        { "const o = {}; o.toString = () => 'x' + row.A; return String(o);", "throws: Cannot assign to read only property 'toString' of Object" },
        { "const e = new Error('m'); e.name = 'Custom'; return e.name;", "throws: Cannot assign to read only property 'name' of Object" },
        { "const o = { toString: () => 'x' + row.A }; return String(o);", "x1|x2|x4" },
        { "class Money { constructor(v) { this.v = v; } toString() { return '$' + this.v; } } return String(new Money(row.A));", "$1|$2|$4" },
        { "const o = {}; Object.defineProperty(o, 'toString', { value: () => 'x' + row.A }); return String(o);", "x1|x2|x4" },
        { "return [row.A, 2].map(x => x * 2).join(',') + Math.max(row.A, 3) + JSON.stringify({ a: row.A });", "2,43{\"a\":1}|4,43{\"a\":2}|8,44{\"a\":4}" },
    };

    [Theory]
    [MemberData(nameof(OwnObjects))]
    public async Task A_Script_Writing_Only_Its_Own_Objects(string body, string expected)
    {
        var compute = await ComputeAsync(body);

        string.Join("|", PlayRows(compute)).Should().Be(expected.StartsWith("throws: ") ? string.Join("|", Enumerable.Repeat(expected, 3)) : expected);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// <paramref name="probe"/> and <paramref name="change"/> as a script of <paramref name="kind"/> whose
    /// column <c>A</c> is <c>-1</c> when the probe finds nothing; a filter keeps the row then.
    /// </summary>
    private static string ViewScript(string kind, string probe, string change) => kind switch
    {
        "compute" => $"A:const p = {probe}; {change}; return p === undefined ? -1 : p;",
        "filter" => $"const p = {probe}; {change}; return p === undefined;",
        "expand" => $"const p = {probe}; {change}; return [{{ A: p === undefined ? -1 : p }}];",
        "window" => $"const row = rows[0]; const p = {probe}; {change}; return [{{ A: p === undefined ? -1 : p }}];",
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    private async Task<BaseColumnarTransformer> CreateAsync(string kind, string script) => kind switch
    {
        "compute" => await InitializeAsync(new ComputeDataTransformer(new ComputeOptions { Compute = [script] }, _js)),
        "filter" => await FilterAsync(script),
        "expand" => await ExpandAsync(script),
        "window" => await WindowAsync(script),
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    private async Task<ComputeDataTransformer> ComputeAsync(string body)
        => await InitializeAsync(new ComputeDataTransformer(new ComputeOptions { Compute = [$"D:{body}"] }, _js));

    private async Task<FilterDataTransformer> FilterAsync(string script)
        => await InitializeAsync(new FilterDataTransformer(new FilterOptions { Filters = [script] }, _js));

    private async Task<ExpandDataTransformer> ExpandAsync(string script)
        => await InitializeAsync(new ExpandDataTransformer(new ExpandOptions { Expand = [script] }, _js));

    private async Task<WindowDataTransformer> WindowAsync(string script, int count = 1)
        => await InitializeAsync(new WindowDataTransformer(new WindowOptions { Count = count, Script = script }, _js));

    private static async Task<T> InitializeAsync<T>(T transformer) where T : BaseColumnarTransformer
    {
        await transformer.InitializeAsync(Columns);
        return transformer;
    }

    /// <summary>The compute column of each row on the row path, or <c>throws: </c> and the script's message.</summary>
    private static List<string?> PlayRows(ComputeDataTransformer compute)
    {
        var results = new List<string?>();
        foreach (var a in Values)
        {
            try
            {
                results.Add(compute.Transform([a])![1]?.ToString());
            }
            catch (InvalidOperationException ex)
            {
                results.Add("throws: " + ex.InnerException!.Message);
            }
        }
        return results;
    }

    /// <summary>Runs <paramref name="values"/> through the row path, its flush included, and returns column 0 of every row it emits.</summary>
    private static List<object?> RunRows(BaseColumnarTransformer transformer, double[] values)
    {
        var output = new List<object?>();
        foreach (var a in values)
        {
            if (transformer is IMultiRowTransformer multi)
                output.AddRange(multi.TransformMany([a]).Select(r => r[0]));
            else if (transformer.Transform([a]) is { } row)
                output.Add(row[0]);
        }
        output.AddRange(transformer.Flush().Select(r => r[0]));
        return output;
    }

    /// <summary>
    /// Runs <paramref name="transformer"/> over <paramref name="batches"/> (by default <c>1, 2</c> then
    /// <c>4</c>), disposing each input once the transformer has run, and returns <paramref name="column"/>
    /// of every row it emits.
    /// </summary>
    private static async Task<List<object?>> RunBatchesAsync(BaseColumnarTransformer transformer, int column, params double[][] batches)
    {
        var output = new List<object?>();
        foreach (var values in batches.Length > 0 ? batches : [[1, 2], [4]])
        {
            var builder = new DoubleArray.Builder();
            builder.AppendRange(values);
            var input = new RecordBatch(new Schema([new Field("A", DoubleType.Default, false)], null), [builder.Build()], values.Length);
            var result = await transformer.TransformBatchAsync(input);
            output.AddRange(Read(result, column));
            if (!ReferenceEquals(result, input))
                result?.Dispose();
            input.Dispose();
        }

        await foreach (var flushed in transformer.FlushBatchAsync())
        {
            using (flushed)
                output.AddRange(Read(flushed, column));
        }
        return output;
    }

    private static IEnumerable<object?> Read(RecordBatch? batch, int column)
    {
        if (batch is null)
            return [];
        var array = batch.Column(column);
        var field = batch.Schema.GetFieldByIndex(column);
        return Enumerable.Range(0, batch.Length).Select(i => ArrowTypeMapper.GetValueForField(array, field, i)).ToList();
    }
}

using Acornima;
using Acornima.Ast;

namespace DtPipe.Transformers.Services;

/// <summary>What a script's one parameter receives.</summary>
internal enum JsRowArgument
{
    /// <summary>A view of one row: <c>--compute</c>, <c>--filter</c>, <c>--expand</c>.</summary>
    Row,

    /// <summary>An array of row views: <c>--window-script</c>.</summary>
    Rows,
}

/// <summary>
/// Whether a transformer may hand a script the view it reuses from one call to the next: the cursor over
/// a batch or a row, a window's pool of views and the array that lists them.
/// </summary>
/// <remarks>
/// <para>
/// It may when the script keeps nothing of its argument once the call returns: no row view, and nothing
/// that holds or captures one, reaches a global, an object the script did not make, a function it did not
/// define, or its result as a function; and a window's array comes back as it went in. Otherwise a row the
/// script kept would read the row the cursor has moved to, and a window's array would carry one call's
/// changes into the next. Such a script receives a view of its own per row instead.
/// </para>
/// <para>
/// The answer is yes only when the analysis proves it: a script it cannot parse, and any form it does not
/// model (a class, a generator, an async function, <c>eval</c>), answers no. The proof is flow-insensitive:
/// every binding carries all it may ever hold, to a fixed point, and one summary stands for everything the
/// script writes into the objects it made. It trusts the engine's built-ins to do what the standard says,
/// and it reads one script: a function another script installs over a built-in is outside it.
/// </para>
/// </remarks>
internal static class JsRowRetention
{
    public static bool CanShareView(string wrappedScript, JsRowArgument argument)
    {
        try
        {
            return new Parser().ParseExpression($"({wrappedScript})") is FunctionExpression { Params.Count: 1 } function
                && function.Params[0] is Identifier
                && new Proof(function, argument == JsRowArgument.Row ? Value.Row : Value.Rows).Holds();
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>What an expression's value may be, or may hold.</summary>
    [Flags]
    private enum Value : byte
    {
        /// <summary>A primitive.</summary>
        None = 0,

        /// <summary>A row view the transformer hands over.</summary>
        Row = 1,

        /// <summary>The array of a window's row views.</summary>
        Rows = 2,

        /// <summary>An object or a closure that holds or captures a row view or the window's array.</summary>
        Holder = 4,

        /// <summary>An object the script did not make: a global, a built-in, what an unknown function returns.</summary>
        Foreign = 8,

        /// <summary>A value read off a row: it belongs to the row and outlives the call on the row path.</summary>
        Cell = 16,

        /// <summary>A function the script defined, or an object that may hold one.</summary>
        Function = 32,

        /// <summary>An object the script made.</summary>
        Local = 64,

        Any = Row | Rows | Holder | Foreign | Cell | Function | Local,
    }

    private const Value Kept = Value.Row | Value.Rows | Value.Holder;

    private enum Effect { None, Stores, Reorders }

    private enum Shape { Primitive, Element, Receiver, Copy, Mapped, Reduced }

    private enum Callback { None, Iterate, Reduce, Compare, Text }

    private readonly record struct Method(Shape Result, Effect Effect = Effect.None, Callback Callback = Callback.None, int CallbackIndex = 0);

    /// <summary>The built-in methods whose behaviour the proof relies on, by name.</summary>
    private static readonly Dictionary<string, Method> Methods = BuildMethods();

    /// <summary>Names a read finds on a prototype: a shared built-in object, never the script's.</summary>
    private static readonly HashSet<string> PrototypeNames = new(Methods.Keys, StringComparer.Ordinal)
    {
        "constructor", "prototype", "__proto__", "__defineGetter__", "__defineSetter__", "__lookupGetter__",
        "__lookupSetter__", "apply", "bind", "call", "caller", "callee", "arguments", "then", "catch", "finally",
    };

    private static readonly HashSet<string> GlobalFunctions = new(StringComparer.Ordinal)
    {
        "parseInt", "parseFloat", "isNaN", "isFinite", "Number", "String", "Boolean", "BigInt", "Symbol",
        "encodeURIComponent", "decodeURIComponent", "encodeURI", "decodeURI", "escape", "unescape",
    };

    private static readonly HashSet<string> Namespaces = new(StringComparer.Ordinal)
    {
        "Math", "JSON", "Number", "String", "Object", "Array", "Date", "Symbol",
    };

    /// <summary>Constructors that make a fresh object: true when it holds what it is given.</summary>
    private static readonly Dictionary<string, bool> Constructors = new(StringComparer.Ordinal)
    {
        ["Date"] = false, ["RegExp"] = false, ["Number"] = false, ["String"] = false, ["Boolean"] = false,
        ["Error"] = false, ["TypeError"] = false, ["RangeError"] = false, ["SyntaxError"] = false,
        ["ReferenceError"] = false, ["EvalError"] = false, ["URIError"] = false,
        ["Map"] = true, ["Set"] = true, ["Array"] = true,
    };

    private static readonly HashSet<string> StandardConstructors = new(StringComparer.Ordinal)
    {
        "Object", "Array", "Date", "RegExp", "Map", "Set", "WeakMap", "WeakSet", "Error", "TypeError",
        "RangeError", "SyntaxError", "ReferenceError", "EvalError", "URIError", "Number", "String", "Boolean",
        "Function", "Promise", "Symbol", "BigInt",
    };

    private static Dictionary<string, Method> BuildMethods()
    {
        var methods = new Dictionary<string, Method>(StringComparer.Ordinal);
        void Add(Method method, params string[] names)
        {
            foreach (var name in names)
                methods[name] = method;
        }

        Add(new(Shape.Primitive, Callback: Callback.Iterate), "forEach", "some", "every", "findIndex", "findLastIndex");
        Add(new(Shape.Mapped, Callback: Callback.Iterate), "map", "flatMap");
        Add(new(Shape.Copy, Callback: Callback.Iterate), "filter");
        Add(new(Shape.Element, Callback: Callback.Iterate), "find", "findLast");
        Add(new(Shape.Reduced, Callback: Callback.Reduce), "reduce", "reduceRight");
        Add(new(Shape.Receiver, Effect.Reorders, Callback.Compare), "sort");
        Add(new(Shape.Copy, Callback: Callback.Compare), "toSorted");
        Add(new(Shape.Receiver, Effect.Reorders), "reverse", "copyWithin");
        Add(new(Shape.Element, Effect.Reorders), "pop", "shift");
        Add(new(Shape.Primitive, Effect.Reorders), "delete", "clear");
        Add(new(Shape.Primitive, Effect.Stores), "push", "unshift");
        Add(new(Shape.Copy, Effect.Stores), "splice");
        Add(new(Shape.Receiver, Effect.Stores), "fill", "set", "add");
        Add(new(Shape.Copy), "concat", "slice", "flat", "toReversed", "toSpliced", "with", "keys", "values",
            "entries", "split", "match", "matchAll", "exec");
        Add(new(Shape.Element), "at", "get");
        Add(new(Shape.Receiver), "valueOf");
        Add(new(Shape.Primitive, Callback: Callback.Text, CallbackIndex: 1), "replace", "replaceAll");
        Add(new(Shape.Primitive), "includes", "indexOf", "lastIndexOf", "join", "has", "test",
            "toString", "toLocaleString", "hasOwnProperty", "propertyIsEnumerable", "isPrototypeOf",
            "charAt", "charCodeAt", "codePointAt", "endsWith", "startsWith", "localeCompare", "normalize",
            "padEnd", "padStart", "repeat", "search", "substring", "substr", "trim", "trimStart", "trimEnd",
            "toLowerCase", "toUpperCase", "toLocaleLowerCase", "toLocaleUpperCase", "isWellFormed",
            "toWellFormed", "toFixed", "toPrecision", "toExponential",
            "getTime", "getFullYear", "getMonth", "getDate", "getDay", "getHours", "getMinutes", "getSeconds",
            "getMilliseconds", "getUTCFullYear", "getUTCMonth", "getUTCDate", "getUTCDay", "getUTCHours",
            "getUTCMinutes", "getUTCSeconds", "getUTCMilliseconds", "getTimezoneOffset", "toISOString",
            "toJSON", "toDateString", "toTimeString", "toUTCString", "toLocaleDateString",
            "toLocaleTimeString", "setTime", "setFullYear", "setMonth", "setDate", "setHours", "setMinutes",
            "setSeconds", "setMilliseconds", "setUTCFullYear", "setUTCMonth", "setUTCDate", "setUTCHours",
            "setUTCMinutes", "setUTCSeconds", "setUTCMilliseconds");
        return methods;
    }

    /// <summary>A call's arguments: <see cref="Union"/> of all, <see cref="Values"/> of those not written as a function in place.</summary>
    private readonly record struct Arguments(Value[] Items, Value Union, Value Values, bool Spread)
    {
        public Value First => Items.Length > 0 ? Items[0] : Value.None;
    }

    /// <summary>A function's bindings, its blocks' included: a name declared twice holds both values.</summary>
    private sealed class Scope(Node owner, Scope? parent, bool arrow)
    {
        private readonly Scope? _parent = parent;

        public Node Owner { get; } = owner;
        public bool Arrow { get; } = arrow;
        public int Depth { get; } = parent is null ? 0 : parent.Depth + 1;
        public Dictionary<string, Value> Bindings { get; } = new(StringComparer.Ordinal);

        public void Declare(string name) => Bindings.TryAdd(name, Value.None);

        public Scope? Find(string name)
        {
            for (var scope = this; scope is not null; scope = scope._parent)
            {
                if (scope.Bindings.ContainsKey(name))
                    return scope;
            }
            return null;
        }
    }

    private sealed class Proof(FunctionExpression function, Value argument)
    {
        private const int MaxPasses = 32;

        private readonly Dictionary<Node, Scope> _scopes = new(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<Node, Value> _returns = new(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<Node, Value> _captures = new(ReferenceEqualityComparer.Instance);
        private readonly List<Scope> _stack = [];

        /// <summary>What the script writes into row views.</summary>
        private Value _view;

        /// <summary>What the script writes into the objects it made, whichever they are.</summary>
        private Value _heap;

        /// <summary>An object the script made has reached something that outlives the call.</summary>
        private bool _shared;

        /// <summary>What the script throws, which a <c>catch</c> of its own receives.</summary>
        private Value _thrown;

        /// <summary>A function of the script's captures a row, or an object that may hold one.</summary>
        private bool _closureKeeps;

        private bool _changed;
        private bool _refused;

        public bool Holds()
        {
            Open(function, null, arrow: false, function.Params, function.Body, function.Id);
            for (var pass = 0; pass < MaxPasses && !_refused; pass++)
            {
                _changed = false;
                Visit(function, [argument]);
                if (!_changed)
                {
                    // The transformer turns the result into values at once, but a function in it survives as
                    // a delegate: one that captures a row would read it after the cursor has moved.
                    return !_refused && !(_closureKeeps && (Reach(_returns.GetValueOrDefault(function)) & Value.Function) != 0);
                }
            }
            return false;
        }

        // ── Declarations ─────────────────────────────────────────────────────

        private void Open(Node owner, Scope? parent, bool arrow, in NodeList<Node> parameters, Node body, Identifier? self)
        {
            var scope = new Scope(owner, parent, arrow);
            _scopes[owner] = scope;
            if (self is not null)
                scope.Declare(self.Name);
            foreach (var parameter in parameters)
                DeclarePattern(parameter, scope);
            Declare(body, scope);
        }

        private void Declare(Node node, Scope scope)
        {
            switch (node)
            {
                case FunctionDeclaration f:
                    if (f.Id is not null)
                        scope.Declare(f.Id.Name);
                    Open(f, scope, arrow: false, f.Params, f.Body, null);
                    return;
                case FunctionExpression f:
                    Open(f, scope, arrow: false, f.Params, f.Body, f.Id);
                    return;
                case ArrowFunctionExpression f:
                    Open(f, scope, arrow: true, f.Params, f.Body, null);
                    return;
                case VariableDeclaration v:
                    if (v.Kind is not (VariableDeclarationKind.Var or VariableDeclarationKind.Let or VariableDeclarationKind.Const))
                        Refuse();
                    foreach (var declarator in v.Declarations)
                    {
                        DeclarePattern(declarator.Id, scope);
                        if (declarator.Init is not null)
                            Declare(declarator.Init, scope);
                    }
                    return;
                case CatchClause c:
                    DeclarePattern(c.Param, scope);
                    Declare(c.Body, scope);
                    return;
                default:
                    foreach (var child in node.ChildNodes)
                        Declare(child, scope);
                    return;
            }
        }

        private void DeclarePattern(Node? pattern, Scope scope)
        {
            switch (pattern)
            {
                case null:
                    return;
                case Identifier id:
                    scope.Declare(id.Name);
                    return;
                case ObjectPattern o:
                    foreach (var property in o.Properties)
                    {
                        if (property is Property p)
                        {
                            if (p.Computed)
                                Declare(p.Key, scope);
                            DeclarePattern(p.Value, scope);
                        }
                        else if (property is RestElement rest)
                            DeclarePattern(rest.Argument, scope);
                        else
                            Refuse();
                    }
                    return;
                case ArrayPattern a:
                    foreach (var element in a.Elements)
                        DeclarePattern(element, scope);
                    return;
                case AssignmentPattern a:
                    DeclarePattern(a.Left, scope);
                    Declare(a.Right, scope);
                    return;
                case RestElement r:
                    DeclarePattern(r.Argument, scope);
                    return;
                default:
                    Refuse();
                    return;
            }
        }

        // ── Functions and statements ─────────────────────────────────────────

        /// <summary>
        /// Proves a function's body, its parameters seeded with what its caller passes (<paramref name="rest"/>
        /// past the seeds), and returns the closure's value.
        /// </summary>
        private Value Visit(Node owner, Value[]? seeds, Value rest = Value.Any)
        {
            NodeList<Node> parameters;
            Node body;
            switch (owner)
            {
                case FunctionExpression { Async: false, Generator: false } f:
                    (parameters, body) = (f.Params, f.Body);
                    break;
                case FunctionDeclaration { Async: false, Generator: false } f:
                    (parameters, body) = (f.Params, f.Body);
                    break;
                case ArrowFunctionExpression { Async: false } f:
                    (parameters, body) = (f.Params, f.Body);
                    break;
                default:
                    Refuse();
                    return Value.Any;
            }

            var scope = _scopes[owner];
            _stack.Add(scope);
            for (var i = 0; i < parameters.Count; i++)
            {
                var seed = seeds is not null && i < seeds.Length ? seeds[i] : rest;
                if (parameters[i] is RestElement restElement)
                    Assign(restElement.Argument, Made(seeds is null ? rest : Union(seeds, i) | rest), scope);
                else
                    Assign(parameters[i], seed, scope);
            }

            if (body is Expression expression)
                Return(owner, Eval(expression, scope));
            else
                Exec(body, scope);
            _stack.RemoveAt(_stack.Count - 1);

            var closure = Value.Function;
            if (IsKept(_captures.GetValueOrDefault(owner)))
            {
                closure |= Value.Holder;
                if (!_closureKeeps)
                {
                    _closureKeeps = true;
                    _changed = true;
                }
            }
            if (owner is FunctionExpression { Id: { } self })
                Grow(scope, self.Name, closure);
            return closure;
        }

        private void Exec(Node? node, Scope s)
        {
            switch (node)
            {
                case null:
                case BreakStatement or ContinueStatement or EmptyStatement or DebuggerStatement:
                    return;
                case BlockStatement b:
                    foreach (var statement in b.Body)
                        Exec(statement, s);
                    return;
                case ExpressionStatement e:
                    Eval(e.Expression, s);
                    return;
                case VariableDeclaration v:
                    foreach (var declarator in v.Declarations)
                        Assign(declarator.Id, declarator.Init is null ? Value.None : Eval(declarator.Init, s), s);
                    return;
                case FunctionDeclaration f:
                    var closure = Visit(f, null);
                    if (f.Id is not null)
                        AssignName(f.Id.Name, closure, s);
                    return;
                case ReturnStatement r:
                    Return(s.Owner, Eval(r.Argument, s));
                    return;
                case IfStatement i:
                    Eval(i.Test, s);
                    Exec(i.Consequent, s);
                    Exec(i.Alternate, s);
                    return;
                case ForStatement f:
                    if (f.Init is Expression init)
                        Eval(init, s);
                    else
                        Exec(f.Init, s);
                    Eval(f.Test, s);
                    Eval(f.Update, s);
                    Exec(f.Body, s);
                    return;
                case ForInStatement f:
                    Eval(f.Right, s);
                    Assign(LoopTarget(f.Left), Value.None, s);
                    Exec(f.Body, s);
                    return;
                case ForOfStatement { Await: false } f:
                    Assign(LoopTarget(f.Left), Contents(Eval(f.Right, s)), s);
                    Exec(f.Body, s);
                    return;
                case WhileStatement w:
                    Eval(w.Test, s);
                    Exec(w.Body, s);
                    return;
                case DoWhileStatement d:
                    Exec(d.Body, s);
                    Eval(d.Test, s);
                    return;
                case LabeledStatement l:
                    Exec(l.Body, s);
                    return;
                case ThrowStatement t:
                    // The transformer catches what a script throws and keeps only its message.
                    var thrown = Eval(t.Argument, s);
                    if ((_thrown | thrown) != _thrown)
                    {
                        _thrown |= thrown;
                        _changed = true;
                    }
                    return;
                case TryStatement t:
                    Exec(t.Block, s);
                    if (t.Handler is { } handler)
                    {
                        if (handler.Param is not null)
                            // What the script throws, an error the engine makes, or what a foreign function throws.
                            Assign(handler.Param, _thrown | Value.Local | Value.Foreign, s);
                        Exec(handler.Body, s);
                    }
                    Exec(t.Finalizer, s);
                    return;
                case SwitchStatement w:
                    Eval(w.Discriminant, s);
                    foreach (var c in w.Cases)
                    {
                        Eval(c.Test, s);
                        foreach (var statement in c.Consequent)
                            Exec(statement, s);
                    }
                    return;
                default:
                    Refuse();
                    return;
            }
        }

        private static Node LoopTarget(Node left)
            => left is VariableDeclaration { Declarations.Count: 1 } v ? v.Declarations[0].Id : left;

        // ── Expressions ──────────────────────────────────────────────────────

        private Value Eval(Node? node, Scope s)
        {
            switch (node)
            {
                case null:
                    return Value.None;
                case RegExpLiteral:
                    return Value.Local;
                case Literal:
                    return Value.None;
                case TemplateLiteral t:
                    foreach (var expression in t.Expressions)
                        Eval(expression, s);
                    return Value.None;
                case TaggedTemplateExpression t:
                    return Tagged(t, s);
                case Identifier id:
                    return Read(id.Name, s);
                case ThisExpression:
                    CaptureContext();
                    return Value.Any;
                case ArrayExpression a:
                    var elements = Value.None;
                    foreach (var element in a.Elements)
                        elements |= element is SpreadElement spread ? Contents(Eval(spread.Argument, s)) : Eval(element, s);
                    return Made(elements);
                case ObjectExpression o:
                    return ObjectLiteral(o, s);
                case FunctionExpression or ArrowFunctionExpression:
                    return Visit(node, null);
                case UpdateExpression u:
                    Assign(u.Argument, Value.None, s);
                    return Value.None;
                case UnaryExpression { Operator: Operator.Delete, Argument: MemberExpression m }:
                    WriteMember(m, Value.None, s);
                    return Value.None;
                case UnaryExpression u:
                    Eval(u.Argument, s);
                    return Value.None;
                case LogicalExpression l:
                    return Eval(l.Left, s) | Eval(l.Right, s);
                case BinaryExpression { Operator: Operator.InstanceOf } b:
                    return InstanceOf(b, s);
                case BinaryExpression b:
                    Eval(b.Left, s);
                    Eval(b.Right, s);
                    return Value.None;
                case AssignmentExpression a:
                    return Assignment(a, s);
                case ConditionalExpression c:
                    Eval(c.Test, s);
                    return Eval(c.Consequent, s) | Eval(c.Alternate, s);
                case SequenceExpression q:
                    var last = Value.None;
                    foreach (var expression in q.Expressions)
                        last = Eval(expression, s);
                    return last;
                case CallExpression c:
                    return Call(c, s);
                case NewExpression n:
                    return New(n, s);
                case MemberExpression m:
                    return Member(m, s);
                case ChainExpression c:
                    return Eval(c.Expression, s);
                case ParenthesizedExpression p:
                    return Eval(p.Expression, s);
                default:
                    Refuse();
                    return Value.Any;
            }
        }

        private Value Read(string name, Scope s)
        {
            if (s.Find(name) is { } owner)
            {
                var value = owner.Bindings[name];
                foreach (var scope in _stack)
                {
                    if (scope.Depth > owner.Depth)
                        GrowCapture(scope.Owner, value);
                }
                return value;
            }

            switch (name)
            {
                case "undefined" or "NaN" or "Infinity":
                    return Value.None;
                case "arguments":
                    CaptureContext();
                    return Value.Any;
                case "eval":
                    Refuse();
                    return Value.Any;
                default:
                    return Value.Foreign;
            }
        }

        /// <summary><c>this</c> and <c>arguments</c> of an arrow function are its enclosing function's.</summary>
        private void CaptureContext()
        {
            for (var i = _stack.Count - 1; i >= 0 && _stack[i].Arrow; i--)
                GrowCapture(_stack[i].Owner, Value.Any);
        }

        private Value ObjectLiteral(ObjectExpression o, Scope s)
        {
            var content = Value.None;
            foreach (var property in o.Properties)
            {
                switch (property)
                {
                    case SpreadElement spread:
                        content |= Contents(Eval(spread.Argument, s));
                        break;
                    case Property p:
                        if (p.Computed)
                            Eval(p.Key, s);
                        content |= Eval(p.Value, s);
                        break;
                    default:
                        Refuse();
                        break;
                }
            }
            return Made(content);
        }

        private Value Assignment(AssignmentExpression a, Scope s)
        {
            switch (a.Operator)
            {
                case Operator.Assignment:
                    var value = Eval(a.Right, s);
                    Assign(a.Left, value, s);
                    return value;
                case Operator.NullishCoalescingAssignment or Operator.LogicalOrAssignment or Operator.LogicalAndAssignment:
                    var before = ReadTarget(a.Left, s);
                    var right = Eval(a.Right, s);
                    Assign(a.Left, right, s);
                    return before | right;
                default:
                    ReadTarget(a.Left, s);
                    Eval(a.Right, s);
                    Assign(a.Left, Value.None, s);
                    return Value.None;
            }
        }

        private Value ReadTarget(Node target, Scope s)
        {
            switch (target)
            {
                case Identifier id:
                    return Read(id.Name, s);
                case MemberExpression m:
                    return Member(m, s);
                default:
                    Refuse();
                    return Value.Any;
            }
        }

        private Value Member(MemberExpression m, Scope s)
        {
            var target = Eval(m.Object, s);
            var name = Key(m.Property, m.Computed, s, out var computedKey);
            // A window's array has a length, not a row, under that name.
            var value = Contents(name == "length" ? target & ~Value.Rows : target);
            if (computedKey || (name is not null && PrototypeNames.Contains(name)))
                value |= Value.Foreign;
            return value;
        }

        /// <summary>A property's name, or null for a number or a key computed at run time (<paramref name="computedKey"/>).</summary>
        private string? Key(Node key, bool computed, Scope s, out bool computedKey)
        {
            computedKey = false;
            switch (key)
            {
                case Identifier id when !computed:
                    return id.Name;
                case StringLiteral text:
                    return text.Value;
                case NumericLiteral:
                    return null;
                case PrivateIdentifier:
                    Refuse();
                    return null;
                default:
                    Eval(key, s);
                    computedKey = true;
                    return null;
            }
        }

        private Value InstanceOf(BinaryExpression b, Scope s)
        {
            var left = Eval(b.Left, s);
            var right = Eval(b.Right, s);
            // A constructor the script did not define may answer with a function of its own.
            var standard = b.Right is Identifier { Name: var name } && s.Find(name) is null && StandardConstructors.Contains(name);
            if (!standard && IsKept(left) && (right & (Value.Foreign | Value.Cell | Kept)) != 0)
                Refuse();
            return Value.None;
        }

        private Value Tagged(TaggedTemplateExpression t, Scope s)
        {
            var given = Value.Local;
            foreach (var expression in t.Quasi.Expressions)
                given |= Eval(expression, s);
            if (t.Tag is MemberExpression { Computed: false, Object: Identifier { Name: "String" }, Property: Identifier { Name: "raw" } }
                && s.Find("String") is null)
            {
                return Value.None;
            }

            if ((Eval(t.Tag, s) & (Value.Foreign | Value.Cell | Kept)) != 0)
                Escape(given);
            return Value.Any;
        }

        // ── Calls ────────────────────────────────────────────────────────────

        private Value Call(CallExpression c, Scope s)
        {
            switch (c.Callee)
            {
                case MemberExpression { Computed: false, Object: Identifier ns, Property: Identifier fn }
                    when Namespaces.Contains(ns.Name) && s.Find(ns.Name) is null:
                    return Namespace(ns.Name, fn.Name, c.Arguments, s);
                case MemberExpression m:
                    var receiver = Eval(m.Object, s);
                    return CallMethod(receiver, Key(m.Property, m.Computed, s, out _), c.Arguments, s);
                case Identifier id when s.Find(id.Name) is null:
                    if (id.Name == "eval")
                    {
                        Refuse();
                        return Value.Any;
                    }
                    if (GlobalFunctions.Contains(id.Name))
                    {
                        Evaluate(c.Arguments, s, null);
                        return Value.None;
                    }
                    return CallForeign(c.Arguments, s);
                case FunctionExpression or ArrowFunctionExpression:
                    var given = Evaluate(c.Arguments, s, null);
                    Visit(c.Callee, given.Spread ? null : given.Items, given.Spread ? Value.Any : Value.None);
                    return _returns.GetValueOrDefault(c.Callee);
                default:
                    return CallValue(Eval(c.Callee, s), c.Arguments, s);
            }
        }

        private Value New(NewExpression n, Scope s)
        {
            switch (n.Callee)
            {
                case Identifier id when s.Find(id.Name) is null && Constructors.TryGetValue(id.Name, out var holds):
                    var given = Evaluate(n.Arguments, s, null).Union;
                    return holds ? Made(given | Contents(given) | Contents(Contents(given))) : Value.Local;
                case FunctionExpression or ArrowFunctionExpression:
                    Evaluate(n.Arguments, s, null);
                    Visit(n.Callee, null);
                    return Value.Any;
                default:
                    return CallValue(Eval(n.Callee, s), n.Arguments, s);
            }
        }

        /// <summary>A call of a function held in a value: one the script defined is proved where it stands.</summary>
        private Value CallValue(Value callee, in NodeList<Expression> arguments, Scope s)
        {
            if ((callee & (Value.Foreign | Value.Cell | Value.Row | Value.Rows)) != 0)
                return CallForeign(arguments, s);
            Evaluate(arguments, s, null);
            return Value.Any;
        }

        /// <summary>A function the script does not see may keep what it is given.</summary>
        private Value CallForeign(in NodeList<Expression> arguments, Scope s)
        {
            Escape(Evaluate(arguments, s, null).Union);
            return Value.Any;
        }

        private Value CallMethod(Value receiver, string? name, in NodeList<Expression> arguments, Scope s)
        {
            Method? method = name is not null && Methods.TryGetValue(name, out var known) ? known : null;
            var foreign = (receiver & Value.Foreign) != 0;
            // An object that holds a function of the script's may answer a built-in's name with it.
            var scripted = (receiver & Value.Function) != 0
                || ((receiver & Value.Local) != 0 && (_heap & Value.Function) != 0)
                || ((receiver & Value.Row) != 0 && (_view & Value.Function) != 0);
            var trusted = method is not null && !foreign && !scripted;
            var element = Contents(receiver);
            var argumentNodes = arguments;
            var given = Evaluate(arguments, s, trusted ? (index, items) => Seeds(method!.Value, index, receiver, element, items, argumentNodes) : null);

            if (method is not { } m)
            {
                // A CLR method of a cell, a built-in this proof does not list, or a function the script wrote.
                if (IsKept(receiver))
                    Refuse();
                Escape(given.Union);
                return Value.Any;
            }

            if (foreign)
            {
                Escape(given.Union);
                return Value.Any;
            }

            if ((receiver & Value.Rows) != 0 && m.Effect != Effect.None)
                Refuse();
            if (m.Effect == Effect.Stores)
                Store(receiver, given.Union, named: false);

            if (!trusted)
                return Value.Any;

            return m.Result switch
            {
                Shape.Primitive => Value.None,
                Shape.Element => element,
                Shape.Receiver => receiver,
                // A callback is called, not copied.
                Shape.Copy => Made(element | given.Values),
                Shape.Mapped => Made(Returned(argumentNodes, m.CallbackIndex)),
                Shape.Reduced => (argumentNodes.Count > 1 ? given.Items[1] : element) | Returned(argumentNodes, m.CallbackIndex),
                _ => Value.Any,
            };
        }

        /// <summary>What a built-in passes the callback at <paramref name="index"/>.</summary>
        private Value[]? Seeds(Method method, int index, Value receiver, Value element, Value[] items, NodeList<Expression> arguments)
        {
            if (index != method.CallbackIndex)
                return null;
            return method.Callback switch
            {
                Callback.Iterate => [element, element, receiver],
                Callback.Reduce => [(arguments.Count > 1 ? items[1] : element) | Returned(arguments, index), element, element, receiver],
                Callback.Compare => [element, element],
                Callback.Text => [],
                _ => null,
            };
        }

        private Value Namespace(string ns, string name, in NodeList<Expression> arguments, Scope s)
        {
            switch (ns, name)
            {
                case ("Math", _):
                case ("JSON", "stringify"):
                case ("Number", "isFinite" or "isInteger" or "isNaN" or "isSafeInteger" or "parseFloat" or "parseInt"):
                case ("String", "fromCharCode" or "fromCodePoint" or "raw"):
                case ("Array", "isArray"):
                case ("Date", "now" or "UTC" or "parse"):
                case ("Object", "is" or "hasOwn"):
                case ("Symbol", "for" or "keyFor"):
                    Evaluate(arguments, s, null);
                    return Value.None;
                case ("JSON", "parse"):
                case ("Object", "keys" or "getOwnPropertyNames"):
                    Evaluate(arguments, s, null);
                    return Value.Local;
                case ("Object", "values" or "entries"):
                    return Made(Contents(Evaluate(arguments, s, null).First));
                case ("Object", "fromEntries"):
                    return Made(Contents(Contents(Evaluate(arguments, s, null).First)));
                case ("Object", "assign"):
                    return ObjectAssign(arguments, s);
                case ("Array", "of"):
                    return Made(Evaluate(arguments, s, null).Union);
                case ("Array", "from"):
                    var given = Evaluate(arguments, s, (index, items) => index == 1 ? [Contents(items[0]), Value.None] : null);
                    return Made(arguments.Count > 1 ? Returned(arguments, 1) : Contents(given.First));
                default:
                    return CallForeign(arguments, s);
            }
        }

        private Value ObjectAssign(in NodeList<Expression> arguments, Scope s)
        {
            var given = Evaluate(arguments, s, null);
            if (given.Items.Length == 0 || given.Spread)
            {
                Escape(given.Union);
                return Value.Any;
            }

            var target = given.Items[0];
            var copied = Value.None;
            for (var i = 1; i < given.Items.Length; i++)
                copied |= Contents(given.Items[i]);

            // Every key is copied, a symbol's too.
            Store(target, copied, named: false);
            return target;
        }

        /// <summary>
        /// Evaluates a call's arguments: the values first, then each function written in place, which
        /// <paramref name="seeds"/> may type from the values (null: anything).
        /// </summary>
        private Arguments Evaluate(in NodeList<Expression> arguments, Scope s, Func<int, Value[], Value[]?>? seeds)
        {
            var items = new Value[arguments.Count];
            var spread = false;
            for (var i = 0; i < arguments.Count; i++)
            {
                switch (arguments[i])
                {
                    case SpreadElement element:
                        items[i] = Contents(Eval(element.Argument, s));
                        spread = true;
                        break;
                    case FunctionExpression or ArrowFunctionExpression:
                        break;
                    default:
                        items[i] = Eval(arguments[i], s);
                        break;
                }
            }

            var values = Value.None;
            foreach (var item in items)
                values |= item;

            var union = values;
            for (var i = 0; i < arguments.Count; i++)
            {
                if (arguments[i] is FunctionExpression or ArrowFunctionExpression)
                {
                    var typed = spread ? null : seeds?.Invoke(i, items);
                    items[i] = Visit(arguments[i], typed, typed is null ? Value.Any : Value.Local);
                    union |= items[i];
                }
            }
            return new Arguments(items, union, values, spread);
        }

        private Value Returned(in NodeList<Expression> arguments, int index)
            => index < arguments.Count && arguments[index] is FunctionExpression or ArrowFunctionExpression
                ? _returns.GetValueOrDefault(arguments[index])
                : Value.Any;

        // ── Writes ───────────────────────────────────────────────────────────

        private void Assign(Node target, Value value, Scope s)
        {
            switch (target)
            {
                case Identifier id:
                    AssignName(id.Name, value, s);
                    return;
                case MemberExpression m:
                    WriteMember(m, value, s);
                    return;
                case ObjectPattern o:
                    foreach (var property in o.Properties)
                    {
                        switch (property)
                        {
                            case Property p:
                                var item = Contents(value);
                                var name = Key(p.Key, p.Computed, s, out var computedKey);
                                if (computedKey || (name is not null && PrototypeNames.Contains(name)))
                                    item |= Value.Foreign;
                                Assign(p.Value, item, s);
                                break;
                            case RestElement rest:
                                Assign(rest.Argument, Made(Contents(value)), s);
                                break;
                            default:
                                Refuse();
                                break;
                        }
                    }
                    return;
                case ArrayPattern a:
                    var element = Contents(value);
                    foreach (var e in a.Elements)
                    {
                        if (e is RestElement rest)
                            Assign(rest.Argument, Made(element), s);
                        else if (e is not null)
                            Assign(e, element, s);
                    }
                    return;
                case AssignmentPattern d:
                    Assign(d.Left, value | Eval(d.Right, s), s);
                    return;
                default:
                    Refuse();
                    return;
            }
        }

        private void AssignName(string name, Value value, Scope s)
        {
            if (s.Find(name) is { } owner)
                Grow(owner, name, value);
            else
                Escape(value);
        }

        private void WriteMember(MemberExpression m, Value value, Scope s)
        {
            var target = Eval(m.Object, s);
            Store(target, value, named: Key(m.Property, m.Computed, s, out _) is not null);
        }

        /// <summary>
        /// <paramref name="value"/> written into <paramref name="target"/>, under a name when
        /// <paramref name="named"/>, else under a number, a symbol or keys not known here.
        /// </summary>
        private void Store(Value target, Value value, bool named)
        {
            // A window's array comes back as it went in.
            if ((target & Value.Rows) != 0)
                Refuse();
            if ((target & Value.Row) != 0)
            {
                // A view keeps a write under a name for its row; a symbol or a number lands on the view itself.
                if (!named)
                    Refuse();
                GrowView(value);
            }
            if ((target & Value.Foreign) != 0)
                Escape(value);
            if ((target & Value.Cell) != 0)
                WriteCell(value);
            if ((target & (Value.Local | Value.Function | Value.Holder)) != 0)
                GrowHeap(value);
            // A foreign function on an object that holds a row would see it through an implicit conversion.
            if ((target & Kept) != 0 && (value & Value.Foreign) != 0)
                Refuse();
        }

        /// <summary>A value that reaches something outliving the call: a row must not, and what the script made is now shared.</summary>
        private void Escape(Value value)
        {
            if (IsKept(value))
                Refuse();
            if ((value & (Value.Local | Value.Function)) != 0)
                Share();
        }

        /// <summary>A cell outlives the call on the row path: it takes no row, function or object of the script's.</summary>
        private void WriteCell(Value value)
        {
            if ((value & (Kept | Value.Function)) != 0)
                Refuse();
            if ((value & Value.Local) != 0)
                Share();
        }

        // ── Values ───────────────────────────────────────────────────────────

        private bool IsKept(Value value)
            => (value & Kept) != 0 || ((value & (Value.Local | Value.Function)) != 0 && (_heap & Kept) != 0);

        /// <summary>What reading a property, an element or an iteration of <paramref name="value"/> may give.</summary>
        private Value Contents(Value value)
        {
            // A function of the script's, an accessor among them, may return anything.
            if ((value & Value.Function) != 0 || ((value & Value.Local) != 0 && (_heap & Value.Function) != 0))
                return Value.Any;

            var contents = value & (Value.Foreign | Value.Cell | Value.Local);
            if ((value & Value.Row) != 0)
                contents |= _view | Value.Cell;
            if ((value & Value.Rows) != 0)
                contents |= Value.Row;
            if ((value & Value.Holder) != 0)
                contents |= Kept;
            if ((value & Value.Local) != 0)
                contents |= _heap;
            return contents;
        }

        /// <summary>Everything a value may lead to.</summary>
        private Value Reach(Value value)
        {
            while (true)
            {
                var next = value | Contents(value);
                if (next == value)
                    return value;
                value = next;
            }
        }

        /// <summary>A new object holding <paramref name="content"/>.</summary>
        private Value Made(Value content)
        {
            var made = Value.Local
                | (content & (Value.Foreign | Value.Cell | Value.Function | Value.Local))
                | ((content & Kept) != 0 ? Value.Holder : Value.None);
            // A foreign function on an object that holds a row would see it through an implicit conversion.
            if ((made & Value.Holder) != 0 && (made & Value.Foreign) != 0)
                Refuse();
            return made;
        }

        private static Value Union(Value[] values, int from)
        {
            var union = Value.None;
            for (var i = from; i < values.Length; i++)
                union |= values[i];
            return union;
        }

        private void Grow(Scope owner, string name, Value value)
        {
            var before = owner.Bindings[name];
            if ((before | value) != before)
            {
                owner.Bindings[name] = before | value;
                _changed = true;
            }
        }

        private void GrowCapture(Node owner, Value value)
        {
            var before = _captures.GetValueOrDefault(owner);
            if ((before | value) != before)
            {
                _captures[owner] = before | value;
                _changed = true;
            }
        }

        private void Return(Node owner, Value value)
        {
            var before = _returns.GetValueOrDefault(owner);
            if ((before | value) != before)
            {
                _returns[owner] = before | value;
                _changed = true;
            }
        }

        private void GrowView(Value value)
        {
            if ((_view | value) != _view)
            {
                _view |= value;
                _changed = true;
            }
            // The view would hand itself to a foreign function through an implicit conversion.
            if ((_view & Value.Foreign) != 0)
                Refuse();
        }

        private void GrowHeap(Value value)
        {
            if ((_heap | value) != _heap)
            {
                _heap |= value;
                _changed = true;
            }
            if ((_heap & Kept) != 0 && (_shared || (_heap & Value.Foreign) != 0))
                Refuse();
        }

        private void Share()
        {
            if (!_shared)
            {
                _shared = true;
                _changed = true;
            }
            if ((_heap & Kept) != 0)
                Refuse();
        }

        private void Refuse() => _refused = true;
    }
}

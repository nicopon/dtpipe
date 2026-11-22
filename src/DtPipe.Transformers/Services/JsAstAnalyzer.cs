using Acornima;
using Acornima.Ast;
using DtPipe.Core.Models;

namespace DtPipe.Transformers.Services;

internal static class JsAstAnalyzer
{
    internal sealed record ExpandUnnestPlan(int ListColumnIndex, int TargetColumnIndex);

    private static readonly HashSet<string> PureGlobals = new(StringComparer.Ordinal)
    {
        "row", "Math", "Number", "String", "Boolean", "JSON",
        "parseInt", "parseFloat", "isNaN", "isFinite",
        "undefined", "NaN", "Infinity"
    };

    // ── Purity analysis for intra-batch parallel Jint execution ──────────────

    public static bool IsPureForParallel(string wrappedScript)
    {
        try
        {
            var parser = new Parser();
            if (parser.ParseExpression($"({wrappedScript})") is not FunctionExpression fn)
                return false;

            var locals = new HashSet<string>(StringComparer.Ordinal);
            foreach (var param in fn.Params)
            {
                if (!CollectBindingNames(param, locals))
                    return false;
            }

            if (!CollectDeclaredLocals(fn.Body, locals))
                return false;

            return CheckPureNode(fn.Body, locals);
        }
        catch
        {
            return false;
        }
    }

    private static bool CollectBindingNames(Node? pattern, HashSet<string> locals)
    {
        switch (pattern)
        {
            case null:
                return true;
            case Identifier id:
                locals.Add(id.Name);
                return true;
            case AssignmentPattern ap:
                return CollectBindingNames(ap.Left, locals);
            case RestElement re:
                return CollectBindingNames(re.Argument, locals);
            case ArrayPattern arr:
                foreach (var el in arr.Elements)
                    if (!CollectBindingNames(el, locals)) return false;
                return true;
            case ObjectPattern obj:
                foreach (var prop in obj.Properties)
                {
                    if (prop is ObjectProperty op && !CollectBindingNames(op.Value, locals)) return false;
                    else if (prop is RestElement rest && !CollectBindingNames(rest.Argument, locals)) return false;
                    else if (prop is not ObjectProperty and not RestElement) return false;
                }
                return true;
            default:
                return false;
        }
    }

    private static bool CollectDeclaredLocals(Node node, HashSet<string> locals)
    {
        switch (node)
        {
            case VariableDeclaration vd:
                foreach (var decl in vd.Declarations)
                {
                    if (!CollectBindingNames(decl.Id, locals)) return false;
                    if (decl.Init != null && !CollectDeclaredLocals(decl.Init, locals)) return false;
                }
                return true;
            case FunctionDeclaration fd:
                if (fd.Id != null) locals.Add(fd.Id.Name);
                return true;
            case CatchClause cc:
                if (cc.Param != null && !CollectBindingNames(cc.Param, locals)) return false;
                return CollectDeclaredLocals(cc.Body, locals);
            case FunctionExpression or ArrowFunctionExpression:
                // Nested closures introduce their own scope; inspect lazily during traversal.
                return true;
            default:
                foreach (var child in node.ChildNodes)
                    if (!CollectDeclaredLocals(child, locals)) return false;
                return true;
        }
    }

    private static bool CheckPureNode(Node node, HashSet<string> locals)
    {
        switch (node)
        {
            case Identifier id:
                return locals.Contains(id.Name) || PureGlobals.Contains(id.Name);

            case AssignmentExpression ae:
                if (ae.Left is not Identifier targetId || !locals.Contains(targetId.Name))
                    return false;
                return CheckPureNode(ae.Right, locals);

            case UpdateExpression ue:
                return ue.Argument is Identifier updId && locals.Contains(updId.Name);

            case VariableDeclaration vd:
                foreach (var decl in vd.Declarations)
                {
                    if (decl.Init != null && !CheckPureNode(decl.Init, locals)) return false;
                }
                return true;

            case MemberExpression me:
                if (!CheckPureNode(me.Object, locals)) return false;
                return !me.Computed || CheckPureNode(me.Property, locals);

            case ObjectProperty op:
                if (op.Computed && !CheckPureNode(op.Key, locals)) return false;
                return CheckPureNode(op.Value, locals);

            case CallExpression ce:
                if (ce.Callee is Identifier callId && (callId.Name == "eval" || callId.Name == "Function"))
                    return false;
                if (ce.Callee is MemberExpression { Computed: false, Object: Identifier { Name: "Math" }, Property: Identifier { Name: "random" } })
                    return false;
                if (!CheckPureNode(ce.Callee, locals)) return false;
                foreach (var arg in ce.Arguments)
                    if (!CheckPureNode(arg, locals)) return false;
                return true;

            case NewExpression ne:
                if (ne.Callee is Identifier { Name: "Date" })
                    return false;
                if (!CheckPureNode(ne.Callee, locals)) return false;
                foreach (var arg in ne.Arguments)
                    if (!CheckPureNode(arg, locals)) return false;
                return true;

            case LabeledStatement ls:
                return CheckPureNode(ls.Body, locals);

            case BreakStatement or ContinueStatement:
                return true;

            case FunctionDeclaration fd:
                return CheckNestedFunctionPure(fd.Params, fd.Body, locals);

            case FunctionExpression fe:
                var feLocals = new HashSet<string>(locals, StringComparer.Ordinal);
                if (fe.Id != null) feLocals.Add(fe.Id.Name);
                return CheckNestedFunctionPure(fe.Params, fe.Body, feLocals);

            case ArrowFunctionExpression af:
                return CheckNestedFunctionPure(af.Params, af.Body, locals);

            default:
                foreach (var child in node.ChildNodes)
                    if (!CheckPureNode(child, locals)) return false;
                return true;
        }
    }

    private static bool CheckNestedFunctionPure(in NodeList<Node> parameters, Node body, HashSet<string> outerLocals)
    {
        var nested = new HashSet<string>(outerLocals, StringComparer.Ordinal);
        foreach (var p in parameters)
            if (!CollectBindingNames(p, nested)) return false;
        if (!CollectDeclaredLocals(body, nested)) return false;
        return CheckPureNode(body, nested);
    }

    // ── Expand structural unnest analyzer ────────────────────────────────────

    public static ExpandUnnestPlan? TryMatchExpandUnnest(
        string script,
        IReadOnlyList<PipeColumnInfo> inputColumns,
        IReadOnlyList<PipeColumnInfo> outputColumns)
    {
        var expr = ParseSingleExpression(script);
        if (expr is not CallExpression call
            || call.Callee is not MemberExpression { Computed: false, Property: Identifier { Name: "map" } } callee
            || !TryMatchRowColumn(callee.Object, inputColumns, out int listColIdx)
            || call.Arguments.Count != 1
            || call.Arguments[0] is not ArrowFunctionExpression { Params.Count: 1 } arrow
            || arrow.Params[0] is not Identifier paramId)
        {
            return null;
        }

        ObjectExpression? objExpr = arrow.Body as ObjectExpression;
        if (objExpr == null && arrow.Body is BlockStatement { Body.Count: 1 } block
            && block.Body[0] is ReturnStatement { Argument: ObjectExpression retObj })
        {
            objExpr = retObj;
        }

        if (objExpr == null
            || objExpr.Properties.Count != 2
            || objExpr.Properties[0] is not SpreadElement { Argument: Identifier { Name: "row" } }
            || objExpr.Properties[1] is not ObjectProperty { Computed: false, Key: Identifier keyId, Value: Identifier valId }
            || valId.Name != paramId.Name)
        {
            return null;
        }

        int targetColIdx = -1;
        for (int i = 0; i < outputColumns.Count; i++)
        {
            if (string.Equals(outputColumns[i].Name, keyId.Name, StringComparison.OrdinalIgnoreCase))
            {
                targetColIdx = i;
                break;
            }
        }

        if (targetColIdx < 0 || targetColIdx == listColIdx)
            return null;

        return new ExpandUnnestPlan(listColIdx, targetColIdx);
    }

    // ── Shared expression parsing & schema helpers ───────────────────────────

    private static Expression? ParseSingleExpression(string script)
    {
        var trimmed = script.Trim();
        if (trimmed.Length == 0) return null;

        var parser = new Parser();
        var exprText = trimmed.TrimEnd(';').Trim();
        if (exprText.Length > 0)
        {
            try { return parser.ParseExpression(exprText); }
            catch { }
        }

        try
        {
            var parsed = parser.ParseScript(trimmed);
            if (parsed.Body.Count == 1)
            {
                if (parsed.Body[0] is ReturnStatement { Argument: { } retExpr })
                    return retExpr;
                if (parsed.Body[0] is ExpressionStatement { Expression: { } stmtExpr })
                    return stmtExpr;
            }
        }
        catch { }

        return null;
    }

    private static bool TryMatchRowColumn(Node node, IReadOnlyList<PipeColumnInfo> inputColumns, out int colIndex)
    {
        colIndex = -1;
        string? name = node switch
        {
            MemberExpression { Computed: false, Object: Identifier { Name: "row" }, Property: Identifier propId } => propId.Name,
            MemberExpression { Computed: true, Object: Identifier { Name: "row" }, Property: StringLiteral strLit } => strLit.Value,
            _ => null
        };

        if (name == null) return false;

        // JS property lookup on `row` is case-sensitive (Ordinal).
        for (int i = 0; i < inputColumns.Count; i++)
        {
            if (string.Equals(inputColumns[i].Name, name, StringComparison.Ordinal))
            {
                colIndex = i;
                return true;
            }
        }
        return false;
    }
}

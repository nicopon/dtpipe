using AwesomeAssertions;
using DtPipe.Transformers.Services;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// What decides whether a script gets the view a transformer reuses from row to row. A script that
/// cannot keep its argument shares it; one that can, or that the analysis cannot read, gets a view of
/// its own per row. A form that does not keep the row but that the analysis cannot tell apart from one
/// that does is listed with the per-row view: the shared view is an optimisation, never a guess.
/// </summary>
public class JsRowRetentionTests
{
    [Theory]
    // Reads, computations, locals.
    [InlineData("return row.A * 2;")]
    [InlineData("const r = row; return r.A + 1;")]
    [InlineData("return Math.max(row.A, 0);")]
    [InlineData("return row.Name.toUpperCase();")]
    [InlineData("return row.Email.split('@')[1];")]
    [InlineData("const a = row.A; if (a >= 7000) return 'C'; return 'A';")]
    [InlineData("const { A, B } = row; return A + B;")]
    [InlineData("return JSON.stringify(row);")]
    [InlineData("return Object.keys(row).length;")]
    [InlineData("return new Date(row.D).getFullYear();")]
    [InlineData("return (() => { const x = row.A; return x * 2; })();")]
    // State that holds values, not the row.
    [InlineData("globalThis.prevA = row.A; return 1;")]
    [InlineData("globalThis.n = (globalThis.n || 0) + 1; return globalThis.n;")]
    [InlineData("return __seq++;")]
    [InlineData("({ A: globalThis.lastA } = row); return 1;")]
    [InlineData("globalThis.seen = globalThis.seen || new Set(); globalThis.seen.add(row.Id); return 1;")]
    [InlineData("return globalThis.fmt(row.A);")]
    [InlineData("try { return row.A.x; } catch (e) { globalThis.err = e.message; return null; }")]
    // Rows returned, copied or written, which the transformer reads before the next call.
    [InlineData("return [row];")]
    [InlineData("return [row, { ...row, A: row.A * 10 }];")]
    [InlineData("row.X = row.A + 1; return [row];")]
    [InlineData("return row.Tags.map(t => ({ ...row, Tag: t }));")]
    [InlineData("return JSON.parse(row.Tags).map(t => ({ ...row, Tags: t }));")]
    [InlineData("const out = []; for (const t of row.Tags) out.push({ ...row, Tag: t }); return out;")]
    [InlineData("const out = []; out.push(row); return out;")]
    [InlineData("return Object.assign({}, row, { X: 1 });")]
    public void A_Script_That_Cannot_Keep_Its_Row_Shares_The_View(string body)
        => JsRowRetention.CanShareView($"function(row) {{ {body} }}", JsRowArgument.Row).Should().BeTrue();

    [Theory]
    [InlineData("return rows;")]
    [InlineData("return rows.map(r => ({ ...r, X: 1 }));")]
    [InlineData("return [{ Id: rows[0].Id, Total: rows.reduce((s, r) => s + r.V, 0) / rows.length }];")]
    [InlineData("const sum = rows.reduce((s, r) => s + r.V, 0); rows.forEach(r => r.V = sum); return rows;")]
    [InlineData("return rows.filter(r => r.V > 0);")]
    [InlineData("return [...rows].sort((a, b) => a.V - b.V);")]
    [InlineData("globalThis.lastV = rows[rows.length - 1].V; return rows;")]
    public void A_Window_Script_That_Keeps_No_Row_And_Leaves_The_Array_Shares_The_Pool(string body)
        => JsRowRetention.CanShareView($"function(rows) {{ {body} }}", JsRowArgument.Rows).Should().BeTrue();

    [Theory]
    // Into a global, or a variable declared outside the script.
    [InlineData("globalThis.p = row; return 1;")]
    [InlineData("const p = globalThis.__p; globalThis.__p = row; return p ? row.A - p.A : null;")]
    [InlineData("p = row; return 1;")]
    [InlineData("acc.push(row); return 1;")]
    [InlineData("globalThis.acc.push(row); return 1;")]
    [InlineData("globalThis.m.set(row.Id, row); return 1;")]
    [InlineData("Object.assign(globalThis, { k: row }); return 1;")]
    [InlineData("globalThis.k = arguments[0]; return 1;")]
    // A closure stored or returned.
    [InlineData("globalThis.f = () => row.A; return 1;")]
    [InlineData("return () => row.A;")]
    [InlineData("const o = {}; o.r = row; return () => o.r;")]
    // The row in an object or an array that is kept.
    [InlineData("globalThis.o = { r: row }; return 1;")]
    [InlineData("globalThis.a = [row]; return 1;")]
    [InlineData("const o = {}; o.r = row; globalThis.k = o; return 1;")]
    [InlineData("const o = {}; globalThis.k = o; o.r = row; return 1;")]
    // Destructuring towards the outside, and a local alias that escapes later.
    [InlineData("({ r: globalThis.r } = { r: row }); return 1;")]
    [InlineData("const r = row; globalThis.k = r; return 1;")]
    [InlineData("[row].forEach(r => { globalThis.k = r; }); return 1;")]
    [InlineData("try { throw row; } catch (e) { globalThis.k = e; } return 1;")]
    // Changes that land on the view itself rather than on its row.
    [InlineData("row[Symbol.for('k')] = 1; return 1;")]
    [InlineData("Object.defineProperty(row, 'x', { value: 1 }); return 1;")]
    // Forms the analysis does not read.
    [InlineData("return eval('row.A');")]
    [InlineData("class A {} return 1;")]
    [InlineData("return (function* () { yield row; })();")]
    [InlineData("return (async () => row)();")]
    [InlineData("return row.;")]
    // Forms that keep nothing, which the analysis cannot tell apart from ones that do.
    [InlineData("row.prev = globalThis.prevA; return [row];")]
    [InlineData("const h = { id(x) { return x; } }; return h.id(row).A;")]
    public void A_Script_That_May_Keep_Its_Row_Gets_A_View_Per_Row(string body)
        => JsRowRetention.CanShareView($"function(row) {{ {body} }}", JsRowArgument.Row).Should().BeFalse();

    [Theory]
    [InlineData("globalThis.w = rows; return rows;")]
    [InlineData("globalThis.last = rows[rows.length - 1]; return rows;")]
    [InlineData("[globalThis.first] = rows; return rows;")]
    [InlineData("globalThis.acc = (globalThis.acc || []).concat(rows); return rows;")]
    [InlineData("const p = globalThis.__p; globalThis.__p = rows[0]; return rows;")]
    // A change to the array, which the next window would receive.
    [InlineData("rows.push({ V: 0 }); return rows;")]
    [InlineData("rows.sort((a, b) => a.V - b.V); return rows;")]
    [InlineData("rows.reverse(); return rows;")]
    [InlineData("rows.length = 0; return rows;")]
    [InlineData("rows[0] = { V: 0 }; return rows;")]
    [InlineData("rows.map((r, i, all) => all.pop()); return rows;")]
    public void A_Window_Script_That_May_Keep_A_Row_Or_Change_The_Array_Gets_Its_Own(string body)
        => JsRowRetention.CanShareView($"function(rows) {{ {body} }}", JsRowArgument.Rows).Should().BeFalse();
}

using Jint;

namespace DtPipe.Transformers.Services;

/// <summary>
/// Freezes an engine's realm when the engine is made: the global object and every object reachable from
/// it or only through syntax (the iterator and generator prototypes, <c>%ThrowTypeError%</c>), each
/// property read-only and each object closed to new properties. A script then keeps nothing from one
/// call to the next in the realm: a write to a global, a built-in or a prototype throws a
/// <c>TypeError</c> in the engine's strict mode. The legacy <c>RegExp</c> statics (<c>RegExp.$1</c>,
/// <c>RegExp.input</c>…) are removed first: they read and write state the engine keeps outside any
/// property, which freezing cannot close.
/// </summary>
/// <remarks>
/// What the freeze does not reach: the objects a transformer hands a script (its row views refuse to
/// lose their extensibility and take their prototype back at each row, a window gets a new array each
/// call), a binding declared at the top level of a script the engine evaluates, and the engine's job
/// queue (the transformers run it at the end of each call: <c>JsEngineExtensions.CallAndSettle</c>).
/// <c>JsRealmFreezeTests</c> holds each of them.
/// </remarks>
internal static class JsRealmFreeze
{
    /// <summary>
    /// Walks the realm depth first and freezes each object once its properties have been listed. It declares
    /// nothing at the top level, so the global object it freezes holds only Jint's own bindings.
    /// </summary>
    private const string FreezeScript = """
        (function () {
            'use strict';
            const roots = [
                globalThis,
                Object.getPrototypeOf(function* () {}),
                Object.getPrototypeOf(async function () {}),
                Object.getPrototypeOf(async function* () {}),
                Object.getPrototypeOf(Object.getPrototypeOf((function* () {})())),
                Object.getPrototypeOf(Object.getPrototypeOf((async function* () {})())),
                Object.getPrototypeOf([][Symbol.iterator]()),
                Object.getPrototypeOf(''[Symbol.iterator]()),
                Object.getPrototypeOf(new Map()[Symbol.iterator]()),
                Object.getPrototypeOf(new Set()[Symbol.iterator]()),
                Object.getPrototypeOf(/a/[Symbol.matchAll]('a')),
                Object.getOwnPropertyDescriptor(arguments, 'callee').get,
            ];
            // The two iterator prototypes no global names: a helper's, and the wrapper Iterator.from makes
            // around an iterator that does not inherit from Iterator.prototype.
            if (typeof Iterator === 'function') {
                if (typeof Iterator.prototype.map === 'function') {
                    roots.push(Object.getPrototypeOf([].values().map(x => x)));
                }
                roots.push(Object.getPrototypeOf(Iterator.from({ next() { return { done: true }; } })));
            }
            if (typeof AsyncIterator === 'function') {
                roots.push(Object.getPrototypeOf(AsyncIterator.from([1])));
                roots.push(Object.getPrototypeOf(AsyncIterator.from({ next() { return Promise.resolve({ done: true }); } })));
                if (typeof AsyncIterator.prototype.map === 'function') {
                    roots.push(Object.getPrototypeOf((async function* () {})().map(x => x)));
                }
            }
            delete globalThis.ShadowRealm;
            for (const key of ['input', '$_', 'lastMatch', '$&', 'lastParen', '$+', 'leftContext', '$`',
                'rightContext', "$'", '$1', '$2', '$3', '$4', '$5', '$6', '$7', '$8', '$9']) {
                delete RegExp[key];
            }
            const seen = new Set();
            const pending = roots.slice();
            while (pending.length > 0) {
                const o = pending.pop();
                if (o === null || (typeof o !== 'object' && typeof o !== 'function') || seen.has(o)) continue;
                seen.add(o);
                for (const key of Reflect.ownKeys(o)) {
                    const d = Reflect.getOwnPropertyDescriptor(o, key);
                    if ('value' in d) pending.push(d.value);
                    else pending.push(d.get, d.set);
                }
                pending.push(Reflect.getPrototypeOf(o));
                Object.freeze(o);
            }
        })();
        """;

    public static void Apply(Engine engine) => engine.Execute(FreezeScript);
}

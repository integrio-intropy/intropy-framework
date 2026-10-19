using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// Applies the component's claim-check configuration to a pipeline the framework composed.
/// </summary>
/// <remarks>
/// It runs after the component's own configuration lambda, in the two places the hosting layer owns
/// a builder — the registered pipeline and each declared route — so neither a component with one
/// pipeline nor one with several routes has to mention the claim check. It is also the only thing
/// that can configure one: <see cref="ExtractorBuilder{TInput,TOutput,TCtx}.WithClaimCheck"/> is
/// internal, so whether a payload is claim-checked is a deployment decision and nothing else.
/// </remarks>
internal static class ExtractorClaimCheck
{
    /// <summary>Configures <paramref name="builder"/>'s claim check from the registered store and
    /// the component's options, or leaves it alone when there is no store.</summary>
    /// <param name="builder">The pipeline being composed.</param>
    /// <param name="scope">The provider the store and options are resolved from.</param>
    /// <returns>The builder, for chaining into <c>Build()</c>.</returns>
    /// <exception cref="InvalidOperationException">A store is registered and the options do not
    /// describe a usable policy — in <see cref="ClaimCheckMode.Auto"/> with no threshold.</exception>
    internal static ExtractorBuilder<TInput, TOutput, TCtx> Apply<TInput, TOutput, TCtx>(
        ExtractorBuilder<TInput, TOutput, TCtx> builder, IServiceProvider scope) where TCtx : Context
    {
        var store = scope.GetKeyedService<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey);
        if (store is null)
            return builder;

        // Nothing to reconcile and nothing to clear: a pipeline cannot configure a claim check of
        // its own, so Never simply means none is added. That is what keeps the mode a rollback an
        // operator can reach for — there is no second control surface to disagree with it.
        var options = scope.GetRequiredService<ExtractorOptions>();
        if (options.ClaimCheckPolicy() is { } policy)
            return builder.WithClaimCheck(store, policy);

        // Worth one line, because the alternative is an operator wondering why the store they
        // registered stays empty.
        scope.GetRequiredService<ILoggerFactory>()
            .CreateLogger(typeof(ClaimStoreServiceCollectionExtensions))
            .LogInformation("Claim store registered; claim check mode {Mode}: publishing every payload inline",
                options.ClaimCheckMode);
        return builder;
    }
}

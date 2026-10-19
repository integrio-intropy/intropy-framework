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
/// pipeline nor one with several routes has to mention the claim check. A component that registers a
/// prebuilt <see cref="Extractor{TInput,TOutput,TCtx}"/> itself calls
/// <see cref="ExtractorBuilder{TInput,TOutput,TCtx}.WithClaimCheck"/> directly, the same way it
/// already supplies its own sender.
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
        var options = scope.GetRequiredService<ExtractorOptions>();
        var store = scope.GetKeyedService<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey);

        // The mode is read before the store, and Never clears rather than merely declining to add:
        // it is the documented rollback, so it has to stop new claims whatever configured them —
        // including a pipeline that called WithClaimCheck itself, with a store of its own that was
        // never registered here. Declining would leave exactly that component claim-checking while
        // its configuration said otherwise.
        if (options.ClaimCheckMode == ClaimCheckMode.Never)
        {
            // Worth one line, because the alternative is an operator wondering why the store they
            // registered stays empty.
            if (store is not null)
                scope.GetRequiredService<ILoggerFactory>()
                    .CreateLogger(typeof(ClaimStoreServiceCollectionExtensions))
                    .LogInformation(
                        "Claim store registered; claim check mode {Mode}: publishing every payload inline",
                        options.ClaimCheckMode);
            return builder.WithoutClaimCheck();
        }

        if (store is null)
            return builder;

        // Non-null after the Never guard above; a missing threshold throws here, naming the setting.
        return builder.WithClaimCheck(store, options.ClaimCheckPolicy()!);
    }
}

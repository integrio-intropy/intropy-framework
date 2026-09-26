using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.Extractor;

internal static class ExtractorCompositionValidation
{
    /// <summary>Resolves the composed pipeline in a fresh scope without executing it, as the job
    /// does before listing any file.</summary>
    internal static async ValueTask ValidateExtractorCompositionAsync<TInput, TOutput, TCtx>(
        this IServiceProvider provider) where TCtx : Context
    {
        await using var scope = provider.CreateAsyncScope();
        _ = scope.ServiceProvider.GetRequiredService<Extractor<TInput, TOutput, TCtx>>();
    }
}

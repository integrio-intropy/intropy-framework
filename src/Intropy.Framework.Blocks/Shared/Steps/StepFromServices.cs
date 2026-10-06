using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Blocks.Shared.Steps;

/// <summary>
/// Resolves a pipeline step from the service provider the pipeline is being built in. This is the
/// one rule behind the builders' services-resolving configuration members:
/// <c>WithSenderFromServices()</c> and the generic <c>WithDeserializer&lt;TStep&gt;()</c>-style
/// overloads on <see cref="Extractor.ExtractorBuilder{TInput, TOutput, TCtx}"/> and
/// <see cref="Loader.LoaderBuilder{TInput, TOutput, TCtx}"/>.
/// </summary>
internal static class StepFromServices
{
    /// <summary>Resolves <typeparamref name="TStep"/> from the service provider, or throws the
    /// missing-dependency error the builders throw for their own dependencies: the step's name and
    /// how to register it.</summary>
    /// <param name="serviceProvider">The provider the pipeline is being built in — the file's or
    /// the message's own scope for a hosted pipeline.</param>
    /// <typeparam name="TStep">The concrete step type to resolve. Register it against this type,
    /// e.g. <c>services.AddScoped&lt;MyDeserializer&gt;()</c>, not against a base class.</typeparam>
    /// <returns>The resolved step.</returns>
    /// <exception cref="InvalidOperationException">No <typeparamref name="TStep"/> is registered
    /// in the service provider.</exception>
    internal static TStep Resolve<TStep>(IServiceProvider serviceProvider) where TStep : class =>
        serviceProvider.GetService<TStep>() ?? throw new InvalidOperationException(
            $"{typeof(TStep).Name} is not registered in the service provider. " +
            $"Please register it using services.AddScoped<{typeof(TStep).Name}>() (or AddSingleton) " +
            "so the pipeline can resolve it when it is built.");
}

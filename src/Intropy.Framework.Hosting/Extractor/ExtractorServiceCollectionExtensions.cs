using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.RunToCompletion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Extractor;

/// <summary>
/// Registers a file-driven extractor: the pipeline, built per file with
/// <see cref="ExtractorBuilder{TInput,TOutput,TCtx}"/>, and the job that runs each source file
/// through it, hosted by the run-to-completion runner.
/// </summary>
public static class ExtractorServiceCollectionExtensions
{
    /// <summary>
    /// Registers one extractor component for file-sweep processing hosted by
    /// <see cref="RunToCompletionRunner"/>. Only one extractor component may be registered per
    /// service provider.
    /// </summary>
    /// <remarks>
    /// The pipeline is built in each file's own scope, so steps resolved from the provider passed to
    /// <paramref name="configurePipeline"/> may be scoped. Before any file is listed, the job builds
    /// the pipeline once, so a missing registration fails the run once. Caller-owned: the component
    /// identity (<c>AddIntropyFramework</c>), logging, <c>DaprClient</c>, the platform-service
    /// clients, the process steps, and the source port (<c>AddSourcePort</c>).
    /// </remarks>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">The published payload.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configurePipeline">Configures the pipeline builder, given the file's scope.</param>
    /// <param name="contextFactory">Creates the context for each source file.</param>
    /// <param name="configureJob">Optional runner settings: the job name (default: the component
    /// name) and the sidecar timeouts.</param>
    public static IServiceCollection AddExtractor<TInput, TOutput, TCtx>(
        this IServiceCollection services,
        Func<ExtractorBuilder<TInput, TOutput, TCtx>, IServiceProvider, ExtractorBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory,
        Action<RunToCompletionOptions>? configureJob = null) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        if (services.Any(d => d.ServiceType == typeof(Registration)))
            throw new InvalidOperationException("Only one extractor component may be registered per service provider.");

        services.AddSingleton(new Registration());
        services.AddScoped(provider => BuildPipeline(provider, configurePipeline));
        services.AddSingleton(provider => new ExtractorJob<TInput, TOutput, TCtx>(provider,
            provider.GetRequiredService<FrameworkOptions>(), contextFactory,
            provider.GetRequiredService<ILoggerFactory>()));
        services.AddRunToCompletionJob<ExtractorJob<TInput, TOutput, TCtx>>(configureJob);
        return services;
    }

    private static Extractor<TInput, TOutput, TCtx> BuildPipeline<TInput, TOutput, TCtx>(IServiceProvider provider,
        Func<ExtractorBuilder<TInput, TOutput, TCtx>, IServiceProvider, ExtractorBuilder<TInput, TOutput, TCtx>> configurePipeline)
        where TCtx : Context
    {
        var componentName = provider.GetService<FrameworkOptions>()?.ComponentName ??
            throw new InvalidOperationException(
                "Extractor composition failed: no component identity is registered. Call AddIntropyFramework.");
        try
        {
            var builder = ExtractorBuilder<TInput, TOutput, TCtx>.Create($"{componentName}.Process", provider);
            return configurePipeline(builder, provider).Build();
        }
        catch (InvalidOperationException error)
        {
            throw new InvalidOperationException($"Extractor '{componentName}' composition failed: {error.Message}", error);
        }
    }

    private sealed class Registration;
}

using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.Configuration;
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
    /// <see cref="JobRunner"/>. Only one extractor component may be registered per
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
        Action<JobOptions>? configureJob = null) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ArgumentNullException.ThrowIfNull(contextFactory);
        ComponentRegistration.EnsureNoOther(services, "extractor");
        return AddExtractorCore(services, configurePipeline, contextFactory, configureJob);
    }

    /// <summary>
    /// Registers one extractor component whose pipelines use the base <see cref="Context"/>. See
    /// <see cref="AddExtractor{TInput,TOutput,TCtx}(IServiceCollection,
    /// Func{ExtractorBuilder{TInput,TOutput,TCtx}, IServiceProvider, ExtractorBuilder{TInput,TOutput,TCtx}},
    /// ContextFactory{TCtx}, Action{JobOptions}?)"/>; the context factory — plain
    /// <see cref="Context"/> records for each source file — is provided for you.
    /// </summary>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">The published payload.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configurePipeline">Configures the pipeline builder, given the file's scope.</param>
    /// <param name="configureJob">Optional runner settings: the job name (default: the component
    /// name) and the sidecar timeouts.</param>
    public static IServiceCollection AddExtractor<TInput, TOutput>(
        this IServiceCollection services,
        Func<ExtractorBuilder<TInput, TOutput, Context>, IServiceProvider, ExtractorBuilder<TInput, TOutput, Context>> configurePipeline,
        Action<JobOptions>? configureJob = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurePipeline);
        ComponentRegistration.EnsureNoOther(services, "extractor");
        return AddExtractorCore(services, configurePipeline,
            static (metadata, isRetry) => new Context(metadata, isRetry), configureJob);
    }

    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">The published payload.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The section holding the extractor's values — its
    /// <c>SourcePort</c> key names the port to sweep. Each value only sets what the definition does
    /// not; the factory's definition wins for everything it sets.</param>
    /// <param name="factory">Builds the definition the code owns — the pipeline, the context
    /// factory — and overrides anything configuration set. Leave a member unset (null) to let the
    /// configuration provide it.</param>
    /// <returns>The service collection, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The definition is incomplete after both
    /// configuration and the factory have run, or an extractor is already registered.</exception>
    public static IServiceCollection AddExtractor<TInput, TOutput, TCtx>(this IServiceCollection services,
        IConfiguration configuration, Func<ExtractorDefinition<TInput, TOutput, TCtx>> factory)
        where TCtx : Context =>
        services.AddExtractor(ExtractorDefinition<TInput, TOutput, TCtx>.BoundTo(configuration, factory));

    /// <summary>
    /// Registers one extractor component described by a <paramref name="definition"/>: the pipeline,
    /// the source port, the context factory and the runner settings in one object, validated at
    /// registration so a misconfigured component fails at startup with the member that is wrong —
    /// not on the first file. See <see cref="ExtractorDefinition{TInput,TOutput,TCtx}"/>.
    /// </summary>
    /// <remarks>
    /// The pipeline is built in each file's own scope, so steps resolved from the provider passed to
    /// the definition's <see cref="ExtractorDefinition{TInput,TOutput,TCtx}.Pipeline"/> may be
    /// scoped. Before any file is listed, the job builds the pipeline once, so a missing
    /// registration fails the run once. Caller-owned: the component identity
    /// (<c>AddIntropyFramework</c>), logging, <c>DaprClient</c>, and the platform-service clients.
    /// Only one extractor component may be registered per service provider.
    /// </remarks>
    /// <typeparam name="TInput">The deserialized input.</typeparam>
    /// <typeparam name="TOutput">The published payload.</typeparam>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="definition">The extractor description, validated at registration.</param>
    /// <exception cref="InvalidOperationException">The definition is incomplete, or an extractor is
    /// already registered.</exception>
    public static IServiceCollection AddExtractor<TInput, TOutput, TCtx>(
        this IServiceCollection services, ExtractorDefinition<TInput, TOutput, TCtx> definition) where TCtx : Context
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(definition);
        // Validate everything before any registration: a rejected definition leaves no marker behind.
        var pipeline = definition.Validate();
        var contextFactory = definition.ResolveContextFactory();
        ComponentRegistration.EnsureNoOther(services, "extractor");
        if (definition.SourcePort is { } port)
            services.AddSourcePort(port, definition.Completion);
        return AddExtractorCore(services, pipeline, contextFactory, definition.ConfigureJob);
    }

    private static IServiceCollection AddExtractorCore<TInput, TOutput, TCtx>(
        IServiceCollection services,
        Func<ExtractorBuilder<TInput, TOutput, TCtx>, IServiceProvider, ExtractorBuilder<TInput, TOutput, TCtx>> configurePipeline,
        ContextFactory<TCtx> contextFactory,
        Action<JobOptions>? configureJob) where TCtx : Context
    {
        services.AddScoped(provider => BuildPipeline(provider, configurePipeline));
        services.AddSingleton(provider => new ExtractorJob<TInput, TOutput, TCtx>(provider,
            provider.GetRequiredService<FrameworkOptions>(), contextFactory,
            provider.GetRequiredService<ILoggerFactory>()));
        services.AddJob<ExtractorJob<TInput, TOutput, TCtx>>(configureJob);
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
}

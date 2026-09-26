using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.Sweep;
using Intropy.Framework.Hosting.RunToCompletion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// A test extractor described by its step types and selectors, so the tests can vary one step
/// (<c>with { Validator = ... }</c>) and register it the way a component would: steps in DI and
/// the pipeline through <see cref="ExtractorServiceCollectionExtensions.AddExtractor{TInput,TOutput,TCtx}"/>.
/// </summary>
internal sealed record TestExtractor<TInput, TOutput, TCtx> where TCtx : Context
{
    public string SourcePort { get; init; } = ExtractorSweepTestProcess.SourceKey;
    public SweepCompletion Completion { get; init; } = SweepCompletion.Delete;
    public required Type Deserializer { get; init; }
    public required Type Validator { get; init; }
    public required Type Transformer { get; init; }
    public IReadOnlyList<Type> Enrichments { get; init; } = [];
    public required Func<TInput, TCtx, string> InputIdentity { get; init; }
    public required Func<TInput, TCtx, DateTimeOffset> InputDate { get; init; }
    public Func<TInput, string>? InputHash { get; init; }
    public required Func<TOutput, string> OutputSubject { get; init; }
    public required Func<TOutput, DateTimeOffset> OutputTime { get; init; }
    public required Func<TCtx, string> IncidentIdentity { get; init; }
    public required Func<TCtx, string> IncidentSubject { get; init; }
    public required ContextFactory<TCtx> ContextFactory { get; init; }

    /// <summary>Registers the steps (existing registrations win), a default Dapr publisher, and
    /// the extractor itself.</summary>
    public IServiceCollection Register(IServiceCollection services, Action<RunToCompletionOptions>? configureJob = null)
    {
        services.AddSingleton(this);
        foreach (var step in (Type[])[Deserializer, Validator, Transformer, .. Enrichments])
            services.TryAdd(ServiceDescriptor.Scoped(step, step));
        services.TryAddSingleton<SendStep<TCtx>>(provider => new DaprTopicPublisher<TCtx>(
            provider.GetRequiredService<DaprClient>(), "events", "orders.accepted", new Uri("urn:example:orders"),
            "orders.accepted"));
        // The tests register the source adapter themselves (a fake), so only the port is declared.
        services.AddSourcePort(SourcePort, Completion);
        return services.AddExtractor<TInput, TOutput, TCtx>(Configure, ContextFactory, configureJob);
    }

    /// <summary>A job over this extractor, for tests that drive the job without the runner.</summary>
    public ExtractorJob<TInput, TOutput, TCtx> CreateJob(IServiceProvider provider, ILoggerFactory loggerFactory) =>
        new(provider, provider.GetRequiredService<FrameworkOptions>(), ContextFactory, loggerFactory);

    private ExtractorBuilder<TInput, TOutput, TCtx> Configure(ExtractorBuilder<TInput, TOutput, TCtx> builder,
        IServiceProvider provider)
    {
        builder
            .WithDeserializer((DeserializeStep<TInput, TCtx>)provider.GetRequiredService(Deserializer))
            .WithValidator((ValidateStep<TInput, TCtx>)provider.GetRequiredService(Validator))
            .WithIdempotency(InputIdentity, InputDate, InputHash)
            .WithTransformer((TransformStep<TInput, TOutput, TCtx>)provider.GetRequiredService(Transformer))
            .WithSerializer(new CloudEventSerializeStep<TOutput, TCtx>(OutputSubject, OutputTime))
            .WithSenderFromServices()
            .WithBusinessIncidents(IncidentIdentity, IncidentSubject);
        foreach (var enrichment in Enrichments)
            builder.WithExtractor((ExtractStep<TInput, TCtx>)provider.GetRequiredService(enrichment));
        return builder;
    }
}

internal static class TestExtractorServiceCollectionExtensions
{
    /// <summary>Registers <paramref name="extractor"/> as a component would, with optional
    /// runner settings.</summary>
    internal static IServiceCollection AddExtractor<TInput, TOutput, TCtx>(this IServiceCollection services,
        TestExtractor<TInput, TOutput, TCtx> extractor, Action<RunToCompletionOptions>? configureJob = null)
        where TCtx : Context => extractor.Register(services, configureJob);
}

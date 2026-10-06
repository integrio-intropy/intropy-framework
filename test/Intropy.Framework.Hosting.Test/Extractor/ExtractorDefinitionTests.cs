using Dapr.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.Extractor;

/// <summary>
/// The <see cref="ExtractorDefinition{TInput,TOutput,TCtx}"/> registration: validated at
/// registration (missing pipeline, missing context factory) so misconfiguration fails at startup
/// with the member that is wrong, registering equivalently to the lambda overload, registering the
/// declared source port, flowing the runner settings, and still guarded by the
/// one-extractor-per-provider rule.
/// </summary>
public class ExtractorDefinitionTests
{
    [Fact]
    public void MissingPipeline_FailsAtRegistration()
    {
        var services = new ServiceCollection();

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddExtractor(new ExtractorDefinition<int, int, Context> { Pipeline = null! }));

        Assert.Contains("Extractor composition failed: Pipeline", error.Message);
    }

    [Fact]
    public void DerivedContextWithoutFactory_FailsAtRegistration()
    {
        var services = new ServiceCollection();

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddExtractor(new ExtractorDefinition<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
                ExtractorTestProcess.TestContext> { Pipeline = (_, _) => null! }));

        // Derived contexts have no default factory; the error says so and names the member.
        Assert.Contains("Extractor composition failed: ContextFactory", error.Message);
        Assert.Contains(nameof(ExtractorTestProcess.TestContext), error.Message);
    }

    [Fact]
    public void RegistersEquivalentlyToTheLambdaOverload()
    {
        Action<JobOptions>? configureJob = job => job.JobName = "orders";
        ContextFactory<Context> contextFactory = (metadata, isRetry) => new Context(metadata, isRetry);
        var pipeline = (ExtractorBuilder<int, int, Context> builder, IServiceProvider _) => builder;

        var byLambda = BaseServices();
        byLambda.AddExtractor<int, int, Context>(pipeline, contextFactory, configureJob);
        var byDefinition = BaseServices();
        byDefinition.AddExtractor(new ExtractorDefinition<int, int, Context>
        {
            Pipeline = pipeline,
            ContextFactory = contextFactory,
            ConfigureJob = configureJob,
        });

        // Same registrations, in the same order, with the same lifetimes — the definition is the
        // lambda overload's parameters plus validated sugar at registration, not a second model.
        Assert.Equal(
            byLambda.Select(d => (d.ServiceType, d.Lifetime)).ToArray(),
            byDefinition.Select(d => (d.ServiceType, d.Lifetime)).ToArray());
    }

    [Fact]
    public async Task BaseContextPipeline_ComposesWithoutACustomContextFactory()
    {
        // The non-generic-context overload: no context factory passed, plain Context records.
        var services = BaseServices();
        services.AddSingleton<PassThroughDeserializer>();
        services.AddSingleton<AcceptAllValidator>();
        services.AddSingleton<IdentityTransformer>();
        services.AddSingleton<BuiltInSerializer>();
        services.AddExtractor<int, int>((ExtractorBuilder<int, int, Context> builder, IServiceProvider _) => builder
            .WithDeserializer<PassThroughDeserializer>()
            .WithIdempotency((input, _) => input.ToString(), (_, _) => DateTimeOffset.UnixEpoch)
            .WithValidator<AcceptAllValidator>()
            .WithTransformer<IdentityTransformer>()
            .WithSerializer<BuiltInSerializer>()
            .WithSenderFromServices()
            .WithBusinessIncidents(_ => "test", _ => "test"));

        await using var provider = services.BuildServiceProvider();

        // The composed pipeline resolves in a scope, on the base Context.
        var extractor = provider.CreateScope().ServiceProvider
            .GetRequiredService<Extractor<int, int, Context>>();
        Assert.NotNull(extractor);
    }

    [Fact]
    public async Task DeclaredSourcePort_IsRegistered()
    {
        var services = BaseServices();
        var completion = FileCompletion.Delete;

        services.AddExtractor(new ExtractorDefinition<int, int, Context>
        {
            SourcePort = "orders-inbox",
            Completion = completion,
            Pipeline = (_, _) => null!,
        });

        await using var provider = services.BuildServiceProvider();
        var sourcePort = provider.GetRequiredService<SourcePort>();
        Assert.Equal("orders-inbox", sourcePort.Name);
        Assert.Same(completion, sourcePort.Completion);
    }

    [Fact]
    public async Task ConfigureJob_AppliesRunnerSettings()
    {
        var services = BaseServices();

        services.AddExtractor(new ExtractorDefinition<int, int, Context>
        {
            Pipeline = (_, _) => null!,
            ConfigureJob = job =>
            {
                job.JobName = "orders";
                job.SidecarTimeout = TimeSpan.FromSeconds(7);
            },
        });

        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<JobOptions>();
        Assert.Equal("orders", options.JobName);
        Assert.Equal(TimeSpan.FromSeconds(7), options.SidecarTimeout);
    }

    [Fact]
    public void SecondExtractor_IsRejected()
    {
        var services = BaseServices();
        services.AddExtractor(new ExtractorDefinition<int, int, Context> { Pipeline = (_, _) => null! });

        var error = Assert.Throws<InvalidOperationException>(() => services.AddExtractor(
            new ExtractorDefinition<int, int, Context> { Pipeline = (_, _) => null! }));

        Assert.Contains("Only one extractor component", error.Message);
    }

    [Fact]
    public void RejectedDefinition_LeavesNoMarkerBehind()
    {
        // A definition that fails validation must not count as the component: the corrected retry
        // registers normally instead of tripping the one-component-per-provider guard.
        var services = BaseServices();
        var error = Assert.Throws<InvalidOperationException>(() => services.AddExtractor(
            new ExtractorDefinition<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
                ExtractorTestProcess.TestContext> { Pipeline = (_, _) => null! }));
        Assert.Contains("ContextFactory", error.Message);

        services.AddExtractor(new ExtractorDefinition<ExtractorTestProcess.Input, ExtractorTestProcess.Output,
            ExtractorTestProcess.TestContext>
        {
            Pipeline = (_, _) => null!,
            ContextFactory = (_, _) => new ExtractorTestProcess.TestContext(),
        });
    }

    /// <summary>A deserializer for the base-Context pipeline: an int in its raw form.</summary>
    public sealed class PassThroughDeserializer : DeserializeStep<int, Context>
    {
        public override Task<(BusinessStepResult<int> Result, Context Context)> ExecuteAsync(string input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<int>, Context)>(
                (new BusinessStepResult<int>.Success(int.Parse(input)), context));
    }

    /// <summary>Accepts every input.</summary>
    public sealed class AcceptAllValidator : ValidateStep<int, Context>
    {
        public override Task<(BusinessStepResult<int> Result, Context Context)> ExecuteAsync(int input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<int>, Context)>(
                (new BusinessStepResult<int>.Success(input), context));
    }

    /// <summary>The output is the input.</summary>
    public sealed class IdentityTransformer : TransformStep<int, int, Context>
    {
        public override Task<(TechnicalStepResult<int> Result, Context Context)> ExecuteAsync(int input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<int>, Context)>(
                (new TechnicalStepResult<int>.Success(input), context));
    }

    /// <summary>The serializer, registered in DI by its base type so the generic overload resolves it.</summary>
    public sealed class BuiltInSerializer : SerializeStep<int, Context>
    {
        private readonly CloudEventSerializeStep<int, Context> _inner = new(
            output => $"{output}", _ => DateTimeOffset.UnixEpoch);

        public override Task<(TechnicalStepResult<CloudNative.CloudEvents.CloudEvent> Result, Context Context)>
            ExecuteAsync(int input, Context context, CancellationToken ct) => _inner.ExecuteAsync(input, context, ct);
    }

    private static ServiceCollection BaseServices()
    {
        var services = new ServiceCollection();
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "orders-extractor";
            options.ServiceNamespace = "example";
        });
        services.AddSingleton(Substitute.For<DaprClient>());
        services.AddSingleton(Substitute.For<IIdempotencyServiceClient>());
        services.AddSingleton(Substitute.For<IBusinessIncidentServiceClient>());
        services.AddSingleton<SendStep<Context>>(Substitute.For<SendStep<Context>>());
        services.AddLogging();
        return services;
    }
}

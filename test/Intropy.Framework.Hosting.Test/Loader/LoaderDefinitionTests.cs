using Dapr.Client;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Testing.Delivery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// The <see cref="LoaderDefinition{TInput,TOutput,TCtx}"/> registration: validated at registration
/// (missing pipeline, pub/sub and topic; out-of-range callback port) so misconfiguration fails at
/// startup with the member that is wrong, registering equivalently to the lambda overload, keeping
/// the definition's pub/sub and topic authoritative over <c>Configure</c>, and still guarded by the
/// one-loader-per-provider rule.
/// </summary>
public class LoaderDefinitionTests
{
    [Fact]
    public void MissingPipeline_FailsAtRegistration()
    {
        var services = new ServiceCollection();

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddLoader(new LoaderDefinition<int, int, Context>
            {
                PubSubName = LoaderHost.PubSub,
                TopicName = LoaderHost.Topic,
                Pipeline = null!,
            }));

        Assert.Contains("Loader composition failed: Pipeline", error.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void MissingPubSubName_FailsAtRegistration(string? pubSubName)
    {
        var services = new ServiceCollection();

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddLoader(new LoaderDefinition<int, int, Context>
            {
                PubSubName = pubSubName!,
                TopicName = LoaderHost.Topic,
                Pipeline = (_, _) => null!,
            }));

        Assert.Contains("Loader composition failed: PubSubName", error.Message);
    }

    [Fact]
    public void MissingTopicName_FailsAtRegistration()
    {
        var services = new ServiceCollection();

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddLoader(new LoaderDefinition<int, int, Context>
            {
                PubSubName = LoaderHost.PubSub,
                TopicName = "",
                Pipeline = (_, _) => null!,
            }));

        Assert.Contains("Loader composition failed: TopicName", error.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65536)]
    public void OutOfRangeCallbackPort_FailsAtRegistration(int port)
    {
        var services = new ServiceCollection();

        var error = Assert.Throws<ArgumentOutOfRangeException>(() =>
            services.AddLoader(new LoaderDefinition<int, int, Context>
            {
                PubSubName = LoaderHost.PubSub,
                TopicName = LoaderHost.Topic,
                Configure = options => options.CallbackPort = port,
                Pipeline = (_, _) => null!,
            }));

        Assert.Contains("LoaderOptions.CallbackPort must be a port number (1-65535)", error.Message);
    }

    [Fact]
    public void RegistersEquivalentlyToTheLambdaOverload()
    {
        var pipeline = (LoaderBuilder<int, int, Context> builder, IServiceProvider _) => builder;
        ContextFactory<Context> contextFactory = (metadata, isRetry) => new Context(metadata, isRetry);

        var byLambda = new ServiceCollection();
        byLambda.AddLoader(pipeline, contextFactory, options =>
        {
            options.PubSubName = LoaderHost.PubSub;
            options.TopicName = LoaderHost.Topic;
        });
        var byDefinition = new ServiceCollection();
        byDefinition.AddLoader(new LoaderDefinition<int, int, Context>
        {
            PubSubName = LoaderHost.PubSub,
            TopicName = LoaderHost.Topic,
            Pipeline = pipeline,
            ContextFactory = contextFactory,
        });

        // Same registrations, in the same order, with the same lifetimes — the definition is the
        // lambda overload's parameters plus validated sugar at registration, not a second model.
        Assert.Equal(
            byLambda.Select(d => (d.ServiceType, d.Lifetime)).ToArray(),
            byDefinition.Select(d => (d.ServiceType, d.Lifetime)).ToArray());
    }

    [Fact]
    public void PubSubNameAndTopicName_WinOverConfigure()
    {
        var services = new ServiceCollection();
        services.AddLoader(new LoaderDefinition<int, int, Context>
        {
            PubSubName = LoaderHost.PubSub,
            TopicName = LoaderHost.Topic,
            Configure = options =>
            {
                options.PubSubName = "other";
                options.TopicName = "other-topic";
                options.MaxMessageProcessingTime = TimeSpan.FromSeconds(90);
            },
            Pipeline = (_, _) => null!,
        });

        var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<MessageConsumerSettings>();
        // The definition's required members are authoritative; the tweaks around them flow through.
        Assert.Equal(LoaderHost.PubSub, settings.PubSubName);
        Assert.Equal(LoaderHost.Topic, settings.TopicName);
        Assert.Equal(TimeSpan.FromSeconds(90), provider.GetRequiredService<LoaderOptions>().MaxMessageProcessingTime);
    }

    [Fact]
    public void BaseContextPipeline_RegistersWithoutACustomContextFactory()
    {
        var services = new ServiceCollection();
        services.AddLoader(new LoaderDefinition<int, int, Context>
        {
            PubSubName = LoaderHost.PubSub,
            TopicName = LoaderHost.Topic,
            Pipeline = (_, _) => null!,
        });

        var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<LoaderOptions>());
        Assert.NotNull(provider.GetRequiredService<MessageConsumerSettings>());
    }

    [Fact]
    public void SecondLoader_IsRejected()
    {
        var services = new ServiceCollection();
        services.AddLoader(new LoaderDefinition<int, int, Context>
        {
            PubSubName = LoaderHost.PubSub,
            TopicName = LoaderHost.Topic,
            Pipeline = (_, _) => null!,
        });

        var error = Assert.Throws<InvalidOperationException>(() => services.AddLoader(
            new LoaderDefinition<int, int, Context>
            {
                PubSubName = LoaderHost.PubSub,
                TopicName = LoaderHost.Topic,
                Pipeline = (_, _) => null!,
            }));

        Assert.Contains("Only one loader", error.Message);
    }

    [Fact]
    public void RejectedLoaderDefinition_LeavesNoMarkerBehind()
    {
        // A definition that fails validation must not count as the loader: the corrected retry
        // registers normally instead of tripping the one-loader-per-provider guard.
        var services = new ServiceCollection();
        var error = Assert.Throws<InvalidOperationException>(() => services.AddLoader(
            new LoaderDefinition<int, int, Context>
            {
                PubSubName = LoaderHost.PubSub,
                TopicName = "",
                Pipeline = (_, _) => null!,
            }));
        Assert.Contains("TopicName", error.Message);

        services.AddLoader(new LoaderDefinition<int, int, Context>
        {
            PubSubName = LoaderHost.PubSub,
            TopicName = LoaderHost.Topic,
            Pipeline = (_, _) => null!,
        });
    }
}

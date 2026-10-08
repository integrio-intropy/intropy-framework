using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// The loader's <see cref="LoaderOptions.Subscription"/> composition and the configuration-based
/// registration: the composed shape and the class's own members are one value each, the
/// configuration section fills what the definition's delegate leaves unset, and code wins over
/// configuration where both speak.
/// </summary>
public class LoaderSubscriptionTests
{
    [Fact]
    public void ComposedSubscription_AndOwnMembers_AreOneValueEach()
    {
        // Delegation, both directions: a value set through either shape reads back through the other.
        var options = new LoaderOptions { PubSubName = "pubsub", TopicName = "orders" };
        options.Subscription.MaxMessageProcessingTime = TimeSpan.FromSeconds(90);
        options.Subscription.CallbackPort = 9191;

        Assert.Equal("pubsub", options.Subscription.PubSubName);
        Assert.Equal("orders", options.Subscription.TopicName);
        Assert.Equal(TimeSpan.FromSeconds(90), options.MaxMessageProcessingTime);
        Assert.Equal(9191, options.CallbackPort);

        options.PubSubName = "other-pubsub";
        options.MaxMessageProcessingTime = TimeSpan.FromMinutes(2);

        Assert.Equal("other-pubsub", options.Subscription.PubSubName);
        Assert.Equal(TimeSpan.FromMinutes(2), options.Subscription.MaxMessageProcessingTime);
    }

    [Fact]
    public void DefinitionConfigure_ThroughTheComposedShape_FlowsThrough()
    {
        // The definition's Configure receives the real options: shaping the subscription through the
        // composed object is the same as shaping it through the loader's own members.
        var services = new ServiceCollection();
        services.AddLoader(new LoaderDefinition<int, int, Context>
        {
            PubSubName = LoaderHost.PubSub,
            TopicName = LoaderHost.Topic,
            Configure = options =>
            {
                options.Subscription.MaxMessageProcessingTime = TimeSpan.FromSeconds(75);
                options.Subscription.CallbackPort = 9090;
            },
            Pipeline = (_, _) => null!,
        });

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<LoaderOptions>();
        Assert.Equal(TimeSpan.FromSeconds(75), options.MaxMessageProcessingTime);
        Assert.Equal(TimeSpan.FromSeconds(75), options.Subscription.MaxMessageProcessingTime);
        Assert.Equal(9090, options.CallbackPort);
    }

    [Fact]
    public void Configuration_FillsTheSubscriptionTheDelegateLeavesUnset()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Loader:PubSubName"] = "config-pubsub",
            ["Loader:TopicName"] = "config-topic",
            ["Loader:MaxMessageProcessingTime"] = "00:02:00",
            ["Loader:ShutdownGracePeriod"] = "00:00:30",
            ["Loader:Unrouted"] = "Ack",
            ["Loader:CallbackPort"] = "9090",
        }).Build();

        services.AddLoader<int, int, Context>(configuration.GetSection("Loader"),
            () => new LoaderDefinition<int, int, Context>
            {
                PubSubName = null!, // filled from configuration
                TopicName = null!, // filled from configuration
                Pipeline = (_, _) => null!,
            });

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<LoaderOptions>();
        Assert.Equal("config-pubsub", options.Subscription.PubSubName);
        Assert.Equal("config-topic", options.Subscription.TopicName);
        Assert.Equal(TimeSpan.FromMinutes(2), options.MaxMessageProcessingTime);
        Assert.Equal(TimeSpan.FromSeconds(30), options.ShutdownGracePeriod);
        Assert.Equal(UnroutedPolicy.Ack, options.Unrouted);
        Assert.Equal(9090, options.CallbackPort);
        // The same values the subscription consumer registers with.
        var settings = provider.GetRequiredService<MessageConsumerSettings>();
        Assert.Equal("config-pubsub", settings.PubSubName);
        Assert.Equal("config-topic", settings.TopicName);
    }

    [Fact]
    public void Code_WinsOverConfiguration()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Loader:PubSubName"] = "config-pubsub",
            ["Loader:TopicName"] = "config-topic",
            ["Loader:MaxMessageProcessingTime"] = "00:02:00",
            ["Loader:ShutdownGracePeriod"] = "00:00:30",
        }).Build();

        services.AddLoader<int, int, Context>(configuration.GetSection("Loader"),
            () => new LoaderDefinition<int, int, Context>
            {
                PubSubName = "code-pubsub",
                TopicName = null!, // filled from configuration
                Configure = options => options.MaxMessageProcessingTime = TimeSpan.FromSeconds(50),
                Pipeline = (_, _) => null!,
            });

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<LoaderOptions>();
        // The definition's required members and its own Configure run after the section's values.
        Assert.Equal("code-pubsub", options.PubSubName);
        Assert.Equal("config-topic", options.TopicName);
        Assert.Equal(TimeSpan.FromSeconds(50), options.MaxMessageProcessingTime);
        // Configuration still carries what code does not speak to.
        Assert.Equal(TimeSpan.FromSeconds(30), options.ShutdownGracePeriod);
    }

    [Fact]
    public void ValueUnsetInConfigurationAndCode_StaysUnset()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Loader:TopicName"] = "config-topic",
        }).Build();

        services.AddLoader<int, int, Context>(configuration.GetSection("Loader"),
            () => new LoaderDefinition<int, int, Context>
            {
                // PubSubName: not set in code, not in configuration — any pub/sub is accepted.
                // TopicName: filled from configuration.
                Pipeline = (_, _) => null!,
            });

        var options = services.BuildServiceProvider().GetRequiredService<LoaderOptions>();
        Assert.Equal("", options.PubSubName);
        Assert.Equal("config-topic", options.TopicName);
    }

    [Fact]
    public void MalformedConfigurationValue_FailsAtRegistration()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Loader:PubSubName"] = "config-pubsub",
            ["Loader:TopicName"] = "config-topic",
            ["Loader:MaxMessageProcessingTime"] = "not-a-timespan",
        }).Build();

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddLoader<int, int, Context>(configuration.GetSection("Loader"),
                () => new LoaderDefinition<int, int, Context>
                {
                    PubSubName = null!, // filled from configuration
                    TopicName = null!, // filled from configuration
                    Pipeline = (_, _) => null!,
                }));

        Assert.Contains("MaxMessageProcessingTime", error.Message);
        Assert.Contains("not-a-timespan", error.Message);
    }
}

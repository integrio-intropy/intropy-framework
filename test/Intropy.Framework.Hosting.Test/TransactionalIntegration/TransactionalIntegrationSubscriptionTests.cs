using Dapr.Client;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>
/// The Transactional Integration's subscription shapes and registration guard: the legacy members
/// (<see cref="TransactionalIntegrationOptions.DaprPubSubName"/>,
/// <see cref="TransactionalIntegrationOptions.DaprTopicName"/>) and the composed
/// <see cref="TransactionalIntegrationOptions.Subscription"/> carry the same values either way,
/// disagreements fail at registration, and only one integration registers per provider.
/// </summary>
public class TransactionalIntegrationSubscriptionTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PubSubNameAndTopicName_AreTheSameValue_ThroughEitherShape(bool viaComposed)
    {
        // Whichever shape the value is set through, both shapes read it back after registration.
        var services = GetServices();

        services.AddTransactionalIntegration(options =>
        {
            if (viaComposed)
            {
                options.Subscription.PubSubName = "test-pubsub";
                options.Subscription.TopicName = "test-topic";
            }
            else
            {
                options.DaprPubSubName = "test-pubsub";
                options.DaprTopicName = "test-topic";
            }
        });

        var registered = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("test-pubsub", registered.DaprPubSubName);
        Assert.Equal("test-topic", registered.DaprTopicName);
        Assert.Equal("test-pubsub", registered.Subscription.PubSubName);
        Assert.Equal("test-topic", registered.Subscription.TopicName);
    }

    [Fact]
    public void ComposedPubSubName_SatisfiesTheRequiredPubSubAndTopicCheck()
    {
        // The pub/sub and topic are required once, not once per shape: setting them through the
        // composed Subscription is enough.
        var services = GetServices();

        services.AddTransactionalIntegration(options =>
        {
            options.Subscription.PubSubName = "test-pubsub";
            options.Subscription.TopicName = "test-topic";
        });

        var registered = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("test-pubsub", registered.DaprPubSubName);
        Assert.Equal("test-topic", registered.DaprTopicName);
    }

    [Fact]
    public void LegacyMembers_SettleOntoTheComposedSubscription()
    {
        var services = GetServices();

        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
            options.MaxMessageProcessingTime = TimeSpan.FromSeconds(20);
            options.CallbackPort = 9191;
        });

        var registered = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("test-pubsub", registered.Subscription.PubSubName);
        Assert.Equal("test-topic", registered.Subscription.TopicName);
        Assert.Equal(TimeSpan.FromSeconds(20), registered.Subscription.MaxMessageProcessingTime);
        Assert.Equal(9191, registered.Subscription.CallbackPort);
    }

    [Theory]
    [InlineData("DaprPubSubName")]
    [InlineData("DaprTopicName")]
    public void SameValueThroughBothShapes_FailsAtRegistration(string member)
    {
        // The two shapes are one value each; two different values for it is a contradiction, not a
        // configuration — it fails at registration with both members named.
        var services = GetServices();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
            if (member == "DaprPubSubName")
                options.Subscription.PubSubName = "other-pubsub";
            else
                options.Subscription.TopicName = "other-topic";
        }));

        Assert.Contains("disagree", error.Message);
        Assert.Contains("Subscription", error.Message);
        Assert.Contains(member, error.Message);
    }

    [Fact]
    public void SecondTransactionalIntegration_IsRejected()
    {
        // The one-component-per-provider guard rejects a second integration rather than silently
        // double-registering it.
        var services = GetServices();
        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "other-pubsub";
            options.DaprTopicName = "other-topic";
        }));

        Assert.Contains("Only one transactional integration component", error.Message);
    }

    [Fact]
    public void SecondTransactionalIntegration_ByDefinition_IsRejected()
    {
        var services = GetServices();
        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(
            new TransactionalIntegrationDefinition<Context>
            {
                DaprPubSubName = "other-pubsub",
                DaprTopicName = "other-topic",
            }));

        Assert.Contains("Only one transactional integration component", error.Message);
    }

    [Fact]
    public void RejectedRegistration_LeavesNoMarkerBehind()
    {
        // An invalid first registration must not count as the integration: the corrected retry
        // registers normally instead of tripping the one-component-per-provider guard.
        var services = GetServices();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(options =>
        {
            // DaprPubSubName intentionally not set — the registration is rejected.
            options.DaprTopicName = "test-topic";
        }));
        Assert.Equal("DaprPubSubName must be configured.", error.Message);

        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });
        var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredService<TransactionalIntegrationOptions>());
    }

    [Fact]
    public void SecondRegistration_OfAnInvalidFirstOne_FailsInTheSecondGuard()
    {
        // Even the second, invalid registration fails on the guard, not on its own validation:
        // the first registration marked the provider.
        var services = GetServices();
        services.AddTransactionalIntegration(options =>
        {
            options.DaprPubSubName = "test-pubsub";
            options.DaprTopicName = "test-topic";
        });

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTransactionalIntegration(options =>
        {
            // Neither shape sets the pub/sub — but the guard fires first.
        }));

        Assert.Contains("Only one transactional integration component", error.Message);
    }

    [Fact]
    public void Configuration_FillsTheOptionsTheDelegateLeavesUnset()
    {
        var services = GetServices();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TransactionalIntegration:DaprPubSubName"] = "config-pubsub",
            ["TransactionalIntegration:DaprTopicName"] = "config-topic",
            ["TransactionalIntegration:MaxMessageProcessingTime"] = "00:00:20",
            ["TransactionalIntegration:IdleTimeout"] = "00:00:10",
            ["TransactionalIntegration:PostIdleGracePeriod"] = "00:00:30",
            ["TransactionalIntegration:CallbackPort"] = "9191",
        }).Build();

        services.AddTransactionalIntegration<Context>(configuration.GetSection("TransactionalIntegration"),
            () => new TransactionalIntegrationDefinition<Context>
            {
                DaprPubSubName = null!, // filled from configuration
                DaprTopicName = null!, // filled from configuration
            });

        var registered = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("config-pubsub", registered.DaprPubSubName);
        Assert.Equal("config-topic", registered.DaprTopicName);
        Assert.Equal(TimeSpan.FromSeconds(20), registered.MaxMessageProcessingTime);
        Assert.Equal(TimeSpan.FromSeconds(10), registered.IdleTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), registered.PostIdleGracePeriod);
        Assert.Equal(9191, registered.CallbackPort);
    }

    [Fact]
    public void Configuration_ThroughTheComposedShape_SatisfiesTheRequiredChecks()
    {
        // A section written in the loader's spelling — PubSubName/TopicName — flows just the same.
        var services = GetServices();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TransactionalIntegration:PubSubName"] = "config-pubsub",
            ["TransactionalIntegration:TopicName"] = "config-topic",
        }).Build();

        services.AddTransactionalIntegration<Context>(configuration.GetSection("TransactionalIntegration"),
            () => new TransactionalIntegrationDefinition<Context>
            {
                DaprPubSubName = null!, // filled from configuration, through the composed shape's keys
                DaprTopicName = null!, // filled from configuration, through the composed shape's keys
            });

        var registered = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("config-pubsub", registered.DaprPubSubName);
        Assert.Equal("config-topic", registered.DaprTopicName);
        Assert.Equal("config-pubsub", registered.Subscription.PubSubName);
    }

    [Fact]
    public void Code_WinsOverConfiguration()
    {
        var services = GetServices();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TransactionalIntegration:DaprPubSubName"] = "config-pubsub",
            ["TransactionalIntegration:DaprTopicName"] = "config-topic",
            ["TransactionalIntegration:MaxMessageProcessingTime"] = "00:00:20",
        }).Build();

        services.AddTransactionalIntegration<Context>(configuration.GetSection("TransactionalIntegration"),
            () => new TransactionalIntegrationDefinition<Context>
            {
                DaprPubSubName = null!, // filled from configuration, then overridden in Configure
                DaprTopicName = null!, // filled from configuration
                Configure = options =>
                {
                    options.DaprPubSubName = "code-pubsub";
                    options.MaxMessageProcessingTime = TimeSpan.FromSeconds(50);
                },
            });

        var registered = services.BuildServiceProvider().GetRequiredService<TransactionalIntegrationOptions>();
        Assert.Equal("code-pubsub", registered.DaprPubSubName);
        Assert.Equal("config-topic", registered.DaprTopicName);
        Assert.Equal(TimeSpan.FromSeconds(50), registered.MaxMessageProcessingTime);
        // The composed shape settles onto the winning value.
        Assert.Equal("code-pubsub", registered.Subscription.PubSubName);
    }

    [Fact]
    public void MalformedConfigurationValue_FailsAtRegistration()
    {
        var services = GetServices();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TransactionalIntegration:DaprPubSubName"] = "config-pubsub",
            ["TransactionalIntegration:DaprTopicName"] = "config-topic",
            ["TransactionalIntegration:CallbackPort"] = "not-a-port",
        }).Build();

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddTransactionalIntegration<Context>(configuration.GetSection("TransactionalIntegration"),
                () => new TransactionalIntegrationDefinition<Context>
                {
                    DaprPubSubName = null!, // filled from configuration
                    DaprTopicName = null!, // filled from configuration
                }));

        Assert.Contains("CallbackPort", error.Message);
        Assert.Contains("not-a-port", error.Message);
    }

    [Fact]
    public void MissingRequiredValue_InConfigurationAndCode_FailsAtRegistration()
    {
        var services = GetServices();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TransactionalIntegration:DaprTopicName"] = "config-topic",
        }).Build();

        var error = Assert.Throws<InvalidOperationException>(() =>
            services.AddTransactionalIntegration<Context>(configuration.GetSection("TransactionalIntegration"),
                () => new TransactionalIntegrationDefinition<Context>
                {
                    DaprPubSubName = null!, // not set in code, not in configuration
                    DaprTopicName = null!, // filled from configuration
                }));

        Assert.Contains("DaprPubSubName", error.Message);
    }

    private static ServiceCollection GetServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_ => new DaprClientBuilder().Build());
        services.AddIntropyFramework(options =>
        {
            options.ComponentName = "orders-integration";
            options.ServiceNamespace = "example";
        });
        return services;
    }
}

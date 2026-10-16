using Dapr.Client;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Intropy.Framework.Testing.Adapters;
using Intropy.Framework.Testing.Topics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>
/// Whether the registration puts the claim check into the receive pipeline: a registered claim store
/// makes it possible, the mode decides whether it engages, and <see cref="ClaimCheckMode.Never"/>
/// leaves the step out entirely while the store stays registered and resolvable.
/// </summary>
public class ClaimCheckWiringTests
{
    private const string Component = "orders-integration";

    private static (IReceivePipeline<Context> Pipeline, InMemoryFileAdapter Claims, FakeEnqueueStep<Context> Topic)
        Build(ClaimCheckMode mode, bool withClaimStore = true) => Build(mode, new List<string>(), withClaimStore);

    private static (IReceivePipeline<Context> Pipeline, InMemoryFileAdapter Claims, FakeEnqueueStep<Context> Topic)
        Build(ClaimCheckMode mode, List<string> log, bool withClaimStore = true)
    {
        var options = new FrameworkOptions
        {
            ComponentName = Component,
            ServiceNamespace = "example",
            PubSubName = "order-flow-queue-pubsub",
            TopicName = $"internal-{Component}"
        };
        var claims = new InMemoryFileAdapter();
        var topic = new FakeEnqueueStep<Context>(options);

        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(new CapturingLoggerFactory(log));
        services.AddSingleton(_ => new DaprClientBuilder().Build());
        services.AddIntropyFramework(o =>
        {
            o.ComponentName = options.ComponentName;
            o.ServiceNamespace = options.ServiceNamespace;
            o.PubSubName = options.PubSubName;
            o.TopicName = options.TopicName;
        });
        // Registered before the integration, so it wins over the Dapr enqueuer's TryAdd.
        services.AddSingleton<EnqueueStep<Context>>(topic);
        if (withClaimStore)
            services.AddKeyedSingleton<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey, claims);
        services.AddTransactionalIntegration(o =>
        {
            o.ClaimCheckMode = mode;
            o.ClaimCheckThresholdBytes = 1024;
        });

        var provider = services.BuildServiceProvider();
        return (provider.GetRequiredService<IReceivePipeline<Context>>(), claims, topic);
    }

    private sealed class CapturingLoggerFactory(List<string> entries) : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(entries);

        public void Dispose() { }

        private sealed class CapturingLogger(List<string> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter) => entries.Add(formatter(state, exception));
        }
    }

    private static SourceItem Oversized()
    {
        var payload = new byte[8192];
        Array.Fill(payload, (byte)0x3B);
        return new SourceItem("order-1.json", payload);
    }

    [Fact]
    public async Task NeverMode_LeavesTheStepOut_SoAnOversizedPayloadStaysInline()
    {
        var (pipeline, claims, topic) = Build(ClaimCheckMode.Never);

        await pipeline.Execute(Oversized(), new Context([]), detachTrace: true, CancellationToken.None);

        // Nothing stored, and the envelope carries the payload rather than a pointer.
        Assert.Empty(claims.Files);
        var envelope = Assert.Single(topic.Captured).DecodeCloudEvent();
        Assert.Null(envelope[CloudEventExtensionKeys.DataRef]);
        Assert.NotNull(envelope.Data);
    }

    [Fact]
    public async Task AutoMode_WithTheSameOversizedPayload_DoesClaimCheckIt()
    {
        // The control for the test above: without it, "nothing stored" could pass for the wrong reason.
        var (pipeline, claims, topic) = Build(ClaimCheckMode.Auto);

        await pipeline.Execute(Oversized(), new Context([]), detachTrace: true, CancellationToken.None);

        var claim = Assert.Single(claims.Files);
        var envelope = Assert.Single(topic.Captured).DecodeCloudEvent();
        Assert.Equal(claim.Key, envelope[CloudEventExtensionKeys.DataRef]);
        Assert.Null(envelope.Data);
    }

    [Fact]
    public void NeverMode_SaysSoOnce_SoARollbackIsVisibleToAnOperator()
    {
        // The step leaves no per-message span in Never, so the one place this is observable is here.
        var neverLog = new List<string>();
        Build(ClaimCheckMode.Never, neverLog);
        var autoLog = new List<string>();
        Build(ClaimCheckMode.Auto, autoLog);

        Assert.Contains(neverLog, entry =>
            entry.Contains("resolving pointers, writing none", StringComparison.Ordinal) &&
            entry.Contains(nameof(ClaimCheckMode.Never), StringComparison.Ordinal));
        Assert.DoesNotContain(autoLog, entry =>
            entry.Contains("resolving pointers, writing none", StringComparison.Ordinal));
    }

    [Fact]
    public async Task WithoutAClaimStore_NothingIsClaimChecked_WhateverTheMode()
    {
        var (pipeline, claims, topic) = Build(ClaimCheckMode.Always, withClaimStore: false);

        await pipeline.Execute(Oversized(), new Context([]), detachTrace: true, CancellationToken.None);

        Assert.Empty(claims.Files);
        Assert.Null(Assert.Single(topic.Captured).DecodeCloudEvent()[CloudEventExtensionKeys.DataRef]);
    }
}

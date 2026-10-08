using System.Net;
using CloudNative.CloudEvents;
using Dapr.Client;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.Extractor.Steps;

/// <summary>
/// The extractor's service-invocation sender: only a success status counts as delivered. Anything
/// else is a technical failure, so the item is not completed and is retried instead of lost.
/// </summary>
public class DaprServiceInvokerTests
{
    [Theory]
    [InlineData(HttpStatusCode.OK)]
    [InlineData(HttpStatusCode.Accepted)]
    [InlineData(HttpStatusCode.NoContent)]
    public async Task ExecuteAsync_Succeeds_WhenTheServiceAnswersWithASuccessStatus(HttpStatusCode status)
    {
        var (result, _) = await Invoke(status);

        Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(result);
    }

    [Theory]
    [InlineData(HttpStatusCode.BadRequest)]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    public async Task ExecuteAsync_FailsTechnically_WhenTheServiceAnswersWithAnErrorStatus(HttpStatusCode status)
    {
        var (result, _) = await Invoke(status);

        var failure = Assert.IsType<TechnicalStepResult<CloudEvent>.Failure>(result);
        Assert.Contains($"'reconciler' answered {(int)status}", failure.Value.Description);
        var exception = Assert.IsType<HttpRequestException>(failure.Value.Exception);
        Assert.Equal(status, exception.StatusCode);
    }

    [Fact]
    public async Task ExecuteAsync_AppliesTheConfiguredType_WhenTheSerializeStepDidNotSetOne()
    {
        var (result, _) = await Invoke(HttpStatusCode.OK);

        var success = Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(result);
        Assert.Equal("com.test.customer.extracted", success.Value.Type);
    }

    [Fact]
    public async Task ExecuteAsync_KeepsTheTypeSetByTheSerializeStep_WhenPresent()
    {
        // A component-supplied type extractor wins over the configured fallback.
        var daprClient = Substitute.For<DaprClient>();
        daprClient.CreateInvokeMethodRequest(Arg.Any<HttpMethod>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns(_ => new HttpRequestMessage(HttpMethod.Post, "http://localhost:3500/v1.0/invoke/reconciler/method/ingest"));
        using var handler = new StatusHandler(HttpStatusCode.OK);
        using var httpClient = new HttpClient(handler);
        var invoker = new DaprServiceInvoker<Context>(daprClient, httpClient, "reconciler", new Uri("urn:test:crm"),
            "com.test.customer.extracted");
        var input = new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            Type = "io.intropy.orders.new",
            Subject = "order-42",
            Time = DateTimeOffset.UtcNow,
            DataContentType = "application/json",
            Data = "{}"
        };

        var (result, _) = await invoker.ExecuteAsync(input, new Context(new Dictionary<string, string>()),
            CancellationToken.None);

        var success = Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(result);
        Assert.Equal("io.intropy.orders.new", success.Value.Type);
    }

    private static async Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> Invoke(HttpStatusCode status)
    {
        var daprClient = Substitute.For<DaprClient>();
        daprClient.CreateInvokeMethodRequest(Arg.Any<HttpMethod>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns(_ => new HttpRequestMessage(HttpMethod.Post, "http://localhost:3500/v1.0/invoke/reconciler/method/ingest"));
        using var handler = new StatusHandler(status);
        using var httpClient = new HttpClient(handler);
        var invoker = new DaprServiceInvoker<Context>(daprClient, httpClient, "reconciler", new Uri("urn:test:crm"),
            "com.test.customer.extracted");

        return await invoker.ExecuteAsync(new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            Subject = "customer-42",
            Time = DateTimeOffset.UtcNow,
            DataContentType = "application/json",
            Data = "{}"
        }, new Context(new Dictionary<string, string>()), CancellationToken.None);
    }

    private sealed class StatusHandler(HttpStatusCode status) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(status));
    }
}

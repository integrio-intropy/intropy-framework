using System.Net;
using System.Text.Json;
using Dapr.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Extractor;
using Intropy.Framework.Hosting.Gateway;
using Intropy.Framework.Hosting.Test.Extractor;
using Intropy.Framework.Testing.Services;
using Intropy.Framework.Testing.Topics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Intropy.Framework.Hosting.Test.Gateway;

/// <summary>
/// The gateway's registration and startup contract, below the declarative component: the
/// definition and options are checked at registration, the pipeline is composed at startup, and
/// the gateway takes no request until its sidecar answers.
/// </summary>
public class GatewayEndpointTests
{
    private const string ItemBody = """{"Id":"item-1"}""";

    [Fact]
    public async Task Post_BeforeTheSidecarAnswers_AsksTheCallerToRetry()
    {
        var sidecar = new TaskCompletionSource();
        var dapr = Substitute.For<DaprClient>();
        dapr.WaitForSidecarAsync(Arg.Any<CancellationToken>()).Returns(sidecar.Task);
        var topic = new FakeTopic<Context>();
        await using var app = await StartAsync(dapr, topic, configure: options => options.WaitForSidecar = true);
        using var client = app.GetTestClient();

        using var response = await client.PostAsync(new Uri("/items", UriKind.Relative), new StringContent(ItemBody));
        using var notReady = await client.GetAsync(new Uri(GatewayServiceCollectionExtensions.ReadinessPath, UriKind.Relative));

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.NotNull(response.Headers.RetryAfter);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, notReady.StatusCode);
        Assert.Equal(0, topic.Count);

        sidecar.SetResult();
        await WaitUntilReadyAsync(client);

        using var accepted = await client.PostAsync(new Uri("/items", UriKind.Relative), new StringContent(ItemBody));
        Assert.Equal(HttpStatusCode.Accepted, accepted.StatusCode);
        Assert.Equal(1, topic.Count);
    }

    [Fact]
    public async Task Start_WithAPipelineThatDoesNotCompose_Fails()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => StartAsync(
            Substitute.For<DaprClient>(), new FakeTopic<Context>(),
            endpoints: e => e.Endpoint<Item, Item>("items-endpoint", "/items",
                (builder, _) => builder.WithDeserializer(new ItemDeserializer()))));

        Assert.Contains("composition failed", error.Message, StringComparison.Ordinal);
    }

    // Each endpoint runs its endpoint's pipeline, as each of an extractor's source ports does.
    [Fact]
    public async Task Post_ToEachEndpoint_StampsItsPort()
    {
        var topic = new FakeTopic<Context>();
        await using var app = await StartAsync(Substitute.For<DaprClient>(), topic,
            endpoints: e => e.Endpoint<Item, Item>("items-a", "/items/a", Pipeline)
                            .Endpoint<Item, Item>("items-b", "/items/b", Pipeline));
        using var client = app.GetTestClient();

        using var a = await client.PostAsync(new Uri("/items/a", UriKind.Relative), new StringContent(ItemBody));
        using var b = await client.PostAsync(new Uri("/items/b", UriKind.Relative), new StringContent("""{"Id":"item-2"}"""));

        Assert.Equal(HttpStatusCode.Accepted, a.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, b.StatusCode);
        Assert.Equal(["items-a", "items-b"], topic.Events.Select(e => (string?)e[CloudEventExtensionKeys.SourcePort]));
    }

    [Fact]
    public void AddGateway_WithoutAnEndpoint_SaysHow()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddGateway(
            new GatewayDefinition { Endpoints = _ => { } }));

        Assert.Contains("no endpoint is declared", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoint_WithAPathThatIsNotAPath_NamesThePath()
    {
        var error = Assert.Throws<ArgumentException>(() => new GatewayEndpointDeclarations()
            .Endpoint<Item, Item>("items", "items", Pipeline));

        Assert.Contains("starts with '/'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoint_WithAPortAnotherEndpointHas_Fails()
    {
        var endpoints = new GatewayEndpointDeclarations().Endpoint<Item, Item>("items-a", "/items/a", Pipeline);

        var error = Assert.Throws<ArgumentException>(() =>
            endpoints.Endpoint<Item, Item>("items-a", "/items/b", Pipeline));

        Assert.Contains("Endpoint 'items-a' is already declared", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoint_WithAPathAnotherEndpointHas_Fails()
    {
        var endpoints = new GatewayEndpointDeclarations().Endpoint<Item, Item>("items-a", "/items", Pipeline);

        // ASP.NET Core matches paths case-insensitively.
        var error = Assert.Throws<ArgumentException>(() => endpoints.Endpoint<Item, Item>("items-b", "/Items", Pipeline));

        Assert.Contains("already the path of endpoint 'items-a'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoint_WithoutAPipeline_Fails()
    {
        var error = Assert.Throws<ArgumentNullException>(() => new GatewayEndpointDeclarations()
            .Endpoint<Item, Item>("items", "/items", null!));

        Assert.Equal("configurePipeline", error.ParamName);
    }

    [Fact]
    public void AddGateway_WithANonPositiveLimit_NamesTheOption()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddGateway(
            Definition(configure: options => options.MaxRequestBodyBytes = 0)));

        Assert.Contains(nameof(GatewayOptions.MaxRequestBodyBytes), error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddGateway_Twice_Fails()
    {
        var services = new ServiceCollection().AddGateway(Definition());

        var error = Assert.Throws<InvalidOperationException>(() => services.AddGateway(Definition()));

        Assert.Contains("Only one gateway", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MapGateway_WithoutAGateway_Fails()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        await using var app = builder.Build();

        var error = Assert.Throws<InvalidOperationException>(() => app.MapGateway());

        Assert.Contains("AddGateway", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Post_WithARoutedIncident_RejectsTheCaller()
    {
        var incidents = new FakeBusinessIncidentServiceClient();
        var topic = new FakeTopic<Context>();
        await using var app = await StartAsync(Substitute.For<DaprClient>(), topic, incidents,
            endpoints: e => e.Endpoint<Item, Item>("items-endpoint", "/items", FailingPipeline));
        using var client = app.GetTestClient();

        using var response = await client.PostAsync(new Uri("/items", UriKind.Relative), new StringContent(ItemBody));

        // The router ends the pipeline as a success; the gateway tells the routed incident apart
        // from a publish by the successful result's subtype, and answers 422.
        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
        Assert.Contains("Item has no lines", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
        Assert.Equal(0, topic.Count);
        Assert.Single(incidents.Incidents);
    }

    private static async Task WaitUntilReadyAsync(HttpClient client)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            using var ready = await client.GetAsync(new Uri(GatewayServiceCollectionExtensions.ReadinessPath, UriKind.Relative));
            if (ready.StatusCode == HttpStatusCode.OK)
                return;
            await Task.Delay(20);
        }

        Assert.Fail("The gateway did not become ready after its sidecar answered.");
    }

    private static async Task<WebApplication> StartAsync(DaprClient dapr, FakeTopic<Context> topic,
        IBusinessIncidentServiceClient? incidents = null,
        Action<GatewayOptions>? configure = null,
        Action<GatewayEndpointDeclarations>? endpoints = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        var services = builder.Services;
        services.AddTestIdentity();
        services.AddSingleton(dapr);
        services.AddSingleton<SendStep<Context>>(topic);
        services.AddSingleton<IIdempotencyServiceClient>(new FakeIdempotencyServiceClient());
        services.AddSingleton<IBusinessIncidentServiceClient>(incidents ?? new FakeBusinessIncidentServiceClient());
        services.AddGateway(Definition(endpoints: endpoints, configure: options =>
        {
            options.WaitForSidecar = false;
            configure?.Invoke(options);
        }));

        var app = builder.Build();
        app.MapGateway();
        try
        {
            await app.StartAsync();
        }
        catch
        {
            await app.DisposeAsync();
            throw;
        }

        return app;
    }

    private static GatewayDefinition Definition(
        Action<GatewayEndpointDeclarations>? endpoints = null,
        Action<GatewayOptions>? configure = null) => new()
    {
        Endpoints = endpoints ?? (e => e.Endpoint<Item, Item>("items-endpoint", "/items", Pipeline)),
        ConfigureOptions = configure,
    };

    private static ExtractorBuilder<Item, Item, Context> Pipeline(ExtractorBuilder<Item, Item, Context> builder,
        IServiceProvider services) => builder
        .WithDeserializer(new ItemDeserializer())
        .WithValidator(new ItemValidator())
        .WithIdempotency((item, _) => item.Id, (_, _) => DateTimeOffset.UnixEpoch)
        .WithTransformer(new ItemTransformer())
        .WithSerializer(new CloudEventSerializeStep<Item, Context>(item => item.Id, _ => DateTimeOffset.UnixEpoch))
        .WithSenderFromServices()
        .WithBusinessIncidents(_ => "id", _ => "subject");

    private static ExtractorBuilder<Item, Item, Context> FailingPipeline(ExtractorBuilder<Item, Item, Context> builder,
        IServiceProvider services) => builder
        .WithDeserializer(new ItemDeserializer())
        .WithValidator(new NoLinesValidator())
        .WithIdempotency((item, _) => item.Id, (_, _) => DateTimeOffset.UnixEpoch)
        .WithTransformer(new ItemTransformer())
        .WithSerializer(new CloudEventSerializeStep<Item, Context>(item => item.Id, _ => DateTimeOffset.UnixEpoch))
        .WithSenderFromServices()
        .WithBusinessIncidents(_ => "id", _ => "subject");

    public sealed record Item(string Id);

    private sealed class ItemDeserializer : DeserializeStep<Item, Context>
    {
        public override Task<(BusinessStepResult<Item> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<Item>, Context)>(
                (new BusinessStepResult<Item>.Success(JsonSerializer.Deserialize<Item>(input.Span)!), context));
    }

    private sealed class ItemValidator : ValidateStep<Item, Context>
    {
        public override Task<(BusinessStepResult<Item> Result, Context Context)> ExecuteAsync(Item input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<Item>, Context)>((new BusinessStepResult<Item>.Success(input), context));
    }

    private sealed class NoLinesValidator : ValidateStep<Item, Context>
    {
        public override Task<(BusinessStepResult<Item> Result, Context Context)> ExecuteAsync(Item input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(BusinessStepResult<Item>, Context)>(
                (new BusinessStepResult<Item>.Failure(new BusinessIncidentData { Description = "Item has no lines" }),
                    context));
    }

    private sealed class ItemTransformer : TransformStep<Item, Item, Context>
    {
        public override Task<(TechnicalStepResult<Item> Result, Context Context)> ExecuteAsync(Item input,
            Context context, CancellationToken ct) =>
            Task.FromResult<(TechnicalStepResult<Item>, Context)>((new TechnicalStepResult<Item>.Success(input), context));
    }
}

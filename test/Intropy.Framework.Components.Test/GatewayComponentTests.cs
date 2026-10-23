using System.Globalization;
using System.Net;
using System.Net.Http.Json;
using CloudNative.CloudEvents;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Components.Test.Samples;
using Intropy.Framework.Hosting.Gateway;
using Intropy.Framework.Testing.Components;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Components.Test;

public class GatewayComponentTests
{
    private const string Order =
        """{"orderId":"ORD-1","orderDate":"2026-01-15T09:30:00+00:00","lines":2}""";

    private const string EmptyOrder =
        """{"orderId":"ORD-1","orderDate":"2026-01-15T09:30:00+00:00","lines":0}""";

    [Fact]
    public async Task Post_PublishesTheEventTheRouteDescribes()
    {
        await using var host = new GatewayTestHost<OrderGateway>();

        using var response = await host.PostAsync(Order);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var published = Assert.Single(host.Topic.Events);
        Assert.Equal("ORD-1", published.Subject);
        Assert.Equal(DateTimeOffset.Parse("2026-01-15T09:30:00+00:00", CultureInfo.InvariantCulture), published.Time);
        var body = await response.Content.ReadFromJsonAsync<GatewayResponses.PublishedBody>();
        Assert.Equal(published.Id, body!.EventId);
        // The pipeline runs as the endpoint's route, so its idempotency records are the endpoint's own.
        var committed = Assert.Single(host.Idempotency.Committed);
        Assert.StartsWith("order-gateway-endpoint:", committed.Id, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Endpoints_DefaultToTheOneTheManifestNames()
    {
        await using var host = new GatewayTestHost<RecordedEndpointGateway>();

        var endpoint = Assert.Single(host.Endpoints);
        Assert.Equal(new GatewayEndpointDeclaration("order-gateway-endpoint", "/orders"), endpoint);
        using var response = await host.PostAsync(Order);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public void Endpoints_WithoutAnEndpointPath_DefaultToTheMessage()
    {
        var host = new GatewayTestHost<DefaultPathGateway>();

        Assert.Equal("/order-placed", host.Path);
    }

    [Fact]
    public async Task Post_ToEachEndpoint_RunsItsOwnRoute()
    {
        await using var host = new GatewayTestHost<TwoEndpointGateway>();

        using var json = await host.PostAsync(Order, endpoint: TwoEndpointGateway.Json);
        using var edi = await host.PostAsync("EDI-7;2026-02-01T08:00:00+00:00", "text/plain", endpoint: TwoEndpointGateway.Edi);

        Assert.Equal(HttpStatusCode.Accepted, json.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, edi.StatusCode);
        Assert.Equal(["ORD-1", "EDI-7"], host.Topic.Events.Select(e => e.Subject));
        // Each endpoint is stamped on its events and keeps its own idempotency records.
        Assert.Equal([TwoEndpointGateway.Json, TwoEndpointGateway.Edi],
            host.Topic.Events.Select(e => (string?)e[CloudEventExtensionKeys.SourcePort]));
        Assert.Equal(["order-gateway-endpoint:ORD-1", "order-gateway-edi:EDI-7"], host.Idempotency.Committed.Select(c => c.Id));
    }

    [Fact]
    public async Task Post_WithSeveralEndpoints_NeedsTheEndpointNamed()
    {
        await using var host = new GatewayTestHost<TwoEndpointGateway>();

        Assert.Equal("/orders/edi", host.PathOf(TwoEndpointGateway.Edi));
        Assert.Throws<InvalidOperationException>(() => host.Path);
    }

    [Fact]
    public async Task Post_EchoesTheCallersRequestId()
    {
        await using var host = new GatewayTestHost<OrderGateway>();

        using var response = await host.PostAsync(Order, requestId: "req-42");

        Assert.Equal("req-42", Assert.Single(response.Headers.GetValues("X-Request-Id")));
        var body = await response.Content.ReadFromJsonAsync<GatewayResponses.PublishedBody>();
        Assert.Equal("req-42", body!.RequestId);
    }

    [Fact]
    public async Task Post_WithADuplicate_AnswersOkWithoutPublishing()
    {
        await using var host = new GatewayTestHost<OrderGateway>();
        host.Idempotency.NextStatus = new StatusResponse(Action.Ignore, Reason.SameData);

        using var response = await host.PostAsync(Order);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(0, host.Topic.Count);
    }

    [Fact]
    public async Task Post_WithoutIdempotency_PublishesEveryResendWithoutCallingTheService()
    {
        await using var host = new GatewayTestHost<NoIdempotencyGateway>();
        // What a deduplicating gateway would answer 200 for: no check runs, so it is never asked.
        host.Idempotency.NextStatus = new StatusResponse(Action.Ignore, Reason.SameData);

        using var first = await host.PostAsync(Order);
        using var resend = await host.PostAsync(Order);

        Assert.Equal(HttpStatusCode.Accepted, first.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, resend.StatusCode);
        Assert.Equal(2, host.Topic.Count);
        Assert.Empty(host.Idempotency.StatusChecks);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Fact]
    public void Pipeline_DeclaringARuleAndNoIdempotency_Fails()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new GatewayTestHost<BothIdempotencyGateway>());

        Assert.Contains("not both", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Post_WithExtractSteps_RunsThemInOrderBeforeTheIdempotencyCheck()
    {
        await using var host = new GatewayTestHost<EnrichingGateway>();

        using var response = await host.PostAsync(Order);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        // Each extract step enriched the item, in declaration order, before the transformer.
        Assert.Equal("ORD-1:first:second", Assert.Single(host.Topic.Events).Subject);
        // The idempotency check ran after them: its key reads the enriched item.
        Assert.EndsWith("ORD-1:first:second", Assert.Single(host.Idempotency.Committed).Id, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Post_WithTheComponentsContext_RunsTheStepsAndRulesOnIt()
    {
        await using var host = new GatewayTestHost<TenantGateway>();

        using var response = await host.PostAsync(Order, requestId: "req-9");

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        // The extract step read the component's context, and the gateway's metadata on it, though
        // the factory started from an empty dictionary.
        Assert.Equal("ORD-1:acme:req-9", Assert.Single(host.Topic.Events).Subject);
        // The idempotency rule read the component's context too.
        Assert.EndsWith("acme/ORD-1:acme:req-9", Assert.Single(host.Idempotency.Committed).Id, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Post_WithABusinessFailure_RejectsAndNamesTheIncidentAfterTheBusinessId()
    {
        await using var host = new GatewayTestHost<OrderGateway>();

        using var response = await host.PostAsync(EmptyOrder, requestId: "req-7");

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
        Assert.Contains("Order has no lines", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
        Assert.Equal(0, host.Topic.Count);
        var incident = Assert.Single(host.Incidents.Incidents);
        Assert.Equal("ORD-1", incident.Id);
        Assert.Equal("ORD-1", incident.Subject);
    }

    [Fact]
    public async Task Post_WithABodyThatDoesNotDeserialize_RejectsAndNamesTheIncidentAfterTheRequest()
    {
        await using var host = new GatewayTestHost<OrderGateway>();

        using var response = await host.PostAsync("not json", requestId: "req-8");

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
        var incident = Assert.Single(host.Incidents.Incidents);
        Assert.Equal("req-8", incident.Id);
        Assert.Equal("request:req-8", incident.Subject);
    }

    [Fact]
    public async Task Post_WhenThePublisherFails_AsksTheCallerToRetry()
    {
        await using var host = new GatewayTestHost<OrderGateway>();
        host.Topic.SendException = new HttpRequestException("pub/sub down");

        using var response = await host.PostAsync(Order);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(TimeSpan.FromSeconds(5), response.Headers.RetryAfter?.Delta);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Fact]
    public async Task Post_WhenThePublisherFailsPermanently_AnswersServerErrorWithoutRetry()
    {
        await using var host = new GatewayTestHost<OrderGateway>();
        host.Topic.SendException = new InvalidOperationException("a fault resending cannot cure");

        using var response = await host.PostAsync(Order);

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Null(response.Headers.RetryAfter);
        Assert.Equal(0, host.Topic.Count);
        Assert.Empty(host.Idempotency.Committed);
    }

    [Fact]
    public async Task Post_WhenTheIdempotencyServiceIsDown_AsksTheCallerToRetry()
    {
        await using var host = new GatewayTestHost<OrderGateway>();
        host.Idempotency.StatusException = new IdempotencyServiceException("unavailable", new HttpRequestException());

        using var response = await host.PostAsync(Order);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(0, host.Topic.Count);
    }

    [Fact]
    public async Task Probes_AnswerOnceTheGatewayIsReady()
    {
        await using var host = new GatewayTestHost<OrderGateway>();
        var client = await host.StartAsync();

        using var live = await client.GetAsync(new Uri(GatewayServiceCollectionExtensions.LivenessPath, UriKind.Relative));
        using var ready = await client.GetAsync(new Uri(GatewayServiceCollectionExtensions.ReadinessPath, UriKind.Relative));

        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        Assert.Equal(HttpStatusCode.OK, ready.StatusCode);
    }

    [Fact]
    public async Task Overrides_ReplaceEachDefault()
    {
        await using var host = new GatewayTestHost<CustomOrderGateway>();

        using var response = await host.PostAsync(Order);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        Assert.Equal("custom:ORD-1", Assert.Single(host.Topic.Events).Subject);
    }

    [Fact]
    public async Task Overrides_ApplyTheIncidentRule()
    {
        await using var host = new GatewayTestHost<CustomOrderGateway>();

        using var _ = await host.PostAsync(EmptyOrder);

        var incident = Assert.Single(host.Incidents.Incidents);
        Assert.Equal("custom-id", incident.Id);
        Assert.Equal("custom-subject", incident.Subject);
    }

    [Fact]
    public async Task Overrides_ApplyTheOptions()
    {
        await using var host = new GatewayTestHost<CustomOrderGateway>();

        using var response = await host.PostAsync(new string(' ', 257) + Order);

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Equal(0, host.Topic.Count);
    }

    [Fact]
    public void Manifest_WithTheEndpointAtAnotherPath_Fails()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new GatewayTestHost<MovedPathGateway>());

        Assert.Contains("at '/elsewhere'", error.Message, StringComparison.Ordinal);
        Assert.Contains("names '/orders'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Manifest_WithoutTheManifestsEndpoint_Fails()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new GatewayTestHost<RenamedEndpointGateway>());

        Assert.Contains("declares no endpoint 'order-gateway-endpoint'", error.Message, StringComparison.Ordinal);
        Assert.Contains("'orders-in'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoint_AcceptingNothing_NamesIt()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new GatewayTestHost<AcceptingNothingGateway>());

        Assert.Contains("Endpoint 'order-gateway-endpoint' accepts nothing", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoint_AcceptingTwice_NamesWhatItAlreadyAccepts()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new GatewayTestHost<AcceptingTwiceGateway>());

        Assert.Contains("Endpoint 'order-gateway-endpoint' already accepts OrderRequest", error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Input_PublishingNothing_NamesIt()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new GatewayTestHost<PublishingNothingGateway>());

        Assert.Contains("Endpoint 'order-gateway-endpoint' accepts OrderRequest but publishes nothing", error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoints_PublishingDifferentTypes_NameThem()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new GatewayTestHost<MixedPayloadGateway>());

        Assert.Contains("publish OrderPlaced, OrderReceived", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Start_WithARouteMissingARule_NamesTheRule()
    {
        await using var host = new GatewayTestHost<RulelessGateway>();

        var error = await Assert.ThrowsAsync<InvalidOperationException>(host.StartAsync);

        Assert.Contains("no idempotency rule", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Start_WithARouteWithoutSteps_NamesTheDsl()
    {
        await using var host = new GatewayTestHost<StepLessGateway>();

        var error = await Assert.ThrowsAsync<InvalidOperationException>(host.StartAsync);

        Assert.Contains("has no steps", error.Message, StringComparison.Ordinal);
        Assert.Contains("Steps(steps => steps.Deserialize<", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Post_ToEndpointsAcceptingTheSameInput_RunsEachEndpointsSteps()
    {
        await using var host = new GatewayTestHost<TwinStepGateway>();

        using var main = await host.PostAsync(Order, endpoint: TwinStepGateway.Main);
        using var twin = await host.PostAsync(Order, endpoint: TwinStepGateway.Twin);

        Assert.Equal(HttpStatusCode.Accepted, main.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, twin.StatusCode);
        // The main endpoint publishes OrderTransformer's event, the twin its own AltOrderTransformer's.
        Assert.Equal(["ORD-1", "twin:ORD-1"], host.Topic.Events.Select(e => e.Subject));
    }

    [Fact]
    public async Task ConfigureServices_ProvidesTheStepsDependencies()
    {
        await using var host = new GatewayTestHost<AuthorStepsGateway>();

        using var response = await host.PostAsync(Order);

        // The pipeline's deserializer took its Greeting from ConfigureServices: the subject carries it.
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal("author:ORD-1", Assert.Single(host.Topic.Events).Subject);
    }

    [Fact]
    public void Endpoint_WithoutAPortInTheManifest_NamesThePort()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new GatewayTestHost<PortlessGateway>());

        Assert.Contains("names no Port", error.Message, StringComparison.Ordinal);
    }
}

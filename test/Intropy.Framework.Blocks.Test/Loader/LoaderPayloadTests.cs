using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.Loader;

/// <summary>
/// What the pipeline is handed, and what it expects to have been done for it: the two halves a host
/// performs before it runs a loader, pinned separately because a host does them from a delivery
/// rather than from an event, and because a claim-checked message's payload does not travel inside
/// the message that names it.
/// </summary>
public class LoaderPayloadTests
{
    private readonly IServiceCollection _services;

    public LoaderPayloadTests()
    {
        var idempotency = Substitute.For<IIdempotencyServiceClient>();
        idempotency.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

        var loggerFactory = Substitute.For<ILoggerFactory>();
        loggerFactory.CreateLogger(Arg.Any<string>()).Returns(Substitute.For<ILogger>());

        _services = new ServiceCollection();
        _services.AddSingleton(idempotency);
        _services.AddSingleton(Substitute.For<IBusinessIncidentServiceClient>());
        _services.AddSingleton(loggerFactory);
        _services.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "LoaderTest";
            conf.ServiceNamespace = "Org";
        });
    }

    [Fact]
    public async Task APayloadSetToAnObject_ReachesTheDeserializerAsThatJsonObject()
    {
        var capture = new CapturingDeserializer();
        var (pipeline, context) = Build(capture);

        await pipeline.Execute(EventWith(new LoaderCustomerIn { CustomerId = "1337", Name = "John Doe" }), context);

        // camelCase, because that is how the framework's formatter writes a payload on the wire.
        Assert.Equal("""{"customerId":"1337","name":"John Doe"}""", capture.Received);
    }

    [Fact]
    public async Task APayloadSetToPreSerializedText_ReachesTheDeserializerQuoted()
    {
        // The trap this contract exists to make visible: pre-serialized JSON text is a JSON *string*,
        // and a string goes on the wire quoted and escaped. Nothing unwraps it — that is what keeps
        // an inline payload and a claim-checked one the same bytes.
        var capture = new CapturingDeserializer();
        var (pipeline, context) = Build(capture);
        var text = JsonSerializer.Serialize(new LoaderCustomerIn { CustomerId = "1337", Name = "John Doe" });

        await pipeline.Execute(EventWith(text), context);

        Assert.StartsWith("\"", capture.Received, StringComparison.Ordinal);
        Assert.Equal(JsonSerializer.Serialize(text), capture.Received);
    }

    [Fact]
    public async Task TheCloudEventOverload_ProjectsEveryAttributeOntoTheContext()
    {
        var (pipeline, context) = Build(new CapturingDeserializer());
        var cloudEvent = EventWith(new LoaderCustomerIn { CustomerId = "1337", Name = "John Doe" });

        var (_, finalContext) = await pipeline.Execute(cloudEvent, context);

        Assert.Equal(cloudEvent.Id, finalContext.Metadata[CloudEventContextKeys.Id]);
        Assert.Equal(cloudEvent.Subject, finalContext.Metadata[CloudEventContextKeys.Subject]);
        Assert.Equal(cloudEvent.Source!.ToString(), finalContext.Metadata[CloudEventContextKeys.Source]);
        Assert.Equal(cloudEvent.Type, finalContext.Metadata[CloudEventContextKeys.Type]);
        Assert.Equal("application/json", finalContext.Metadata[CloudEventContextKeys.DataContentType]);
        Assert.NotNull(finalContext.Metadata[CloudEventContextKeys.Time]);
    }

    [Fact]
    public async Task TheCloudEventOverload_ProjectsEveryExtensionAttributeOntoTheContext()
    {
        var (pipeline, context) = Build(new CapturingDeserializer());
        var cloudEvent = EventWith(new LoaderCustomerIn { CustomerId = "1337", Name = "John Doe" });
        cloudEvent[CloudEventAttribute.CreateExtension("tenantid", CloudEventAttributeType.String)] = "acme";
        cloudEvent[CloudEventAttribute.CreateExtension("priority", CloudEventAttributeType.Integer)] = 5;

        var (_, finalContext) = await pipeline.Execute(cloudEvent, context);

        Assert.Equal("acme", finalContext.Metadata[CloudEventContextKeys.Extension("tenantid")]);
        Assert.Equal("5", finalContext.Metadata[CloudEventContextKeys.Extension("priority")]);
    }

    [Fact]
    public async Task TheBytesOverload_ProjectsNothing_SoACallerThatDidNotProjectFails()
    {
        // The contract this whole shape rests on: the pipeline consumes the context, it does not
        // populate it. A host that forgets to project gets a failure here rather than an idempotency
        // record keyed on nothing.
        var (pipeline, context) = Build(new CapturingDeserializer());
        var payload = Encoding.UTF8.GetBytes("""{"customerId":"1337","name":"John Doe"}""");

        var (result, _) = await pipeline.Execute(payload, context);

        Assert.IsType<StepResult<LoaderCustomerOut>.TechnicalFailure>(result);
    }

    private static CloudEvent EventWith(object data) => new()
    {
        Id = Guid.NewGuid().ToString(),
        Subject = "customer-1337",
        Time = DateTimeOffset.UtcNow,
        Source = new Uri("urn:test:source"),
        Type = "com.test.customer.loaded",
        DataContentType = "application/json",
        Data = data
    };

    private (Loader<LoaderCustomerIn, LoaderCustomerOut, Context> Pipeline, Context Context) Build(
        DeserializeStep<LoaderCustomerIn, Context> deserializer)
    {
        var pipeline = LoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context>
            .Create("LoaderTest", _services.BuildServiceProvider())
            .WithDeserializer(deserializer)
            .WithValidator(new CustomerValidator())
            .WithIdempotency()
            .WithTransformer(new CustomerTransformer())
            .WithSender(new CustomerSender())
            .WithBusinessIncidents(
                context => context.Metadata.GetValueOrDefault(CloudEventContextKeys.Subject, "unknown"),
                context => context.Metadata.GetValueOrDefault(CloudEventContextKeys.Subject, "unknown"))
            .Build();
        return (pipeline, new Context(new Dictionary<string, string>()));
    }

    /// <summary>Records the payload exactly as the pipeline handed it over.</summary>
    private sealed class CapturingDeserializer : DeserializeStep<LoaderCustomerIn, Context>
    {
        private static readonly JsonSerializerOptions s_options = new(JsonSerializerDefaults.Web);

        public string? Received { get; private set; }

        public override Task<(BusinessStepResult<LoaderCustomerIn> Result, Context Context)> ExecuteAsync(
            ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
        {
            Received = Encoding.UTF8.GetString(input.Span);
            var value = JsonSerializer.Deserialize<LoaderCustomerIn>(input.Span, s_options)
                        ?? new LoaderCustomerIn { CustomerId = "1337", Name = "placeholder" };
            return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
                (new BusinessStepResult<LoaderCustomerIn>.Success(value), context));
        }
    }
}

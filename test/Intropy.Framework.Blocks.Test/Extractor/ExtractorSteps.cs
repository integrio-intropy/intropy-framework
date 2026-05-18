using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Test.Extractor;

public sealed class CustomerIn
{
    public required string CustomerId { get; init; }
    public DateTime EventDate { get; init; }
    public List<string> Enrichments { get; } = new();
}

public sealed class CustomerOut
{
    public int CustomerId { get; set; }
    public List<string> Enrichments { get; set; } = new();
}

public sealed class CustomerDeserializer : DeserializeStep<CustomerIn, Context>
{
    public override string StepName => "Deserialize";

    public override Task<(BusinessStepResult<CustomerIn> Result, Context Context)> ExecuteAsync(
        string input, Context context, CancellationToken ct)
    {
        var obj = new CustomerIn
        {
            CustomerId = input,
            EventDate = DateTime.Parse("1970-01-01")
        };

        return Task.FromResult<(BusinessStepResult<CustomerIn>, Context)>(
            (new BusinessStepResult<CustomerIn>.Success(obj), context));
    }
}

public sealed class CustomerExtractor : ExtractStep<CustomerIn, Context>
{
    public override string StepName => "Extract";

    public override Task<(BusinessStepResult<CustomerIn> Result, Context Context)> ExecuteAsync(
        CustomerIn input, Context context, CancellationToken ct)
    {
        // Simulate enriching data from external source                                                                                                            
        return Task.FromResult<(BusinessStepResult<CustomerIn>, Context)>(
            (new BusinessStepResult<CustomerIn>.Success(input), context));
    }
}

public sealed class CustomerValidator : ValidateStep<CustomerIn, Context>
{
    public override string StepName => "Validate";

    public override Task<(BusinessStepResult<CustomerIn> Result, Context Context)> ExecuteAsync(
        CustomerIn input, Context context, CancellationToken ct)
    {
        var parsedOk = int.TryParse(input.CustomerId, out _);
        if (!parsedOk)
        {
            var bi = new BusinessIncidentData
            {
                Description = "Invalid customer ID",
                Context = new Dictionary<string, string> { { "customerId", input.CustomerId } }
            };
            return Task.FromResult<(BusinessStepResult<CustomerIn>, Context)>(
                (new BusinessStepResult<CustomerIn>.Failure(bi), context));
        }

        return Task.FromResult<(BusinessStepResult<CustomerIn>, Context)>(
            (new BusinessStepResult<CustomerIn>.Success(input), context));
    }
}

public sealed class CustomerTransformer : TransformStep<CustomerIn, CustomerOut, Context>
{
    public override string StepName => "Transform";

    public override Task<(TechnicalStepResult<CustomerOut> Result, Context Context)> ExecuteAsync(
        CustomerIn input, Context context, CancellationToken ct)
    {
        var output = new CustomerOut
        {
            CustomerId = int.Parse(input.CustomerId),
            Enrichments = input.Enrichments
        };

        return Task.FromResult<(TechnicalStepResult<CustomerOut>, Context)>(
            (new TechnicalStepResult<CustomerOut>.Success(output), context));
    }
}

public sealed class CustomerSerializer : SerializeStep<CustomerOut, Context>
{
    public override string StepName => "Serialize";

    public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(
        CustomerOut input, Context context, CancellationToken ct)
    {
        var cloudEvent = new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            // Source and Type will be set by DaprCloudEventSender from builder configuration
            // Subject and Time are set here since they come from the data
            Subject = input.CustomerId.ToString(),
            Time = DateTimeOffset.UtcNow,
            Data = input
        };

        return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
            (new TechnicalStepResult<CloudEvent>.Success(cloudEvent), context));
    }
}

public sealed class CustomerSender : SendStep<Context>
{
    public override string StepName => "Send";

    public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(
        CloudEvent input, Context context, CancellationToken ct)
    {
        return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
            (new TechnicalStepResult<CloudEvent>.Success(input), context));
    }
}

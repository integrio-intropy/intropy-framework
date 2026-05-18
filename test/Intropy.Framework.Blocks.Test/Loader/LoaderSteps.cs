using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Test.Loader;

public sealed class LoaderCustomerIn
{
    public required string CustomerId { get; init; }
    public required string Name { get; init; }
}

public sealed class LoaderCustomerOut
{
    public required int CustomerId { get; init; }
    public required string Name { get; init; }
}

public sealed class CustomerDeserializer : DeserializeStep<LoaderCustomerIn, Context>
{
    protected override Task<(BusinessStepResult<LoaderCustomerIn> Result, Context Context)> DeserializeAsync(
        CloudEvent cloudEvent, Context context)
    {
        var jsonString = cloudEvent.Data?.ToString();
        if (string.IsNullOrEmpty(jsonString))
        {
            var bi = new BusinessIncidentData
            {
                Description = "CloudEvent data is null or empty", Context = new Dictionary<string, string>()
            };
            return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
                (new BusinessStepResult<LoaderCustomerIn>.Failure(bi), context));
        }

        var obj = JsonSerializer.Deserialize<LoaderCustomerIn>(jsonString);
        if (obj == null)
        {
            var bi = new BusinessIncidentData{
                Description = "Failed to deserialize CloudEvent data",
                Context = new Dictionary<string, string> { { "data", jsonString } }
            };
            return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
                (new BusinessStepResult<LoaderCustomerIn>.Failure(bi), context));
        }

        return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
            (new BusinessStepResult<LoaderCustomerIn>.Success(obj), context));
    }
}

public sealed class CustomerValidator : ValidateStep<LoaderCustomerIn, Context>
{
    public override Task<(BusinessStepResult<LoaderCustomerIn> Result, Context Context)> ExecuteAsync(
        LoaderCustomerIn input, Context context, CancellationToken ct)
    {
        var parsedOk = int.TryParse(input.CustomerId, out _);
        if (!parsedOk)
        {
            var bi = new BusinessIncidentData
            {
                Description = "Invalid customer ID",
                Context = new Dictionary<string, string> { { "customerId", input.CustomerId } }
            };
            return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
                (new BusinessStepResult<LoaderCustomerIn>.Failure(bi), context));
        }

        if (string.IsNullOrEmpty(input.Name))
        {
            var bi = new BusinessIncidentData
            {
                Description = "Customer name cannot be empty",
                Context = new Dictionary<string, string> { { "customerId", input.CustomerId } }
            };
            return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
                (new BusinessStepResult<LoaderCustomerIn>.Failure(bi), context));
        }

        return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
            (new BusinessStepResult<LoaderCustomerIn>.Success(input), context));
    }
}

public sealed class CustomerNameNormalizer : ExtractStep<LoaderCustomerIn, Context>
{
    public override Task<(BusinessStepResult<LoaderCustomerIn> Result, Context Context)> ExecuteAsync(
        LoaderCustomerIn input, Context context, CancellationToken ct)
    {
        var normalized = new LoaderCustomerIn
        {
            CustomerId = input.CustomerId,
            Name = input.Name.Trim().ToUpperInvariant()
        };

        return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
            (new BusinessStepResult<LoaderCustomerIn>.Success(normalized), context));
    }
}

public sealed class CustomerTransformer : TransformStep<LoaderCustomerIn, LoaderCustomerOut, Context>
{
    public override Task<(TechnicalStepResult<LoaderCustomerOut> Result, Context Context)> ExecuteAsync(
        LoaderCustomerIn input, Context context, CancellationToken ct)
    {
        var output = new LoaderCustomerOut
        {
            CustomerId = int.Parse(input.CustomerId),
            Name = input.Name
        };

        return Task.FromResult<(TechnicalStepResult<LoaderCustomerOut>, Context)>(
            (new TechnicalStepResult<LoaderCustomerOut>.Success(output), context));
    }
}

public sealed class CustomerSender : SendStep<LoaderCustomerOut, Context>
{
    public override Task<(TechnicalStepResult<LoaderCustomerOut> Result, Context Context)> ExecuteAsync(
        LoaderCustomerOut input, Context context, CancellationToken ct)
    {
        // Simulate successful send
        return Task.FromResult<(TechnicalStepResult<LoaderCustomerOut>, Context)>(
            (new TechnicalStepResult<LoaderCustomerOut>.Success(input), context));
    }
}

public sealed class FailingSender : SendStep<LoaderCustomerOut, Context>
{
    public override Task<(TechnicalStepResult<LoaderCustomerOut> Result, Context Context)> ExecuteAsync(
        LoaderCustomerOut input, Context context, CancellationToken ct)
    {
        throw new InvalidOperationException("Simulated send failure");
    }
}

public sealed class ReceiptSender : SendStep<LoaderCustomerOut, Context>
{
    public List<LoaderCustomerOut> SentReceipts { get; } = new();

    public override Task<(TechnicalStepResult<LoaderCustomerOut> Result, Context Context)> ExecuteAsync(
        LoaderCustomerOut input, Context context, CancellationToken ct)
    {
        SentReceipts.Add(input);
        return Task.FromResult<(TechnicalStepResult<LoaderCustomerOut>, Context)>(
            (new TechnicalStepResult<LoaderCustomerOut>.Success(input), context));
    }
}

public sealed class FailingReceiptSender : SendStep<LoaderCustomerOut, Context>
{
    public override Task<(TechnicalStepResult<LoaderCustomerOut> Result, Context Context)> ExecuteAsync(
        LoaderCustomerOut input, Context context, CancellationToken ct)
    {
        throw new InvalidOperationException("Simulated receipt publish failure");
    }
}

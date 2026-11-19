using System.Text;
using System.Text.Json;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.BulkLoader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Test.Loader;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Test.BulkLoader;

// The Bulk Loader's own per-message steps, built on Intropy.Framework.Blocks.BulkLoader.Steps. They
// mirror the Loader's test steps and share their models.

public sealed class CustomerDeserializer : DeserializeStep<LoaderCustomerIn, Context>
{
    // The framework publishes camelCase payloads.
    private static readonly JsonSerializerOptions s_options = new(JsonSerializerDefaults.Web);

    public override Task<(BusinessStepResult<LoaderCustomerIn> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
    {
        if (input.IsEmpty)
        {
            var bi = new BusinessIncidentData
            {
                Description = "The message carried no payload", Context = new Dictionary<string, string>()
            };
            return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
                (new BusinessStepResult<LoaderCustomerIn>.Failure(bi, Retriable: false), context));
        }

        var jsonString = Encoding.UTF8.GetString(input.Span);
        var obj = JsonSerializer.Deserialize<LoaderCustomerIn>(input.Span, s_options);
        if (obj == null)
        {
            var bi = new BusinessIncidentData{
                Description = "Failed to deserialize the payload",
                Context = new Dictionary<string, string> { { "data", jsonString } }
            };
            return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
                (new BusinessStepResult<LoaderCustomerIn>.Failure(bi, Retriable: false), context));
        }

        return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
            (new BusinessStepResult<LoaderCustomerIn>.Success(obj), context));
    }
}

/// <summary>A validator whose rejections are terminal: malformed data has to be fixed in the
/// source system, so retrying the same item would only re-raise the same incident.</summary>
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
                (new BusinessStepResult<LoaderCustomerIn>.Failure(bi, Retriable: false), context));
        }

        if (string.IsNullOrEmpty(input.Name))
        {
            var bi = new BusinessIncidentData
            {
                Description = "Customer name cannot be empty",
                Context = new Dictionary<string, string> { { "customerId", input.CustomerId } }
            };
            return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
                (new BusinessStepResult<LoaderCustomerIn>.Failure(bi, Retriable: false), context));
        }

        return Task.FromResult<(BusinessStepResult<LoaderCustomerIn>, Context)>(
            (new BusinessStepResult<LoaderCustomerIn>.Success(input), context));
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
    public int SentCount { get; private set; }

    public override Task<(BusinessStepResult<LoaderCustomerOut> Result, Context Context)> ExecuteAsync(
        LoaderCustomerOut input, Context context, CancellationToken ct)
    {
        // Simulate successful send
        SentCount++;
        return Task.FromResult<(BusinessStepResult<LoaderCustomerOut>, Context)>(
            (new BusinessStepResult<LoaderCustomerOut>.Success(input), context));
    }
}

/// <summary>Throws for one customer and sends the others, so a batch can mix a failed send with
/// successful ones.</summary>
public sealed class FailingSenderFor(int customerId) : SendStep<LoaderCustomerOut, Context>
{
    public List<int> Sent { get; } = [];

    public override Task<(BusinessStepResult<LoaderCustomerOut> Result, Context Context)> ExecuteAsync(
        LoaderCustomerOut input, Context context, CancellationToken ct)
    {
        if (input.CustomerId == customerId)
            throw new HttpRequestException("Destination unreachable");
        lock (Sent) Sent.Add(input.CustomerId);
        return Task.FromResult<(BusinessStepResult<LoaderCustomerOut>, Context)>(
            (new BusinessStepResult<LoaderCustomerOut>.Success(input), context));
    }
}

public sealed class FailingReceiptSender : SendReceiptStep<LoaderCustomerOut, Context>
{
    public override Task<(TechnicalStepResult<LoaderCustomerOut> Result, Context Context)> ExecuteAsync(
        LoaderCustomerOut input, Context context, CancellationToken ct)
    {
        throw new InvalidOperationException("Simulated receipt publish failure");
    }
}

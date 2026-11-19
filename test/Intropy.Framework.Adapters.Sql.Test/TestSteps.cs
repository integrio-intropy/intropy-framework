using System.Text;
using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Adapters.Sql.Test;

// Pass-through pipeline steps for the extractor tests: the rows' JSON goes through unchanged, and the
// sender records what it published or fails like a broker that is down.

internal sealed class TextDeserializer : DeserializeStep<string, Context>
{
    public override string StepName => "Deserialize";

    public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<string>, Context)>(
            (new BusinessStepResult<string>.Success(Encoding.UTF8.GetString(input.Span)), context));
}

internal sealed class AcceptValidator : ValidateStep<string, Context>
{
    public override string StepName => "Validate";

    public override Task<(BusinessStepResult<string> Result, Context Context)> ExecuteAsync(string input,
        Context context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<string>, Context)>((new BusinessStepResult<string>.Success(input), context));
}

internal sealed class IdentityTransformer : TransformStep<string, string, Context>
{
    public override string StepName => "Transform";

    public override Task<(TechnicalStepResult<string> Result, Context Context)> ExecuteAsync(string input,
        Context context, CancellationToken ct) =>
        Task.FromResult<(TechnicalStepResult<string>, Context)>((new TechnicalStepResult<string>.Success(input), context));
}

internal sealed class RecordingSender : SendStep<Context>
{
    public List<CloudEvent> Sent { get; } = [];
    public bool Fail { get; set; }

    public override Task<(TechnicalStepResult<CloudEvent> Result, Context Context)> ExecuteAsync(CloudEvent input,
        Context context, CancellationToken ct)
    {
        if (Fail)
            return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
                (new TechnicalStepResult<CloudEvent>.Failure(new TechnicalFailure("The broker refused it")), context));
        Sent.Add(input);
        return Task.FromResult<(TechnicalStepResult<CloudEvent>, Context)>(
            (new TechnicalStepResult<CloudEvent>.Success(input), context));
    }
}
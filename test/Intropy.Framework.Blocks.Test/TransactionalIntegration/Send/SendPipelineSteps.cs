using System.Text.Json;
using System.Text.Json.Serialization;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using JetBrains.Annotations;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Send;

public sealed class MyInput
{
    [JsonPropertyName("orderId")] public required string OrderId { get; init; }
    [JsonPropertyName("eventDateTime")] public DateTime EventDateTime { get; init; }
}

public sealed class MyOutput
{
    public required string OrderId { [UsedImplicitly] get; set; }
}

public sealed record MyContext() : Context(new Dictionary<string, string>())
{
    public static MyContext Create() => new();
}

public sealed class JsonDeserializer : DeserializeStep<MyInput, MyContext>
{
    public override Task<(BusinessStepResult<MyInput> Result, MyContext Context)> ExecuteAsync(ReadOnlyMemory<byte> input,
        MyContext context, CancellationToken ct)
    {
        var obj = JsonSerializer.Deserialize<MyInput>(input.Span);
        return Task.FromResult<(BusinessStepResult<MyInput> Result, MyContext Context)>((
            new BusinessStepResult<MyInput>.Success(obj!), context));
    }
}

public sealed class PassThroughExtractor : ExtractStep<MyInput, MyContext>
{
    public override Task<(BusinessStepResult<MyInput> Result, MyContext Context)> ExecuteAsync(MyInput input,
        MyContext context, CancellationToken ct)
    {
        return Task.FromResult<(BusinessStepResult<MyInput> Result, MyContext Context)>((
            new BusinessStepResult<MyInput>.Success(input), context));
    }
}

public sealed class SchemaValidator : ValidateStep<MyInput, MyContext>
{
    public override Task<(BusinessStepResult<MyInput> Result, MyContext Context)> ExecuteAsync(MyInput input,
        MyContext context, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(input.OrderId) || input.OrderId == "_")
        {
            var bi = new BusinessIncidentData
            {
                Description = "OrderId is null or empty",
                Context = new Dictionary<string, string>()
            };
            return Task.FromResult<(BusinessStepResult<MyInput> Result, MyContext Context)>((
                new BusinessStepResult<MyInput>.Failure(bi), context));
        }

        return Task.FromResult<(BusinessStepResult<MyInput> Result, MyContext Context)>((
            new BusinessStepResult<MyInput>.Success(input), context));
    }
}

public sealed class MyBusinessTransformer : TransformStep<MyInput, MyOutput, MyContext>
{
    public override Task<(TechnicalStepResult<MyOutput> Result, MyContext Context)> ExecuteAsync(MyInput input,
        MyContext context, CancellationToken ct)
    {
        var mapped = new MyOutput
        {
            OrderId = input.OrderId
        };

        return Task.FromResult<(TechnicalStepResult<MyOutput> Result, MyContext Context)>((
            new TechnicalStepResult<MyOutput>.Success(mapped), context));
    }
}

public sealed class XmlSerializer : SerializeStep<MyOutput, MyContext>
{
    public override async Task<(TechnicalStepResult<string> Result, MyContext Context)> ExecuteAsync(MyOutput input,
        MyContext context, CancellationToken ct)
    {
        var xmlSerializer = new System.Xml.Serialization.XmlSerializer(typeof(MyOutput));
        await using var stringWriter = new StringWriter();
        xmlSerializer.Serialize(stringWriter, input);
        var serialized = stringWriter.ToString();

        return (new TechnicalStepResult<string>.Success(serialized), context);
    }
}

public sealed class HttpSender : SendStep<MyContext>
{
    public override Task<(BusinessStepResult<string> Result, MyContext Context)> ExecuteAsync(string input,
        MyContext context, CancellationToken ct)
    {
        return Task.FromResult<(BusinessStepResult<string> Result, MyContext Context)>((
            new BusinessStepResult<string>.Success(input), context));
    }
}

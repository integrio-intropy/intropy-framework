using System.Diagnostics;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps.External;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using NSubstitute;

namespace Intropy.Framework.Blocks.Test.Shared.Steps.External;

public class ExternalBusinessIncidentRouterTests
{
    private const string ContextKeysMessageId = "MessageId";
    private const string ContextKeysSubject = "Subject";

    private readonly IBusinessIncidentServiceClient _mockClient;
    private readonly FrameworkOptions _frameworkOptions;
    private readonly Func<Context, string> _messageIdExtractor;
    private readonly Func<Context, string> _subjectExtractor;
    private readonly Func<string> _defaultValueFactory;

    public ExternalBusinessIncidentRouterTests()
    {
        _mockClient = Substitute.For<IBusinessIncidentServiceClient>();
        _frameworkOptions = new FrameworkOptions { ComponentName = "TestComponent", ServiceNamespace = "TestNamespace" };
        _messageIdExtractor = ctx => ctx.Metadata[ContextKeysMessageId];
        _subjectExtractor = ctx => ctx.Metadata[ContextKeysSubject];
        _defaultValueFactory = () => "";
    }

    [Fact]
    public void Constructor_WithAllValidParameters_CreatesInstance()
    {
        // Arrange & Act
        var router = new ExternalBusinessIncidentRouter<string, Context>(
            _mockClient,
            new FrameworkOptions { ComponentName = "TestComponent",  ServiceNamespace = "TestNamespace" },
            ctx => ctx.Metadata[ContextKeysMessageId],
            ctx => ctx.Metadata[ContextKeysSubject],
            () => ""
        );

        // Assert
        Assert.NotNull(router);
    }

    [Fact]
    public async Task ExecuteAsync_WithBusinessFailure_RoutesBusinessIncident()
    {
        // Arrange
        var router = CreateRouter();
        var stepContext = GetContext("123");
        var biContext = new Dictionary<string, string>();
        var businessIncident = new BusinessIncidentData { Description = "failure", Context = biContext };
        var result = new StepResult<string>.BusinessFailure(businessIncident);

        // Act
        await router.ExecuteAsync(result, stepContext, CancellationToken.None);

        // Assert
        await _mockClient.Received(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task ExecuteAsync_WithSuccessNotRetry_DoesNothing()
    {
        // Arrange
        var router = CreateRouter();
        var stepContext = GetContext("123");
        var result = new StepResult<string>.Success("");

        // Act
        await router.ExecuteAsync(result, stepContext, CancellationToken.None);

        // Assert
        await _mockClient.DidNotReceive().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        await _mockClient.DidNotReceive().Resolve(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task ExecuteAsync_WithSuccessAndRetry_Resolves()
    {
        // Arrange
        var router = CreateRouter();
        var stepContext = GetContext("123") with { IsRetry = true };
        var result = new StepResult<string>.Success("");

        // Act
        await router.ExecuteAsync(result, stepContext, CancellationToken.None);

        // Assert
        await _mockClient.DidNotReceive().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        await _mockClient.Received(1).Resolve(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task ExecuteAsync_WithInvalidResult_Throws()
    {
        // Arrange
        var router = CreateRouter();
        var stepContext = GetContext("123") with { IsRetry = true };
        var result = new StepResult<string>.Cancelled();

        // Act & Assert
        await Assert.ThrowsAsync<UnreachableException>(async () => await router.ExecuteAsync(result, stepContext, CancellationToken.None));
    }

    [Fact]
    public async Task ExecuteAsync_WithBusinessFailure_RoutesBusinessIncidentWithCorrectValues()
    {
        // Arrange
        const string expectedSubject = "123";
        const string expectedId = "123";
        var expectedSource = new Uri("urn:testnamespace:testcomponent");
        
        var router = CreateRouter();
        var stepContext = GetContext("123");
        var result = new StepResult<string>.BusinessFailure(new BusinessIncidentData { Description = "failure", Context = []});
        
        // Act
        await router.ExecuteAsync(result, stepContext, CancellationToken.None);
        
        // Assert
        await _mockClient.Received(1).Trigger(
            Arg.Is<Uri>(x => x.Equals(expectedSource)),
            Arg.Is<string>(x => x.Equals(expectedSubject, StringComparison.Ordinal)),
            Arg.Is<string>(x => x.Equals(expectedId, StringComparison.Ordinal)),
            Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }


    private ExternalBusinessIncidentRouter<string, Context> CreateRouter()
    {
        return new ExternalBusinessIncidentRouter<string, Context>(
            _mockClient,
            _frameworkOptions,
            _messageIdExtractor,
            _subjectExtractor,
            _defaultValueFactory
        );
    }

    private static Context GetContext(string id)
    {
        var metadata = new Dictionary<string, string> { { ContextKeysMessageId, id }, { ContextKeysSubject, id } };
        return new Context(metadata);
    }
}

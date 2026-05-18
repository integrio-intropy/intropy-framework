using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Core.Test.Pipeline.Abstractions.Results;

public class StepResultTests
{
    private static readonly TechnicalFailure Failure = new("");
    private static readonly BusinessIncidentData Incident = new() {Description = "", Context =[]};
    
    public static TheoryData<(StepResult<string>, string)> ValidResults =>
    [
        (new StepResult<string>.Success(""), "success"),
        (new StepResult<string>.Cancelled(), "cancelled"),
        (new StepResult<string>.TechnicalFailure(Failure), "technical_failure"),
        (new StepResult<string>.BusinessFailure(Incident), "business_failure"),
        (new StepResult<string>.Aborted(), "aborted")
    ];
    
    [Theory]
    [MemberData(nameof(ValidResults))]
    public void GetResultType_ShouldConvertToString_WhenInputIsValid((StepResult<string>, string) input)
    {
        // Arrange
        var (inputResult, expectedType) = input;
        
        // Act
        var result = inputResult.GetResultType();
        
        // Assert
        Assert.Equal(expectedType, result);
    }
}

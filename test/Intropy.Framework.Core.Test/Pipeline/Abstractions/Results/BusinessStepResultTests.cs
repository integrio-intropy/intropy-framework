using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Core.Test.Pipeline.Abstractions.Results;

public class BusinessStepResultTests
{
    private static readonly BusinessIncidentData Incident = new() { Description = "", Context = [] };

    public static TheoryData<(BusinessStepResult<string>, StepResult<string>, string)> ValidResults => new()
    {
        (new BusinessStepResult<string>.Success(""), new StepResult<string>.Success(""), "success"),
        (new BusinessStepResult<string>.Cancelled(), new StepResult<string>.Cancelled(), "cancelled"),
        (new BusinessStepResult<string>.Failure(Incident), new StepResult<string>.BusinessFailure(Incident), "business_failure"),
        (new BusinessStepResult<string>.Aborted(), new StepResult<string>.Aborted(), "aborted")
    };

    [Theory]
    [MemberData(nameof(ValidResults))]
    public void ToStepResult_ShouldConvertToStepResult_WhenInputIsValid((BusinessStepResult<string>, StepResult<string>, string) input)
    {
        // Arrange
        var (inputResult, expectedResult, _) = input;
        
        // Act
        var result = inputResult.ToStepResult();
        
        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [MemberData(nameof(ValidResults))]
    public void GetResultType_ShouldConvertToString_WhenInputIsValid((BusinessStepResult<string>, StepResult<string>, string) input)
    {
        // Arrange
        var (inputResult, _, expectedType) = input;
        
        // Act
        var result = inputResult.GetResultType();
        
        // Assert
        Assert.Equal(expectedType, result);
    }

    [Fact]
    public void GetResultType_ShouldThrow_WhenInputIsNotValid()
    {
        // Arrange
        var inputResult = new TestBusinessStepUnknownResult<string>();

        // Act & Assert
        Assert.Throws<InvalidOperationException>(
            () => inputResult.ToStepResult()
        );
    }
}

internal sealed record TestBusinessStepUnknownResult<T> : BusinessStepResult<T>;

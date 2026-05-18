using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Core.Test.Pipeline.Abstractions.Results;

public class TechnicalFailureTests
{
    private static readonly TechnicalFailure Failure = new("");

    public static TheoryData<(TechnicalStepResult<string>, StepResult<string>, string)> ValidResults => new()
    {
        (new TechnicalStepResult<string>.Success(""), new StepResult<string>.Success(""), "success"),
        (new TechnicalStepResult<string>.Cancelled(), new StepResult<string>.Cancelled(), "cancelled"),
        (new TechnicalStepResult<string>.Failure(Failure), new StepResult<string>.TechnicalFailure(Failure), "technical_failure"),
        (new TechnicalStepResult<string>.Aborted(), new StepResult<string>.Aborted(), "aborted")
    };

    [Theory]
    [MemberData(nameof(ValidResults))]
    public void ToStepResult_ShouldConvertToStepResult_WhenInputIsValid((TechnicalStepResult<string>, StepResult<string>, string) input)
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
    public void GetResultType_ShouldConvertToString_WhenInputIsValid((TechnicalStepResult<string>, StepResult<string>, string) input)
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
        var inputResult = new TestTechnicalStepUnknownResult<string>();

        // Act & Assert
        Assert.Throws<InvalidOperationException>(
            () => inputResult.ToStepResult()
        );
    }
}

internal sealed record TestTechnicalStepUnknownResult<T> : TechnicalStepResult<T>;

using Intropy.Framework.Core.Pipeline.Abstractions.Failures;

namespace Intropy.Framework.Core.Test.Pipeline.Abstractions.Failures;

public class TechnicalFailureTests
{
    [Fact]
    public void FromExceptionInPipeline_ShouldCreateTechnicalFailure()
    {
        // Arrange
        const string message = "Exception when getting file...";
        const string pipelineName = "pipeline1";
        var exception = new IOException(message);

        // Act
        var result = TechnicalFailure.FromExceptionInPipeline(exception, pipelineName);

        // Assert
        Assert.NotNull(result.Exception);
        Assert.Equal(message, result.Exception.Message);
    }

    [Fact]
    public void FromExceptionInStep_ShouldCreateTechnicalFailure()
    {
        // Arrange
        const string message = "Exception when getting file...";
        const string stepName = "step1";
        var exception = new IOException(message);

        // Act
        var result = TechnicalFailure.FromExceptionInStep(exception, stepName);

        // Assert
        Assert.NotNull(result.Exception);
        Assert.Equal(message, result.Exception.Message);
    }
}

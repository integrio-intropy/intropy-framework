using Intropy.Framework.Core.Pipeline.Abstractions.Enums;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Core.Pipeline.Core;

/// <summary>
/// Core pipeline functionality.
/// </summary>
public static class Pipeline
{
    /// <summary>
    /// Starts a pipeline execution with an initial value and an initial context.
    /// </summary>
    /// <param name="value">The value to start the pipeline with</param>
    /// <param name="context">The context to start the pipeline with</param>
    /// <param name="ct">A cancellation token that flows through the pipeline</param>
    /// <typeparam name="T">The type of the value that's used to start the pipeline</typeparam>
    /// <typeparam name="TCtx">The type of the context</typeparam>
    /// <returns></returns>
    public static Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> Start<T, TCtx>(
        T value, TCtx context, CancellationToken ct = default)
    {
        return Task.FromResult<(StepResult<T>, TCtx, CancellationToken)>(
            (new StepResult<T>.Success(value), context, ct));
    }

    /// <summary>
    /// Adds a <see cref="Step{TIn,TOut,TCtx}"/> to the pipeline. Will only execute if the previous step succeeds.
    /// </summary>
    /// <param name="input">The previous step</param>
    /// <param name="step">The step to add</param>
    /// <typeparam name="TIn">The type that enters the step</typeparam>
    /// <typeparam name="TOut">The type that exits the step</typeparam>
    /// <typeparam name="TCtx">The type of context used in the step</typeparam>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when the result type of the previous step is of unknown type</exception>
    public static async Task<(StepResult<TOut> Result, TCtx Context, CancellationToken CancellationToken)> AddStep<TIn, TOut, TCtx>(
        this Task<(StepResult<TIn> Result, TCtx Context, CancellationToken CancellationToken)> input,
        Step<TIn, TOut, TCtx> step)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(step);

        var (result, context, ct) = await input;

        return result switch
        {
            StepResult<TIn>.Success s => await ExecuteStep(step, s.Value, context, ct),
            StepResult<TIn>.Cancelled => (new StepResult<TOut>.Cancelled(), context, ct),
            StepResult<TIn>.BusinessFailure bf => (new StepResult<TOut>.BusinessFailure(bf.Value), context, ct),
            StepResult<TIn>.TechnicalFailure tf => (new StepResult<TOut>.TechnicalFailure(tf.Value), context, ct),
            StepResult<TIn>.Aborted => (new StepResult<TOut>.Aborted(), context, ct),
            _ => throw new InvalidOperationException("Unknown result type")
        };

        static async Task<(StepResult<TOut>, TCtx, CancellationToken)> ExecuteStep(
            Step<TIn, TOut, TCtx> step, TIn value, TCtx context, CancellationToken ct)
        {
            var (stepResult, newContext) = await step.ExecuteAsyncInternal(value, context, ct);
            return (stepResult, newContext, ct);
        }
    }

    /// <summary>
    /// Adds a <see cref="BusinessStep{TIn,TOut,TCtx}"/> to the pipeline. Will only execute if the previous step succeeds.
    /// </summary>
    /// <param name="input">The previous step</param>
    /// <param name="step">The step to add</param>
    /// <typeparam name="TIn">The type that enters the step</typeparam>
    /// <typeparam name="TOut">The type that exits the step</typeparam>
    /// <typeparam name="TCtx">The type of context used in the step</typeparam>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when the result type of the previous step is of unknown type</exception>
    public static async Task<(StepResult<TOut> Result, TCtx Context, CancellationToken CancellationToken)> AddStep<TIn, TOut, TCtx>(
        this Task<(StepResult<TIn> Result, TCtx Context, CancellationToken CancellationToken)> input,
        BusinessStep<TIn, TOut, TCtx> step)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(step);

        var (result, context, ct) = await input;

        return result switch
        {
            StepResult<TIn>.Success s => await ExecuteStep(step, s.Value, context, ct),
            StepResult<TIn>.Cancelled => (new StepResult<TOut>.Cancelled(), context, ct),
            StepResult<TIn>.BusinessFailure bf => (new StepResult<TOut>.BusinessFailure(bf.Value), context, ct),
            StepResult<TIn>.TechnicalFailure tf => (new StepResult<TOut>.TechnicalFailure(tf.Value), context, ct),
            StepResult<TIn>.Aborted => (new StepResult<TOut>.Aborted(), context, ct),
            _ => throw new InvalidOperationException("Unknown result type")
        };

        static async Task<(StepResult<TOut>, TCtx, CancellationToken)> ExecuteStep(
            BusinessStep<TIn, TOut, TCtx> step, TIn value, TCtx context, CancellationToken ct)
        {
            var (businessStepResult, newContext) = await step.ExecuteAsyncInternal(value, context, ct);
            return (businessStepResult.ToStepResult(), newContext, ct);
        }
    }

    /// <summary>
    /// Adds a <see cref="TechnicalStep{TIn,TOut,TCtx}"/> to the pipeline. Will only execute if the previous step succeeds.
    /// </summary>
    /// <param name="input">The previous step</param>
    /// <param name="step">The step to add</param>
    /// <typeparam name="TIn">The type that enters the step</typeparam>
    /// <typeparam name="TOut">The type that exits the step</typeparam>
    /// <typeparam name="TCtx">The type of context used in the step</typeparam>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when the result type of the previous step is of unknown type</exception>
    public static async Task<(StepResult<TOut> Result, TCtx Context, CancellationToken CancellationToken)> AddStep<TIn, TOut, TCtx>(
        this Task<(StepResult<TIn> Result, TCtx Context, CancellationToken CancellationToken)> input,
        TechnicalStep<TIn, TOut, TCtx> step)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(step);

        var (result, context, ct) = await input;

        return result switch
        {
            StepResult<TIn>.Success s => await ExecuteStep(step, s.Value, context, ct),
            StepResult<TIn>.Cancelled => (new StepResult<TOut>.Cancelled(), context, ct),
            StepResult<TIn>.BusinessFailure bf => (new StepResult<TOut>.BusinessFailure(bf.Value), context, ct),
            StepResult<TIn>.TechnicalFailure tf => (new StepResult<TOut>.TechnicalFailure(tf.Value), context, ct),
            StepResult<TIn>.Aborted => (new StepResult<TOut>.Aborted(), context, ct),
            _ => throw new InvalidOperationException("Unknown result type")
        };

        static async Task<(StepResult<TOut>, TCtx, CancellationToken)> ExecuteStep(
            TechnicalStep<TIn, TOut, TCtx> step, TIn value, TCtx context, CancellationToken ct)
        {
            var (technicalStepResult, newContext) = await step.ExecuteAsyncInternal(value, context, ct);
            return (technicalStepResult.ToStepResult(), newContext, ct);
        }
    }

    /// <summary>
    /// Adds multiple <see cref="BusinessStep{T,T,TCtx}"/> to the pipeline.
    /// Steps are executed in order. Will short-circuit if any step fails.
    /// </summary>
    public static async Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> AddSteps<T, TCtx>(
        this Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> input,
        IEnumerable<BusinessStep<T, T, TCtx>> steps)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(steps);

        var current =
            steps.Aggregate(input, (current1, step) => current1.AddStep(step));

        return await current;
    }

    /// <summary>
    /// Adds multiple <see cref="TechnicalStep{T,T,TCtx}"/> to the pipeline.
    /// Steps are executed in order. Will short-circuit if any step fails.
    /// </summary>
    public static async Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> AddSteps<T, TCtx>(
        this Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> input,
        IEnumerable<TechnicalStep<T, T, TCtx>> steps)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(steps);

        var current =
            steps.Aggregate(input, (current1, step) => current1.AddStep(step));

        return await current;
    }

    /// <summary>
    /// Adds a <see cref="Finalizer{T,TCtx}"/> to the pipeline. Will only execute based on the configured finalizer trigger flag.
    /// </summary>
    /// <param name="input">The previous step</param>
    /// <param name="finalizer">The finalizer to add</param>
    /// <typeparam name="TCtx">The type of context used in the finalizer</typeparam>
    /// <typeparam name="T">The type of the value that enters the finalizer</typeparam>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when the result type of the previous step is of unknown type</exception>
    public static async Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> AddFinalizer<T,
        TCtx>(this Task<(StepResult<T> Result, TCtx Context, CancellationToken CancellationToken)> input,
        Finalizer<T, TCtx> finalizer)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(finalizer);

        var (result, context, ct) = await input;

        if (!ShouldRunFinalizer(finalizer.Triggers, result))
            return (result, context, ct);

        var (finalizerResult, newContext) = await finalizer.ExecuteAsyncInternal(result, context, ct);
        return (finalizerResult, newContext, ct);
    }

    /// <summary>
    /// Determines if a finalizer should be executed.
    /// </summary>
    /// <param name="triggers">The triggers configured for a finalizer.</param>
    /// <param name="result">The result of the previous step.</param>
    /// <typeparam name="T">The type of the return value from previous step.</typeparam>
    /// <returns></returns>
    private static bool ShouldRunFinalizer<T>(FinalizerTrigger triggers, StepResult<T> result)
    {
        return result switch
        {
            StepResult<T>.Success => triggers.HasFlag(FinalizerTrigger.OnSuccess),
            StepResult<T>.Cancelled => triggers.HasFlag(FinalizerTrigger.OnCancelled),
            StepResult<T>.BusinessFailure => triggers.HasFlag(FinalizerTrigger.OnBusinessFailure),
            StepResult<T>.TechnicalFailure => triggers.HasFlag(FinalizerTrigger.OnTechnicalFailure),
            StepResult<T>.Aborted => triggers.HasFlag(FinalizerTrigger.OnAborted),
            _ => false
        };
    }
}

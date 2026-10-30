namespace Intropy.Framework.Hosting.Jobs;

/// <summary>
/// When <see cref="JobRunner"/> shuts the Dapr sidecar down after the job. Match it to how the
/// scheduler retries a failed run.
/// </summary>
public enum SidecarShutdown
{
    /// <summary>
    /// After every run. For a scheduler that retries in a new pod (Kubernetes
    /// <c>restartPolicy: Never</c>): the pod only completes once the sidecar has stopped.
    /// </summary>
    Always,

    /// <summary>
    /// Only after a run that exits <see cref="JobExitCodes.Success"/>. For a scheduler that
    /// restarts the app in the same pod (<c>restartPolicy: OnFailure</c>): a sidecar that exited
    /// cleanly is not restarted, so the retry would find none. Under <c>restartPolicy: Never</c>
    /// a failed run then leaves the sidecar running and the pod never completes.
    /// </summary>
    OnSuccess,
}

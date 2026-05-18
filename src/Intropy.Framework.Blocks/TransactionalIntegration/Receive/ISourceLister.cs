namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive;

/// <summary>
/// Interface for listing available items to process.
/// </summary>
public interface ISourceLister
{
    /// <summary>
    /// Lists available items to process
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A result containing the list of items or a failure.</returns>
    Task<IReadOnlyList<SourceItemInfo>> ListItemsAsync(CancellationToken cancellationToken = default);
}

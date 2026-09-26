namespace Intropy.Framework.Adapters.File;

/// <summary>
/// The Dapr binding a configured file transport speaks. The names match the topology's port
/// binding kinds, so a deployment can pass a port's binding through unchanged.
/// </summary>
public enum FileAdapterKind
{
    /// <summary>A local folder: the Dapr <c>bindings.localstorage</c> binding.</summary>
    Local,
    /// <summary>The Dapr <c>bindings.sftp</c> binding.</summary>
    Sftp,
    /// <summary>Azure Blob Storage: the Dapr <c>bindings.azure.blobstorage</c> binding.</summary>
    AzureBlob
}

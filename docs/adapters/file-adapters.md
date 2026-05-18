# File Adapters

> File system access through Dapr bindings for SFTP, local files, and Azure Blob Storage.

**Namespace:** `Intropy.Framework.Adapters.File`
**Assembly:** `Intropy.Framework.Adapters`

## IFileAdapter

Interface for all file operations. All implementations use Dapr bindings under the hood.

```csharp
public interface IFileAdapter
{
    Task<List<FileInfo>> ListAsync();
    Task<byte[]> GetContentAsync(string fileName);
    Task<string?> GetContentAsync(string fileName, Encoding encoding);
    Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null);
    Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null);
    Task DeleteAsync(string fileName);
}
```

### Methods

| Method | Description |
|--------|-------------|
| `ListAsync()` | Lists files matching the configured filter. Returns `List<FileInfo>`. |
| `GetContentAsync(fileName)` | Reads file content as `byte[]`. |
| `GetContentAsync(fileName, encoding)` | Reads file content as `string` with the specified encoding. |
| `WriteAsync(fileName, content)` | Writes `byte[]` content. Optional `basePathOverride` to write to a different directory. |
| `WriteAsync(fileName, content, encoding)` | Writes `string` content with encoding. |
| `DeleteAsync(fileName)` | Deletes a file. |

---

## FileAdapterOptions

Configuration for file adapters.

```csharp
public class FileAdapterOptions
{
    public string DaprBindingName { get; }
    public string BasePath { get; }
    public Regex? FileNameRegex { get; }
}
```

| Property | Type | Description |
|----------|------|-------------|
| `DaprBindingName` | `string` | Name of the Dapr binding component |
| `BasePath` | `string` | Base directory path for file operations |
| `FileNameRegex` | `Regex?` | Optional regex to filter files in `ListAsync()` |

---

## FileInfo

**Namespace:** `Intropy.Framework.Adapters.Common`

```csharp
public record FileInfo(string FileName);
```

Returned by `IFileAdapter.ListAsync()`. Contains the file name.

---

## Implementations

### SftpAdapter

Connects to SFTP servers via a Dapr SFTP binding component.

```csharp
var adapter = new SftpAdapter(daprClient, new FileAdapterOptions(
    daprBindingName: "sftp-binding",
    basePath: "/incoming/orders",
    fileNameRegex: new Regex(@"\.csv$")));
```

Filters the listing response to exclude directories (`isDirectory: false`).

### LocalFileAdapter

Reads from local or mounted file systems (e.g., SMB shares) via a Dapr local storage binding.

```csharp
var adapter = new LocalFileAdapter(daprClient, new FileAdapterOptions(
    daprBindingName: "local-storage",
    basePath: "/mnt/data/orders"));
```

### AzureBlobStorageAdapter

Reads from Azure Blob Storage via a Dapr Azure Blob Storage binding.

```csharp
var adapter = new AzureBlobStorageAdapter(daprClient, new FileAdapterOptions(
    daprBindingName: "blob-storage",
    basePath: "orders/incoming"));
```

Uses the `blobName` metadata key for blob operations and handles path prefix normalization.

---

## Usage with Transactional Integration

File adapters are commonly used in receive pipelines to list and read source files:

```csharp
// Register the adapter
builder.Services.AddSingleton<IFileAdapter>(sp =>
    new SftpAdapter(
        sp.GetRequiredService<DaprClient>(),
        new FileAdapterOptions("sftp-binding", "/incoming", new Regex(@"\.xml$"))));

// Use in source lister
public class FileSourceLister(IFileAdapter fileAdapter) : ISourceLister
{
    public async Task<List<SourceItemInfo>> ListSourceItemsAsync()
    {
        var files = await fileAdapter.ListAsync();
        return files.Select(f => new SourceItemInfo(f.FileName)).ToList();
    }
}

// Use in receive step
public class FileReceiver(IFileAdapter fileAdapter) : ReceiveStep<Context>
{
    public override async Task<(BusinessStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
        SourceItemInfo input, Context context)
    {
        var content = await fileAdapter.GetContentAsync(input.Id);
        return (new BusinessStepResult<SourceItem>.Success(new SourceItem(input.Id, content)), context);
    }
}
```

## See also

- [Transactional Integration](../blocks/transactional-integration.md) — receive pipeline architecture
- [Builders](../core/builders.md) — pipeline builder configuration

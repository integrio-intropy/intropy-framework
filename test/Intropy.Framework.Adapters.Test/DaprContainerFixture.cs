using System.Globalization;
using Dapr.Client;
using Docker.DotNet;
using Docker.DotNet.Models;

namespace Intropy.Framework.Adapters.Test;

/// <summary>
/// Test fixture that starts a Dapr sidecar container with local storage binding configured.
/// </summary>
public class DaprContainerFixture : IAsyncLifetime
{
    private const int DaprHttpPort = 3500;
    private const int DaprGrpcPort = 50001;
    private const string StoragePath = "/storage";
    private const string ComponentsPath = "/components";
    private const string BindingNameConst = "local-storage";

    private string? _containerId;
    private string? _componentsTempDir;
    private string? _storageTempDir;
    private DockerClient? _dockerClient;

    public DaprClient DaprClient { get; private set; } = null!;
    public static string BindingName => BindingNameConst;

    public async Task InitializeAsync()
    {
        // Create temp directories on host
        var uniqueId = Guid.NewGuid().ToString("N");
        _componentsTempDir = Path.Combine(Path.GetTempPath(), $"dapr-components-{uniqueId}");
        _storageTempDir = Path.Combine(Path.GetTempPath(), $"dapr-storage-{uniqueId}");
        Directory.CreateDirectory(_componentsTempDir);
        Directory.CreateDirectory(_storageTempDir);

        var componentYaml = $"""
                             apiVersion: dapr.io/v1alpha1
                             kind: Component
                             metadata:
                               name: {BindingNameConst}
                             spec:
                               type: bindings.localstorage
                               version: v1
                               metadata:
                                 - name: rootPath
                                   value: "{StoragePath}"
                             """;

        await System.IO.File.WriteAllTextAsync(Path.Combine(_componentsTempDir, "local-storage.yaml"), componentYaml);

        _dockerClient = new DockerClientBuilder().Build();

        // Convert Windows paths to Docker-compatible format
        var componentsHostPath = ConvertToDockerPath(_componentsTempDir);
        var storageHostPath = ConvertToDockerPath(_storageTempDir);

        // Create container with volume mount
        var createResponse = await _dockerClient.Containers.CreateContainerAsync(new CreateContainerParameters
        {
            Image = "daprio/daprd:1.16.1",
            Cmd =
            [
                "./daprd",
                "--app-id", "test-app",
                "--dapr-http-port", DaprHttpPort.ToString(),
                "--dapr-grpc-port", DaprGrpcPort.ToString(),
                "--resources-path", ComponentsPath,
                "--log-level", "info"
            ],
            ExposedPorts = new Dictionary<string, EmptyStruct>
            {
                [$"{DaprHttpPort}/tcp"] = default, [$"{DaprGrpcPort}/tcp"] = default
            },
            HostConfig = new HostConfig
            {
                Binds =
                [
                    $"{componentsHostPath}:{ComponentsPath}:ro",
                    $"{storageHostPath}:{StoragePath}"
                ],
                PortBindings = new Dictionary<string, IList<PortBinding>>
                {
                    [$"{DaprHttpPort}/tcp"] = new List<PortBinding> { new() { HostPort = "0" } },
                    [$"{DaprGrpcPort}/tcp"] = new List<PortBinding> { new() { HostPort = "0" } }
                }
            }
        });

        _containerId = createResponse.ID;

        // Start container
        await _dockerClient.Containers.StartContainerAsync(_containerId, new ContainerStartParameters());

        // Wait a moment for container to start
        await Task.Delay(2000);

        // Check if container is still running
        var inspect = await _dockerClient.Containers.InspectContainerAsync(_containerId);
        if (!inspect.State!.Running)
        {
            var logStream = await _dockerClient.Containers.GetContainerLogsAsync(_containerId,
                new ContainerLogsParameters { ShowStdout = true, ShowStderr = true }, CancellationToken.None);
            var (stdout, stderr) = await logStream.ReadOutputToEndAsync(CancellationToken.None);
            throw new Exception(
                $"Container exited immediately. Exit code: {inspect.State.ExitCode}\nStdout: {stdout}\nStderr: {stderr}");
        }

        var httpPort = int.Parse(inspect.NetworkSettings!.Ports[$"{DaprHttpPort}/tcp"][0].HostPort);
        var grpcPort = int.Parse(inspect.NetworkSettings.Ports[$"{DaprGrpcPort}/tcp"][0].HostPort);

        // Wait for Dapr to be fully ready by polling the health endpoint
        using var httpClient = new HttpClient();
        httpClient.BaseAddress = new Uri($"http://localhost:{httpPort}");
        await WaitForHealthAsync(httpClient);

        DaprClient = new DaprClientBuilder()
            .UseHttpEndpoint($"http://localhost:{httpPort}")
            .UseGrpcEndpoint($"http://localhost:{grpcPort}")
            .Build();
    }

    public async Task DisposeAsync()
    {
        DaprClient.Dispose();

        if (_dockerClient is not null && _containerId is not null)
        {
            try
            {
                await _dockerClient.Containers.StopContainerAsync(_containerId, new ContainerStopParameters());
                await _dockerClient.Containers.RemoveContainerAsync(_containerId, new ContainerRemoveParameters());
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        _dockerClient?.Dispose();

        if (_componentsTempDir is not null && Directory.Exists(_componentsTempDir))
        {
            try { Directory.Delete(_componentsTempDir, recursive: true); }
            catch
            {
                // ignored
            }
        }

        if (_storageTempDir is not null && Directory.Exists(_storageTempDir))
        {
            try { Directory.Delete(_storageTempDir, recursive: true); }
            catch
            {
                // ignored
            }
        }
    }

    private static string ConvertToDockerPath(string windowsPath)
    {
        var path = windowsPath.Replace("\\", "/");
        if (path is [_, ':', ..])
        {
            // Convert C:/path to /c/path for Docker on Windows
            path = "/" + char.ToLower(path[0], CultureInfo.InvariantCulture) + path[2..];
        }

        return path;
    }

    private async Task WaitForHealthAsync(HttpClient httpClient, int maxRetries = 60, int delayMs = 1000)
    {
        Exception? lastException = null;

        for (var i = 0; i < maxRetries; i++)
        {
            try
            {
                var response = await httpClient.GetAsync("/v1.0/healthz");
                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (Exception ex)
            {
                lastException = ex;
            }

            await Task.Delay(delayMs);
        }

        // Get container logs for debugging
        var logs = "";
        if (_dockerClient is null || _containerId is null)
            throw new TimeoutException(
                $"Dapr sidecar did not become healthy in time.\n" +
                $"Last error: {lastException?.Message}\n" +
                $"Container logs:\n{logs}");
        var logStream = await _dockerClient.Containers.GetContainerLogsAsync(_containerId,
            new ContainerLogsParameters { ShowStdout = true, ShowStderr = true }, CancellationToken.None);
        var (stdout, stderr) = await logStream.ReadOutputToEndAsync(CancellationToken.None);
        logs = $"stdout: {stdout}\nstderr: {stderr}";

        throw new TimeoutException(
            $"Dapr sidecar did not become healthy in time.\n" +
            $"Last error: {lastException?.Message}\n" +
            $"Container logs:\n{logs}");
    }
}

/// <summary>
/// Collection definition for sharing the Dapr container across tests.
/// </summary>
[CollectionDefinition(nameof(DaprContainerCollection))]
public class DaprContainerCollection : ICollectionFixture<DaprContainerFixture>;

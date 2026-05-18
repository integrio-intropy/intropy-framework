using CloudNative.CloudEvents;
using Intropy.Framework.EventDispatcher.Abstractions;

namespace Intropy.Framework.EventDispatcher.Test;

public record TestOrderData(string OrderId, decimal Amount);

public record TestInvoiceData(string InvoiceId);

[CloudEventType("order.created")]
public class OrderCreatedHandler : ICloudEventHandler<TestOrderData>
{
    public bool WasCalled { get; private set; }
    public TestOrderData? ReceivedData { get; private set; }
    public CloudEvent? ReceivedEvent { get; private set; }
    public CancellationToken ReceivedCt { get; private set; }

    public Task HandleAsync(TestOrderData data, CloudEvent cloudEvent, CancellationToken ct = default)
    {
        WasCalled = true;
        ReceivedData = data;
        ReceivedEvent = cloudEvent;
        ReceivedCt = ct;
        return Task.CompletedTask;
    }
}

[CloudEventType("invoice.created")]
public class InvoiceCreatedHandler : ICloudEventHandler<TestInvoiceData>
{
    public bool WasCalled { get; private set; }

    public Task HandleAsync(TestInvoiceData data, CloudEvent cloudEvent, CancellationToken ct = default)
    {
        WasCalled = true;
        return Task.CompletedTask;
    }
}

[CloudEventType("order.created")]
public class DuplicateOrderHandler : ICloudEventHandler<TestOrderData>
{
    public Task HandleAsync(TestOrderData data, CloudEvent cloudEvent, CancellationToken ct = default)
    {
        return Task.CompletedTask;
    }
}

[CloudEventType("throws.sync")]
public class SyncThrowingHandler : ICloudEventHandler<TestOrderData>
{
    public Task HandleAsync(TestOrderData data, CloudEvent cloudEvent, CancellationToken ct = default)
    {
        throw new InvalidOperationException("Sync handler failure");
    }
}

[CloudEventType("throws.async")]
public class AsyncThrowingHandler : ICloudEventHandler<TestOrderData>
{
    public async Task HandleAsync(TestOrderData data, CloudEvent cloudEvent, CancellationToken ct = default)
    {
        await Task.Yield();
        throw new InvalidOperationException("Async handler failure");
    }
}

// Handler without the attribute - should be ignored by registry
public class HandlerWithoutAttribute : ICloudEventHandler<TestOrderData>
{
    public Task HandleAsync(TestOrderData data, CloudEvent cloudEvent, CancellationToken ct = default)
    {
        return Task.CompletedTask;
    }
}

// Class with attribute but not implementing the interface - should be ignored
[CloudEventType("no.interface")]
public class ClassWithAttributeButNoInterface
{
}

// Abstract handler - should be ignored
[CloudEventType("abstract.handler")]
public abstract class AbstractHandler : ICloudEventHandler<TestOrderData>
{
    public abstract Task HandleAsync(TestOrderData data, CloudEvent cloudEvent, CancellationToken ct = default);
}

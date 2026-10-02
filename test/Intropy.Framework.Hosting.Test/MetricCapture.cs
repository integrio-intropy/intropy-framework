using System.Diagnostics.Metrics;

namespace Intropy.Framework.Hosting.Test;

/// <summary>Records every measurement of the Hosting meter while it is alive. Measurements from
/// tests running in parallel are recorded too: filter on a tag value unique to the test.</summary>
internal sealed class MetricCapture : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly List<Measurement> _measurements = [];

    public MetricCapture()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Intropy.Framework.Hosting")
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.Start();
    }

    /// <summary>Records the current value of every observable instrument.</summary>
    public void RecordObservableInstruments() => _listener.RecordObservableInstruments();

    /// <summary>The measurements of <paramref name="instrument"/> tagged <paramref name="tag"/> =
    /// <paramref name="value"/>.</summary>
    public List<Measurement> Of(string instrument, string tag, object value)
    {
        lock (_measurements)
            return _measurements.Where(m => m.Instrument == instrument && Equals(m.Tags.GetValueOrDefault(tag), value))
                .ToList();
    }

    public void Dispose() => _listener.Dispose();

    private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var measurement = new Measurement(instrument.Name, value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value));
        lock (_measurements)
            _measurements.Add(measurement);
    }

    internal sealed record Measurement(string Instrument, double Value, Dictionary<string, object?> Tags);
}

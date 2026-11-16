using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FireRakshak.FRMC.Core.Enums;
using FireRakshak.FRMC.Core.Models;
using FireRakshak.FRMC.Infrastructure.Persistence;
using Microsoft.Extensions.Logging;

namespace FireRakshak.FRMC.Managers.SystemConfigurationManager
{
    /// <summary>
    /// Manages persistence of alarm events and zone states to the EventHistory folder.
    /// Only persists state when alarms are triggered or cleared.
    /// </summary>
    public class EventHistoryService : IDisposable
    {
        private readonly ILogger<EventHistoryService> _logger;
        private readonly FileSystemPersistence _fileSystemPersistence;
        private const string EventHistoryFolderPath = "EventHistory";

        private const int QueueCapacity = 20_000;
        private const int MaxBatchSize = 500;
        private static readonly JsonSerializerOptions LineOptions =
            new(JsonSerializerSettings.DefaultOptions) { WriteIndented = false };
        private static readonly JsonSerializerOptions ReadOptions = JsonSerializerSettings.DefaultOptions;

        // Fully qualified: Core.Models also has a Channel class
        private readonly System.Threading.Channels.Channel<AlarmEventRecord> _queue =
            System.Threading.Channels.Channel.CreateBounded<AlarmEventRecord>(
                new System.Threading.Channels.BoundedChannelOptions(QueueCapacity)
                {
                    FullMode = System.Threading.Channels.BoundedChannelFullMode.DropWrite,
                    SingleReader = true
                });
        private readonly Task _writerTask;
        private long _droppedCount;

        public EventHistoryService(
            ILogger<EventHistoryService> logger,
            FileSystemPersistence fileSystemPersistence)
        {
            _logger = logger;
            _fileSystemPersistence = fileSystemPersistence;
            _writerTask = Task.Run(WriteQueuedEventsAsync);
        }

        private void Enqueue(AlarmEventRecord record)
        {
            if (!_queue.Writer.TryWrite(record))
            {
                long dropped = Interlocked.Increment(ref _droppedCount);
                if (dropped % 100 == 1)
                    _logger.LogError("Event history queue full; {Dropped} history records dropped so far", dropped);
            }
        }

        // Single background writer: appends one line per event, one disk write per batch
        private async Task WriteQueuedEventsAsync()
        {
            var batch = new List<AlarmEventRecord>(MaxBatchSize);
            while (await _queue.Reader.WaitToReadAsync())
            {
                while (batch.Count < MaxBatchSize && _queue.Reader.TryRead(out var r)) batch.Add(r);
                try
                {
                    foreach (var day in batch.GroupBy(r => r.Timestamp.ToString("yyyyMMdd")))
                    {
                        var sb = new StringBuilder();
                        foreach (var r in day) sb.AppendLine(JsonSerializer.Serialize(r, LineOptions));
                        await _fileSystemPersistence.AppendToFileAsync(
                            Path.Combine(EventHistoryFolderPath, $"alarm_log_{day.Key}.json"), sb.ToString());
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error writing {Count} event history records", batch.Count);
                }
                batch.Clear();
            }
        }

        public void Dispose()
        {
            _queue.Writer.TryComplete();
            _writerTask.Wait(TimeSpan.FromSeconds(5)); // flush on shutdown
        }

        /// <summary>
        /// Logs a system health fault event to the event history.
        /// </summary>
        public Task LogSystemFaultAsync(
            string faultType,
            bool isActive,
            double? currentValue,
            double? thresholdValue,
            string description,
            DateTime timestamp)
        {
            try
            {
                var eventRecord = new AlarmEventRecord
                {
                    Timestamp = timestamp,
                    ChannelId = 0, // Indicates system-level event
                    ZoneId = 0,
                    ZoneName = "System",
                    AlarmType = $"SystemHealth:{faultType}",
                    IsActive = isActive,
                    TriggerValue = currentValue,
                    BreachPosition = thresholdValue,
                    ZoneStateSnapshot = new ZoneStateSnapshotRecord
                    {
                        AverageTemperature = 0,
                        MaxTemperature = 0,
                        MinTemperature = 0,
                        MaxTempPosition = 0,
                        RateOfRise = 0,
                        Deviation = 0,
                        StartPoint = 0,
                        EndPoint = 0,
                        ActiveAlarms = new List<string>()
                    }
                };

                // Add description into ActiveAlarms so UI can show it
                eventRecord.ZoneStateSnapshot.ActiveAlarms.Add(description);

                // Caller never waits for the disk
                Enqueue(eventRecord);

                _logger.LogInformation(
                    "System health fault logged: Type={Type}, Active={Active}",
                    faultType,
                    isActive);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error logging system health fault {FaultType}",
                    faultType);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Logs an alarm event with the zone state snapshot to a daily JSON file.
        /// </summary>
        public Task LogAlarmEventAsync(
            int channelId,
            int zoneId,
            AlarmType alarmType,
            bool isActive,
            double? triggerValue,
            double? breachPosition,
            ZoneState zoneStateSnapshot,
            DateTime timestamp)
        {
            try
            {
                var eventRecord = new AlarmEventRecord
                {
                    Timestamp = timestamp,
                    ChannelId = channelId,
                    ZoneId = zoneId,
                    ZoneName = zoneStateSnapshot.Name,
                    AlarmType = alarmType.ToString(),
                    IsActive = isActive,
                    TriggerValue = triggerValue,
                    BreachPosition = breachPosition,
                    ZoneStateSnapshot = new ZoneStateSnapshotRecord
                    {
                        AverageTemperature = zoneStateSnapshot.AverageTemperature,
                        MaxTemperature = zoneStateSnapshot.MaxTemperature,
                        MinTemperature = zoneStateSnapshot.MinTemperature,
                        MaxTempPosition = zoneStateSnapshot.MaxTempPosition,
                        RateOfRise = zoneStateSnapshot.RateOfRise,
                        Deviation = zoneStateSnapshot.Deviation,
                        StartPoint = zoneStateSnapshot.StartPoint,
                        EndPoint = zoneStateSnapshot.EndPoint,
                        ActiveAlarms = new List<string>()
                    }
                };

                // Capture all active alarms
                if (zoneStateSnapshot.MaxAlarm) eventRecord.ZoneStateSnapshot.ActiveAlarms.Add("MaxTemperature");
                if (zoneStateSnapshot.MinAlarm) eventRecord.ZoneStateSnapshot.ActiveAlarms.Add("MinTemperature");
                if (zoneStateSnapshot.PreAlarm) eventRecord.ZoneStateSnapshot.ActiveAlarms.Add("PreAlarm");
                if (zoneStateSnapshot.RorAlarm) eventRecord.ZoneStateSnapshot.ActiveAlarms.Add("RateOfRise");
                if (zoneStateSnapshot.DeviationAlarm) eventRecord.ZoneStateSnapshot.ActiveAlarms.Add("Deviation");

                // Alarm path never waits for the disk
                Enqueue(eventRecord);

                _logger.LogInformation(
                    "Alarm event logged: Ch={ChannelId}, Z={ZoneId}, Type={Type}, Active={Active}",
                    channelId, zoneId, alarmType, isActive);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error logging alarm event for Channel {ChannelId}, Zone {ZoneId}",
                    channelId, zoneId);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Retrieves alarm event history within a specified time range.
        /// </summary>
        /// <param name="from">The start date and time.</param>
        /// <param name="to">The end date and time.</param>
        /// <param name="maxEntries">Maximum number of entries to return (default 1000).</param>
        /// <returns>A list of AlarmEventRecord objects.</returns>
        public async Task<List<AlarmEventRecord>> GetEventHistoryAsync(DateTime from, DateTime to, int maxEntries = 1000)
        {
            _logger.LogInformation("Retrieving event history from {From} to {To}, max entries: {MaxEntries}", from, to, maxEntries);

            var allEvents = new List<AlarmEventRecord>();
            if (maxEntries <= 0)
                return allEvents;

            // Min-heap holding only the newest maxEntries records: memory is bounded however long the range is
            var newest = new PriorityQueue<AlarmEventRecord, DateTime>();
            void Offer(AlarmEventRecord? e)
            {
                if (e == null || e.Timestamp < from || e.Timestamp > to) return;
                newest.Enqueue(e, e.Timestamp);
                if (newest.Count > maxEntries) newest.Dequeue();
            }

            try
            {
                for (var date = from.Date; date <= to.Date; date = date.AddDays(1))
                {
                    string day = date.ToString("yyyyMMdd");

                    // Old format (before line-per-event logging): one JSON array per day
                    var legacyPath = Path.Combine(EventHistoryFolderPath, $"alarm_events_{day}.json");
                    if (File.Exists(legacyPath))
                    {
                        try
                        {
                            var legacy = JsonSerializer.Deserialize<List<AlarmEventRecord>>(
                                await File.ReadAllTextAsync(legacyPath), ReadOptions);
                            legacy?.ForEach(Offer);
                        }
                        catch (JsonException ex)
                        {
                            _logger.LogWarning(ex, "Skipping unreadable legacy event file {File}", legacyPath);
                        }
                    }

                    // New format: one event per line, streamed
                    var path = Path.Combine(EventHistoryFolderPath, $"alarm_log_{day}.json");
                    if (!File.Exists(path))
                        continue;

                    using var reader = new StreamReader(
                        new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                    string? line;
                    while ((line = await reader.ReadLineAsync()) != null)
                    {
                        if (string.IsNullOrWhiteSpace(line)) continue;
                        try
                        {
                            Offer(JsonSerializer.Deserialize<AlarmEventRecord>(line, ReadOptions));
                        }
                        catch (JsonException)
                        {
                            // e.g. half-written last line after a power cut: skip it, keep the rest
                        }
                    }
                }

                // Newest first
                allEvents = newest.UnorderedItems.Select(x => x.Element)
                    .OrderByDescending(e => e.Timestamp).ToList();

                _logger.LogInformation("Retrieved {Count} event history entries", allEvents.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving event history from {From} to {To}", from, to);
            }

            return allEvents;
        }

        /// <summary>
        /// Represents a single alarm event record in the history.
        /// </summary>
        public class AlarmEventRecord
        {
            public DateTime Timestamp { get; set; }
            public int ChannelId { get; set; }
            public int ZoneId { get; set; }
            public string ZoneName { get; set; }
            public string AlarmType { get; set; }
            public bool IsActive { get; set; }
            public double? TriggerValue { get; set; }
            public double? BreachPosition { get; set; }
            public ZoneStateSnapshotRecord ZoneStateSnapshot { get; set; }
        }

        /// <summary>
        /// Snapshot of zone state at the time of the alarm.
        /// </summary>
        public class ZoneStateSnapshotRecord
        {
            public double AverageTemperature { get; set; }
            public double MaxTemperature { get; set; }
            public double MinTemperature { get; set; }
            public double MaxTempPosition { get; set; }
            public double RateOfRise { get; set; }
            public double Deviation { get; set; }
            public double StartPoint { get; set; }
            public double EndPoint { get; set; }
            public List<string> ActiveAlarms { get; set; }
        }
    }
}

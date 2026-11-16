using FireRakshak.FRMC.Core.Enums;
using FireRakshak.FRMC.Core.EventAggregator;
using FireRakshak.FRMC.Core.Events;
using FireRakshak.FRMC.Managers.SystemHealthManager;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Ports;
using System.Threading;
using System.Threading.Tasks;

namespace FireRakshak.FRMC.Managers.RS232Manager
{
    public class RS232Manager : IRS232Manager, IDisposable
    {
        private readonly IEventAggregator _eventAggregator;
        private readonly ILogger<RS232Manager> _logger;
        private readonly IConfiguration _configuration;
        private readonly ISystemHealthManager _systemHealthManager;

        private SerialPort? _serialPort;
        private CancellationTokenSource? _cts;
        private Task? _listenerTask;
        private Thread? _heartbeatThread;

        // All serial port writes MUST acquire this lock to prevent byte interleaving.
        private readonly object _serialWriteLock = new();

        private const byte START = 0xAA;
        private const byte END = 0x55;

        private const byte CMD_HEARTBEAT = 0x01;
        private const byte CMD_LASER = 0x02;
        private const byte CMD_RESET = 0x03;
        private const byte CMD_SHUTDOWN = 0x04;
        private const byte CMD_SYNC = 0x05;
        private const byte CMD_RELAY = 0x10;

        private const byte HEARTBEAT_TX = 0x01;
        private const byte HEARTBEAT_ACK = 0x00;

        private const byte RELAY_ACK = 0x00;
        private const byte RELAY_ON = 0x01;
        private const byte RELAY_OFF = 0x00;

        private string _portName = "COM4";
        private int _baudRate = 115200;

        public bool IsRunning { get; private set; }

        private readonly Dictionary<byte, bool> _relayStateCache = new();

        // Relay confirmation (safe mode): replies from the relay board are only required once the
        // board has been seen sending one. Until then relay commands behave exactly as before.
        private const int RelayAckTimeoutMs = 500;
        private volatile bool _boardSendsRelayAcks;
        private readonly ConcurrentDictionary<byte, TaskCompletionSource<bool>> _pendingRelayAcks = new();
        private readonly ConcurrentDictionary<byte, bool> _relayFaultActive = new();

        // Thread-safe state flags (accessed from heartbeat loop, event handlers, and frame handler)
        private volatile bool _laser;
        private volatile bool _alarm;
        private volatile bool _hooter;

        private double _jsonPumpCurrent = 90.0;  // Default pump current (mA) - updated from health checks (protected by _laserCommandLock)

        // Fix for race condition: ignore device state updates briefly after command
        private volatile bool _laserCommandPending;
        private DateTime _laserCommandTime;
        private readonly object _laserCommandLock = new();

        public RS232Manager(
            IEventAggregator eventAggregator,
            ILogger<RS232Manager> logger,
            IConfiguration configuration,
            ISystemHealthManager systemHealthManager)
        {
            _eventAggregator = eventAggregator;
            _logger = logger;
            _configuration = configuration;
            _systemHealthManager = systemHealthManager;

            LoadConfiguration();

            // EXISTING
            _eventAggregator.Subscribe<HealthParameterChangedEvent>(HandleHealthParameterChangedEvent);
            _eventAggregator.Subscribe<AlarmResetRequestedEvent>(HandleAlarmResetRequestedEvent);



        }

        private void LoadConfiguration()
        {
            _portName = _configuration["RS232:PortName"] ?? "COM4";
            _baudRate = int.Parse(_configuration["RS232:BaudRate"] ?? "115200");
        }

        // ================= START =================
        // FIXED: Added proper exception handling and resource cleanup
        public async Task StartAsync()
        {
            try
            {
                // Clean up any existing serial port
                _serialPort?.Dispose();
                _serialPort = null;

                _serialPort = new SerialPort(_portName, _baudRate)
                {
                    ReadTimeout = 500,
                    WriteTimeout = 500
                };

                _serialPort.Open();
                IsRunning = true;

                _cts?.Dispose();
                _cts = new CancellationTokenSource();

                _listenerTask = Task.Run(() => ListenAsync(_cts.Token));

                // Heartbeat runs on a dedicated high-priority thread so it
                // cannot be starved by threadpool work (DTS reads, WS broadcasts, etc.).
                _heartbeatThread = new Thread(() => HeartbeatLoop(_cts.Token))
                {
                    Name = "RS232-Heartbeat",
                    Priority = ThreadPriority.AboveNormal,
                    IsBackground = true
                };
                _heartbeatThread.Start();

                _logger.LogInformation("RS232Manager started successfully on {Port}", _portName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start RS232Manager on {Port}", _portName);

                // Clean up on failure
                _serialPort?.Dispose();
                _serialPort = null;
                IsRunning = false;

                throw; // Re-throw so Program.cs can handle it
            }

            await Task.CompletedTask;
        }

        // ================= HEARTBEAT LOOP =================
        // Runs on a dedicated Thread (not threadpool) with AboveNormal priority.
        // Outer loop owns reconnection; inner loop sends heartbeats with
        // Stopwatch-based absolute deadline tracking (no drift accumulation).
        private void HeartbeatLoop(CancellationToken token)
        {
            const int RECONNECT_MS = 3000;

            while (!token.IsCancellationRequested)
            {
                var port = _serialPort;
                if (port == null || !port.IsOpen)
                {
                    // Port lost — attempt to reconnect
                    TryReconnect(token);
                    if (!token.IsCancellationRequested)
                        Thread.Sleep(RECONNECT_MS);
                    continue;
                }

                // Connected — run steady-state heartbeat with deterministic timing.
                // Returns when connection is lost or cancellation is requested.
                HeartbeatSteadyState(token);
            }
        }

        /// <summary>
        /// Sends heartbeats at a precise 1 s interval using absolute deadline
        /// tracking (nextTickMs += INTERVAL). Sleep‑return‑late errors never
        /// accumulate because the next deadline is always relative to the
        /// single Stopwatch origin, not to the previous wake-up time.
        /// </summary>
        private void HeartbeatSteadyState(CancellationToken token)
        {
            const int INTERVAL_MS = 1000;
            var sw = Stopwatch.StartNew();
            long nextTickMs = 0; // first heartbeat fires immediately after (re)connect

            while (!token.IsCancellationRequested)
            {
                var port = _serialPort;
                if (port == null || !port.IsOpen)
                    return; // port gone — return to outer loop for reconnect

                try
                {
                    SendHeartbeat();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "RS232 heartbeat write failed — closing port for reconnect");
                    ClosePort();
                    return; // return to outer loop for reconnect
                }

                nextTickMs += INTERVAL_MS;
                int sleepMs = (int)(nextTickMs - sw.ElapsedMilliseconds);
                if (sleepMs > 0)
                    Thread.Sleep(sleepMs);
            }
        }

        // ================= RECONNECT =================
        private void TryReconnect(CancellationToken token)
        {
            try
            {
                ClosePort();

                _serialPort = new SerialPort(_portName, _baudRate)
                {
                    ReadTimeout = 500,
                    WriteTimeout = 500
                };
                _serialPort.Open();

                _logger.LogWarning("RS232 reconnected successfully on {Port}", _portName);

                // Restart listener on the new port
                _listenerTask = Task.Run(() => ListenAsync(token));
            }
            catch (Exception)
            {
                _logger.LogDebug("RS232 reconnect failed on {Port} — will retry", _portName);
                ClosePort();
            }
        }

        private void ClosePort()
        {
            var port = _serialPort;
            _serialPort = null;
            if (port != null)
            {
                try { port.Close(); } catch { }
                try { port.Dispose(); } catch { }
                _logger.LogWarning("RS232 port closed");
            }
        }

        private byte BuildHeartbeatStatus()
        {
            byte status = 0x00;
            if (_laser) status |= (1 << 0);
            if (_alarm) status |= (1 << 1);
            // Fault includes LaserOff fault from SystemHealthManager
            if (_systemHealthManager.IsHealthBad()) status |= (1 << 2);
            //  if (_hooter) status |= (1 << 3);
            status |= (1 << 3);
            status |= (1 << 4);

            if (_relayStateCache.TryGetValue(2, out var sysHealth) && sysHealth)
                status |= (1 << 5);

            if (_relayStateCache.TryGetValue(3, out var commonAlarm) && commonAlarm)
                status |= (1 << 6);

            if (_relayStateCache.TryGetValue(4, out var ch1) && ch1)
                status |= (1 << 7);

            return status;
        }

        private byte BuildHeartbeatrelay_FiberBreak()
        {
            byte relay_FiberBreak = 0x00;

            // CH2 → Relay 5
            if (_relayStateCache.TryGetValue(5, out var ch2) && ch2)
                relay_FiberBreak |= (1 << 0);

            // CH3 → Relay 6
            if (_relayStateCache.TryGetValue(6, out var ch3) && ch3)
                relay_FiberBreak |= (1 << 1);

            // CH4 → Relay 7
            if (_relayStateCache.TryGetValue(7, out var ch4) && ch4)
                relay_FiberBreak |= (1 << 2);

            // CH5 → Relay 8
            if (_relayStateCache.TryGetValue(8, out var ch5) && ch5)
                relay_FiberBreak |= (1 << 3);

            // CH6 → Relay 9
            if (_relayStateCache.TryGetValue(9, out var ch6) && ch6)
                relay_FiberBreak |= (1 << 4);

            // CH7 → Relay 10
            if (_relayStateCache.TryGetValue(10, out var ch7) && ch7)
                relay_FiberBreak |= (1 << 5);

            // CH8 → Relay 11
            if (_relayStateCache.TryGetValue(11, out var ch8) && ch8)
                relay_FiberBreak |= (1 << 6);

            return relay_FiberBreak;
        }



        private void SendHeartbeat()
        {
            if (_serialPort == null || !_serialPort.IsOpen)
                return;

            byte status = BuildHeartbeatStatus();
            byte relay_FiberBreak = BuildHeartbeatrelay_FiberBreak();
            byte crc = CRC8_Calc(new[] { CMD_HEARTBEAT, status, relay_FiberBreak }, 3);

            byte[] frame =
            {
                START,
                CMD_HEARTBEAT,
                status,
                relay_FiberBreak,
                crc,
                END
            };

            lock (_serialWriteLock)
            {
                _serialPort.Write(frame, 0, frame.Length);
            }

            _logger.LogWarning("<<<HEARTBEAT>>> laser={Laser} | alarm={Alarm} | fault={Fault} | hooter={Hooter} | status=0x{Status:X2} | relay_FiberBreak=0x{relay_FiberBreak:X2} pending={Pending}",
                _laser, _alarm, _systemHealthManager.IsHealthBad(), _hooter, status, relay_FiberBreak, _laserCommandPending);
        }

        // ================= HEALTH EVENTS =================
        private Task HandleHealthParameterChangedEvent(HealthParameterChangedEvent ev)
        {
            if (ev.Parameter == HealthParameter.PumpCurrent)
            {
                var pumpCurrentValue = Convert.ToDouble(ev.Value);
                var newLaserState = pumpCurrentValue > 0;
                var now = DateTime.UtcNow;

                bool previousState;
                bool laserStateChanged = false;
                bool pumpValueChanged = false;

                lock (_laserCommandLock)
                {
                    if (_laserCommandPending)
                    {
                        var elapsed = (now - _laserCommandTime).TotalSeconds;

                        if (elapsed < 2)
                        {
                            _logger.LogWarning(
                                ">>>PUMP_UPDATE IGNORED<<< value={Value}mA | would_be={NewState} | current_laser={CurrentState} | elapsed={Elapsed:F2}s | reason=PENDING_COMMAND",
                                pumpCurrentValue, newLaserState, _laser, elapsed);

                            return Task.CompletedTask;
                        }

                        _logger.LogWarning(
                            ">>>PUMP_PENDING EXPIRED<<< elapsed={Elapsed:F2}s | clearing pending flag",
                            elapsed);

                        _laserCommandPending = false;
                    }

                    previousState = _laser;

                    // Detect laser state change
                    if (previousState != newLaserState)
                    {
                        laserStateChanged = true;
                        _laser = newLaserState;
                    }

                    // Detect pump current value change (only if > 0)
                    if (pumpCurrentValue > 0 && pumpCurrentValue != _jsonPumpCurrent)
                    {
                        pumpValueChanged = true;
                        _jsonPumpCurrent = pumpCurrentValue;
                    }
                }

                // Log ONLY if something actually changed
                if (laserStateChanged || pumpValueChanged)
                {
                    _logger.LogWarning(
                        ">>>PUMP_UPDATE ACCEPTED<<< value={Value}mA | laser_changed={PrevState}->{NewState} | stored_pump={StoredPump}mA",
                        pumpCurrentValue,
                        previousState,
                        _laser,
                        _jsonPumpCurrent);
                }
            }
            else if (ev.Parameter == HealthParameter.LaserCurrent)
            {
                // Only log if value is meaningful (optional noise reduction)
                var dfbValue = Convert.ToDouble(ev.Value);

                if (dfbValue > 0) // avoid logging useless zero noise
                {
                    _logger.LogDebug(
                        ">>>DFB_LASER<<< value={Value}mA (ignored for laser state)",
                        dfbValue);
                }
            }

            return Task.CompletedTask;
        }



        private Task HandleAlarmResetRequestedEvent(AlarmResetRequestedEvent ev)
        {
            _alarm = false;
            return Task.CompletedTask;
        }



        // ================= LISTENER =================
        // FIXED: Added buffer size limit and frame timeout to prevent overflow
        // Exits cleanly when port is closed/disposed (heartbeat thread handles reconnect)
        private async Task ListenAsync(CancellationToken token)
        {
            var buffer = new List<byte>();
            const int MAX_FRAME_SIZE = 10;  // Expected max is 6 bytes
            DateTime lastByteTime = DateTime.UtcNow;
            const int FRAME_TIMEOUT_MS = 100;  // 100ms timeout for incomplete frames

            while (!token.IsCancellationRequested)
            {
                var port = _serialPort;
                if (port == null || !port.IsOpen)
                    break; // Port closed — exit; heartbeat thread will restart us after reconnect

                try
                {
                    int b = port.ReadByte();
                    if (b < 0) continue;

                    var now = DateTime.UtcNow;

                    // Check for frame timeout (partial frame abandoned)
                    if (buffer.Count > 0 && (now - lastByteTime).TotalMilliseconds > FRAME_TIMEOUT_MS)
                    {
                        _logger.LogWarning("RS232 frame timeout: discarding {Count} bytes", buffer.Count);
                        buffer.Clear();
                    }

                    // Check buffer overflow protection
                    if (buffer.Count >= MAX_FRAME_SIZE)
                    {
                        _logger.LogWarning("RS232 buffer overflow: {Count} bytes, resynchronizing", buffer.Count);
                        buffer.Clear();

                        // Resync: wait for START byte
                        if (b != START)
                        {
                            _logger.LogDebug("Skipping byte 0x{Byte:X2} during resync", b);
                            continue;
                        }
                    }

                    buffer.Add((byte)b);
                    lastByteTime = now;

                    // Validate START byte on first position
                    if (buffer.Count == 1 && b != START)
                    {
                        _logger.LogDebug("Invalid START byte: 0x{Byte:X2}, discarding", b);
                        buffer.Clear();
                        continue;
                    }

                    // Check for complete frame
                    if (b == END)
                    {
                        if (buffer.Count == 5 || buffer.Count == 6)
                        {
                            // Valid frame length
                            HandleFrame(buffer.ToArray());
                            buffer.Clear();
                        }
                        else
                        {
                            // Invalid frame length
                            _logger.LogWarning("Invalid frame length: {Count} bytes (expected 5 or 6), discarding", buffer.Count);
                            buffer.Clear();
                        }
                    }
                }
                catch (TimeoutException)
                {
                    // Expected - serial port ReadByte timeout, continue
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Port disposed/disconnected — exit cleanly
                    if (token.IsCancellationRequested || _serialPort == null)
                        break;

                    _logger.LogWarning(ex, "RS232 read error — listener exiting (heartbeat will reconnect)");
                    buffer.Clear();
                    break;
                }
            }

            _logger.LogInformation("RS232 listener stopped");
        }

        // ================= FRAME HANDLER =================
        private void HandleFrame(byte[] frame)
        {
            if (frame[0] != START || frame[^1] != END)
                return;

            // Validate CRC (CRC is second-to-last byte)
            int crcIndex = frame.Length - 2;
            byte receivedCrc = frame[crcIndex];

            // CRC is calculated over CMD + DATA bytes (positions 1 to crcIndex-1)
            int dataLen = crcIndex - 1;  // Number of bytes to include in CRC
            byte[] crcData = new byte[dataLen];
            Array.Copy(frame, 1, crcData, 0, dataLen);
            byte calculatedCrc = CRC8_Calc(crcData, dataLen);

            if (receivedCrc != calculatedCrc)
            {
                _logger.LogWarning("RS232 CRC mismatch: received=0x{Received:X2}, calculated=0x{Calculated:X2}, discarding frame",
                    receivedCrc, calculatedCrc);
                return;
            }

            byte cmd = frame[1];

            switch (cmd)
            {
                case CMD_LASER:
                    _ = HandleLaserButtonAsync();
                    break;

                case CMD_RESET:
                    _ = HandleResetFromStm32Async();
                    break;

                case CMD_SHUTDOWN:
                    HandleShutdown();
                    break;

                case CMD_SYNC:
                    _ = HandleSyncRequestAsync();
                    break;

                case CMD_RELAY:
                    HandleRelayAck(frame);
                    break;
            }
        }

        /// <summary>
        /// Relay reply from the board: [START][CMD_RELAY][relayId]...[CRC][END].
        /// The first reply switches on confirmation checking for all later relay commands.
        /// </summary>
        private void HandleRelayAck(byte[] frame)
        {
            byte relayId = frame[2];

            if (!_boardSendsRelayAcks)
            {
                _boardSendsRelayAcks = true;
                _logger.LogInformation("[RELAY] Relay board confirms relay commands; confirmation checking enabled");
            }

            if (_pendingRelayAcks.TryRemove(relayId, out var ack))
                ack.TrySetResult(true);
        }

        /// <summary>
        /// Raises or clears a "Relay N failed" health fault (shown in FRCM).
        /// Published in the background so the relay's own lock is never re-entered.
        /// </summary>
        private void SetRelayFault(byte relayId, bool failed, string details)
        {
            if (failed ? !_relayFaultActive.TryAdd(relayId, true) : !_relayFaultActive.TryRemove(relayId, out _))
                return; // already in that state

            var now = DateTime.UtcNow;
            var fault = new FireRakshak.FRMC.Core.Models.SystemHealthFault
            {
                FaultType = SystemHealthFaultType.SystemFault,
                IsActive = failed,
                Description = failed ? $"Relay {relayId} failed" : $"Relay {relayId} confirmed",
                Details = details,
                FirstDetectedAt = now,
                LastUpdatedAt = now,
                ClearedAt = failed ? null : now
            };

            _ = Task.Run(async () =>
            {
                try
                {
                    await _eventAggregator.PublishAsync(new SystemHealthFaultEvent(fault, isNewFault: failed, now));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[RELAY] Failed to publish relay fault for relay {RelayId}", relayId);
                }
            });
        }

        // ================= LASER BUTTON (RS-03) =================
        private async Task HandleLaserButtonAsync()
        {
            try
            {
                var previousState = _laser;

                // Set pending flag atomically to ignore device state updates during command execution
                lock (_laserCommandLock)
                {
                    _laserCommandPending = true;
                    _laserCommandTime = DateTime.UtcNow;
                }

                _logger.LogWarning("===RS232 LASER CMD RECEIVED=== prev_laser={PrevState} | setting pending=true",
                    previousState);

                if (_laser)
                {
                    _laser = false;

                    _logger.LogWarning("===LASER OFF CMD=== laser_changed={PrevState}->False | publishing SetPumpCurrent=0",
                        previousState);

                    await _eventAggregator.PublishAsync(
                        new LaserControlRequestedEvent(
                            LaserControlType.SetPumpCurrent,
                            0,
                            "STM32"));
                }
                else
                {
                    _laser = true;

                    // Safety: Use default if _jsonPumpCurrent is 0 or invalid (read under lock)
                    double pumpCurrentToSet;
                    lock (_laserCommandLock)
                    {
                        pumpCurrentToSet = _jsonPumpCurrent > 0 ? _jsonPumpCurrent : 90.0;
                    }

                    _logger.LogWarning("===LASER ON CMD=== laser_changed={PrevState}->True | publishing SetPumpCurrent={Current}",
                        previousState, pumpCurrentToSet);

                    await _eventAggregator.PublishAsync(
                        new LaserControlRequestedEvent(
                            LaserControlType.SetPumpCurrent,
                            pumpCurrentToSet,
                            "STM32"));
                }

                SendSimpleAck(CMD_LASER);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[RS232] Error handling STM32 LASER button event");
            }
        }

        // ================= INTERFACE METHODS (RESTORED) =================
        /// <summary>
        /// Sends a relay command to STM32 via RS232.
        /// Frame format: [START][CMD_RELAY][relayId][status][CRC][END]
        /// </summary>
        /// <param name="relayId">1-based relay ID (1-255)</param>
        /// <param name="turnOn">True for ON (0x01), False for OFF (0x00)</param>
        public async Task SendRelayCommandAsync(byte relayId, bool turnOn)
        {
            string stateStr = turnOn ? "ON" : "OFF";
            byte status = turnOn ? RELAY_ON : RELAY_OFF;
            byte crc = CRC8_Calc(new[] { CMD_RELAY, relayId, status }, 3);

            byte[] frame =
            {
                START,
                CMD_RELAY,
                relayId,    // 1-based relay ID sent directly to STM32
                status,
                crc,
                END
            };

            const int maxRetries = 3;
            Exception? lastException = null;
            bool sent = false;
            bool notConfirmed = false;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    if (!IsRunning || _serialPort == null || !_serialPort.IsOpen)
                    {
                        throw new InvalidOperationException($"RS232 port is not open or not running (attempt {attempt}/{maxRetries})");
                    }

                    // Register for the board's reply before writing, so a fast reply is not missed
                    TaskCompletionSource<bool>? ack = null;
                    if (_boardSendsRelayAcks)
                    {
                        ack = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                        _pendingRelayAcks[relayId] = ack;
                    }

                    lock (_serialWriteLock)
                    {
                        _serialPort.Write(frame, 0, frame.Length);
                    }

                    // Wanted state (used to re-send after a board reboot), even if not yet confirmed
                    _relayStateCache[relayId] = turnOn;

                    _logger.LogInformation(
                        "[RELAY] RS232 TX -> ID={RelayId} (0x{RelayIdHex:X2}) | {State} | Frame=[{Frame}] (Attempt {Attempt})",
                        relayId,
                        relayId,
                        stateStr,
                        BitConverter.ToString(frame),
                        attempt);

                    if (ack != null &&
                        await Task.WhenAny(ack.Task, Task.Delay(RelayAckTimeoutMs)) != ack.Task)
                    {
                        _pendingRelayAcks.TryRemove(new KeyValuePair<byte, TaskCompletionSource<bool>>(relayId, ack));
                        notConfirmed = true;
                        throw new TimeoutException(
                            $"No confirmation from relay board for relay {relayId} within {RelayAckTimeoutMs} ms");
                    }

                    sent = true;
                    notConfirmed = false;
                    break;
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    _logger.LogWarning(ex,
                        "[RELAY] Attempt {Attempt}/{MaxRetries} failed to send command for relay {RelayId} (State={State})",
                        attempt, maxRetries, relayId, stateStr);

                    if (attempt < maxRetries)
                    {
                        await Task.Delay(50);
                    }
                }
            }

            if (sent)
            {
                SetRelayFault(relayId, failed: false, $"Relay {relayId} {stateStr} confirmed by relay board");
            }
            else
            {
                string errorMsg = $"RS232 send failed for relay {relayId} ({stateStr}) after {maxRetries} attempts: {lastException?.Message}";
                _logger.LogError(lastException, "[RELAY] {ErrorMessage}", errorMsg);

                // Board is known to confirm commands but did not confirm this one
                if (notConfirmed)
                    SetRelayFault(relayId, failed: true, errorMsg);

                // Publish failure event so event aggregator / UI / logging know hardware actuation failed
                await _eventAggregator.PublishAsync(new RelayCommandFailedEvent(
                    relayId,
                    $"RELAY {relayId} {stateStr}",
                    errorMsg,
                    DateTime.UtcNow));

                throw new IOException(errorMsg, lastException);
            }
        }
        // ================= RESET BUTTON (RS-03) =================
        private async Task HandleResetFromStm32Async()
        {
            try
            {
                _logger.LogWarning("STM32 RESET button pressed -> clearing all alarms");

                // 1. Clear heartbeat alarm bit immediately
                _alarm = false;

                // 2. Publish reset event
                await _eventAggregator.PublishAsync(
                    new AlarmResetRequestedEvent(
                        source: "STM32",
                        timestamp: DateTime.UtcNow));

                // 3. ACK back to STM32
                SendSimpleAck(CMD_RESET);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[RS232] Error handling STM32 RESET button event");
            }
        }

        /// <summary>
        /// Handles a sync request from the front panel (sent after watchdog or power-on reset).
        /// Re-sends all ON relay states so the front panel matches the FRMC's current state,
        /// then forces a heartbeat to refresh LED status.
        /// </summary>
        // ================= SYNC REQUEST (RS-03 / RS-05) =================
        private async Task HandleSyncRequestAsync()
        {
            try
            {
                _logger.LogWarning("[RS232] Front panel sync request received -> re-sending all relay states");

                SendSimpleAck(CMD_SYNC);

                // Front panel resets all relays to OFF on boot, so only re-send ON states.
                int count = 0;
                foreach (var kvp in _relayStateCache)
                {
                    if (!kvp.Value)
                        continue;

                    try
                    {
                        await SendRelayCommandAsync(kvp.Key, true);
                        count++;

                        // Pace commands with 25ms delay to prevent overflowing the front panel's 256-byte FIFO (RS-05)
                        // (6 bytes/frame, allows STM32 ample time to process CRC and toggle GPIO without dropped frames)
                        await Task.Delay(25);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "[RS232] Resync failed for relay {RelayId}", kvp.Key);
                    }
                }

                // Force a heartbeat to refresh LED states on the front panel
                try
                {
                    SendHeartbeat();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[RS232] Heartbeat send failed during resync");
                }

                _logger.LogWarning("[RS232] Front panel resync complete: re-sent {Count} relay ON commands", count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[RS232] Unhandled error during front panel sync processing");
            }
        }
        public Task SetHeartbeatAlarmAsync(bool alarmOn)
        {
            _alarm = alarmOn;
            return Task.CompletedTask;
        }

        // ================= UTILS =================
        private void SendSimpleAck(byte cmd)
        {
            byte crc = CRC8_Calc(new[] { cmd, (byte)0x00, (byte)0x00 }, 3);

            byte[] ack =
            {
                START,
                cmd,
                0x00,
                0x00,
                crc,
                END
            };

            lock (_serialWriteLock)
            {
                _serialPort!.Write(ack, 0, ack.Length);
            }
        }

        private static byte CRC8_Calc(byte[] data, int len)
        {
            byte crc = 0x00;

            for (int i = 0; i < len; i++)
            {
                crc ^= data[i];
                for (int bit = 0; bit < 8; bit++)
                {
                    crc = (crc & 0x80) != 0
                        ? (byte)((crc << 1) ^ 0x07)
                        : (byte)(crc << 1);
                }
            }

            return crc;
        }

        // ================= SHUTDOWN CONFIRMATION (RS-04) =================
        private DateTime _lastShutdownRequestTime = DateTime.MinValue;
        private readonly object _shutdownLock = new();
        private const int SHUTDOWN_CONFIRMATION_WINDOW_SECONDS = 3;
        private const int SHUTDOWN_COUNTDOWN_SECONDS = 5;

        /// <summary>
        /// Handles front-panel shutdown requests (CMD_SHUTDOWN).
        /// Implements a two-stage confirmation window (3 seconds) to prevent accidental
        /// touches or electrical noise spikes from shutting down the primary fire monitoring IPC.
        /// A second press within 3 seconds confirms shutdown, triggering a graceful 5-second countdown.
        /// </summary>
        private void HandleShutdown()
        {
            SendSimpleAck(CMD_SHUTDOWN);

            lock (_shutdownLock)
            {
                var now = DateTime.UtcNow;
                var elapsed = (now - _lastShutdownRequestTime).TotalSeconds;

                if (elapsed <= SHUTDOWN_CONFIRMATION_WINDOW_SECONDS)
                {
                    _logger.LogCritical(
                        "****************************** System Shutdown CONFIRMED (Second press within {Window}s) -> Initiating graceful {Countdown}s shutdown ******************************",
                        SHUTDOWN_CONFIRMATION_WINDOW_SECONDS,
                        SHUTDOWN_COUNTDOWN_SECONDS);

                    _lastShutdownRequestTime = DateTime.MinValue;

#if WINDOWS
                    Process.Start(new ProcessStartInfo
                    {
                        FileName = "shutdown",
                        Arguments = $"/s /f /t {SHUTDOWN_COUNTDOWN_SECONDS} /c \"FireRakshak IPC Shutdown confirmed via front panel\"",
                        CreateNoWindow = true,
                        UseShellExecute = false
                    });
#else
                    Process.Start(new ProcessStartInfo
                    {
                        FileName = "/usr/local/sbin/frmc-poweroff",
                        UseShellExecute = false
                    });
#endif
                }
                else
                {
                    _lastShutdownRequestTime = now;
                    _logger.LogWarning(
                        "[SHUTDOWN] First shutdown pulse received. Press front panel shutdown button again within {Window}s to confirm shutdown.",
                        SHUTDOWN_CONFIRMATION_WINDOW_SECONDS);

                    try
                    {
                        Console.Beep(1000, 300);
                    }
                    catch
                    {
                        // Headless service or environment without audio support
                    }
                }
            }
        }
        // ================= STOP =================
        // FIXED: Proper disposal of SerialPort and safe async-over-sync
        public async Task StopAsync()
        {
            if (!IsRunning)
                return;

            IsRunning = false;

            _logger.LogInformation("Stopping RS232Manager...");

            // Cancel tasks
            _cts?.Cancel();

            // Wait for tasks/threads to complete with timeout
            try
            {
                if (_listenerTask != null)
                {
                    await Task.WhenAny(_listenerTask, Task.Delay(2000));
                }

                if (_heartbeatThread != null && _heartbeatThread.IsAlive)
                {
                    _heartbeatThread.Join(TimeSpan.FromSeconds(2));
                    _heartbeatThread = null;
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation occurs
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Exception while waiting for RS232 tasks to complete");
            }

            // Dispose serial port
            ClosePort();

            // Dispose cancellation token source
            try
            {
                _cts?.Dispose();
                _cts = null;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Exception while disposing cancellation token source");
            }

            _logger.LogInformation("RS232Manager stopped");
        }

        // FIXED: Proper sync-over-async pattern
        public void Dispose()
        {
            // Use GetAwaiter().GetResult() instead of .Wait() for better exception handling
            try
            {
                StopAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during RS232Manager disposal");
            }
        }
    }
}


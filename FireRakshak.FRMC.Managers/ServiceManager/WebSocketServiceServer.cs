using FireRakshak.FRMC.Core.Enums;
using FireRakshak.FRMC.Core.EventAggregator;
using FireRakshak.FRMC.Core.Events;
using FireRakshak.FRMC.Core.Mappers;
using FireRakshak.FRMC.Core.Models;
using FireRakshak.FRMC.Managers.AlarmManager;
using FireRakshak.FRMC.Managers.ChannelsManager;
using FireRakshak.FRMC.Managers.RelayManager;
using FireRakshak.FRMC.Managers.ServiceManager;
using FireRakshak.FRMC.Managers.SystemHealthManager;
using FireRakshak.FRMC.Managers.DtsManager;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using ConfigManager = FireRakshak.FRMC.Managers.SystemConfigurationManager.IConfigurationManager;
using UserSession = FireRakshak.FRMC.Managers.ServiceManager.UserSession;



namespace FireRakshak.FRMC.Host
{
    /// <summary>
    /// WebSocket server utility class that hosts WS/WSS listeners, handles
    /// client connections and forwards DTS temperature data to connected clients.
    /// This class replaces the former WebSocketServerHostedService and now supports
    /// channel configuration queries/updates from DTSCM.
    /// </summary>
    public class WebSocketServer
    {
        private readonly ILogger<WebSocketServer> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IEventAggregator _eventAggregator;
        private readonly ConfigManager _configurationManager;
        private readonly NetworkInterfaceService _networkService;
        private readonly IDataStreamServer _dataStreamServer;
        // Role names used to identify ethernet ports in config
        // These match the "Role" property in ethconfig.json
        private const string ROLE_NETRA1 = "NETRA1";
        private const string ROLE_NETRA2 = "NETRA2";

        // Made private to prevent external code from incorrectly accessing/overwriting
        private WebSocket? _currentWebSocket;
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetLocalTime(ref SYSTEMTIME st);
        [StructLayout(LayoutKind.Sequential)]
        private struct SYSTEMTIME
        {
            public ushort Year;
            public ushort Month;
            public ushort DayOfWeek;
            public ushort Day;
            public ushort Hour;
            public ushort Minute;
            public ushort Second;
            public ushort Milliseconds;
        }

        // Flag to control whether live streaming is active
        private volatile bool _isStreamingActive = false;
        public bool IsStreamingActive => _isStreamingActive;
        public double? CorrectionLength;
        private FrmcConfiguration? _frmcConfig;
        private TcpListener? _tcpListener;
        private X509Certificate2? _serverCertificate;

        private HttpListener? _wsListener;
        private HttpListener? _wssListener;
        private CancellationTokenSource? _cts;
        private WebSocket? _connectedClientSocket;

        // background tasks
        private Task? _listenerTaskWs;
        private Task? _listenerTaskWss;
        private Task? _dtsPollingTask;
        // FM-08: Background Dead Socket Reaper task and tracked connected sockets
        private Task? _deadSocketReaperTask;
        private readonly System.Collections.Concurrent.ConcurrentDictionary<WebSocket, string> _connectedSockets = new();

        // Session management - maps WebSocket connections to authenticated user sessions
        private readonly System.Collections.Concurrent.ConcurrentDictionary<WebSocket, UserSession> _activeSessions = new();

        // Channel subscription tracking - maps WebSocket connections to subscribed channel IDs
        // Only clients subscribed to a channel will receive live_temperature_data for that channel
        private readonly System.Collections.Concurrent.ConcurrentDictionary<WebSocket, System.Collections.Concurrent.ConcurrentBag<int>> _channelSubscriptions = new();

        public WebSocketServer(
            ILogger<WebSocketServer> logger,
            IServiceProvider serviceProvider,
            IEventAggregator eventAggregator,
            IConfiguration config,
            ConfigManager configurationManager,
            NetworkInterfaceService networkService,
            IDataStreamServer dataStreamServer)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _eventAggregator = eventAggregator;
            _configurationManager = configurationManager ?? throw new ArgumentNullException(nameof(configurationManager));
            _networkService = networkService;
            _dataStreamServer = dataStreamServer ?? throw new ArgumentNullException(nameof(dataStreamServer));
        }

        /// <summary>
        /// Start the server: listeners and DTS polling.
        /// </summary>
        public Task StartAsync(CancellationToken externalCancellationToken = default)
        {
            _logger.LogInformation("WebSocketServer is starting.");
            _cts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken);
            CancellationToken linkedToken = _cts.Token;

            // Load configuration
            _frmcConfig = FrmcConfiguration.LoadFromFile("config.json") ?? new FrmcConfiguration();

            if (string.IsNullOrWhiteSpace(_frmcConfig.CertificatePath))
            {
                _frmcConfig.CertificatePath = "localhost.pfx";
            }

            if (string.IsNullOrWhiteSpace(_frmcConfig.CertificatePassword))
            {
                _frmcConfig.CertificatePassword = "FRMC@123";
            }

#if USE_SECURE
            _frmcConfig.UseSecure = true;
#else
            _frmcConfig.UseSecure = false;
#endif

            if (_frmcConfig.UseSecure)
            {
                StartSecureWssMode(linkedToken);
            }
            else
            {
                StartUnsecureWsMode(linkedToken);
            }

            _deadSocketReaperTask = Task.Run(() => RunDeadSocketReaperAsync(linkedToken), linkedToken);
            _logger.LogInformation("Dead Socket Reaper background task started (15s interval, 5s probe timeout).");

            return Task.CompletedTask;
        }

        private void StartSecureWssMode(CancellationToken linkedToken)
        {
            _logger.LogInformation("WebSocketServer starting in WSS (Secure) mode.");
            LoadServerCertificate();
            StartWssTcpListener(linkedToken);
        }

        private void LoadServerCertificate()
        {
            try
            {
                if (string.IsNullOrEmpty(_frmcConfig?.CertificatePath))
                {
                    throw new ArgumentException("CertificatePath is empty or null in FrmcConfiguration when UseSecure is true.");
                }

                _serverCertificate = string.IsNullOrEmpty(_frmcConfig.CertificatePassword)
                    ? new X509Certificate2(_frmcConfig.CertificatePath)
                    : new X509Certificate2(_frmcConfig.CertificatePath, _frmcConfig.CertificatePassword);

                _logger.LogInformation("Successfully loaded SSL/TLS certificate from {Path}", _frmcConfig.CertificatePath);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to load SSL/TLS certificate from '{_frmcConfig?.CertificatePath ?? "null"}'.", ex);
            }
        }

        private void StartWssTcpListener(CancellationToken linkedToken)
        {
            try
            {
                _tcpListener = new TcpListener(IPAddress.IPv6Any, 6164);
                _tcpListener.Server.SetSocketOption(SocketOptionLevel.IPv6, SocketOptionName.IPv6Only, false);
                _tcpListener.Start();
                _logger.LogInformation("WSS TCP Listener started on port 6164");
                _listenerTaskWss = Task.Run(() => StartListeningTcpAsync(_tcpListener, linkedToken), linkedToken);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to start WSS TCP Listener on port 6164.", ex);
            }
        }

        private void StartUnsecureWsMode(CancellationToken linkedToken)
        {
            _logger.LogInformation("WebSocketServer starting in WS (Unsecure) mode.");
            try
            {
                _wsListener = new HttpListener();
                string prefix = "http://+:6164/general_ws/";
                try
                {
                    _wsListener.Prefixes.Add(prefix);
                    _wsListener.Start();
                    _logger.LogInformation("WS Listener started on {Prefix} (accepting remote connections)", prefix);
                }
                catch (HttpListenerException)
                {
                    _wsListener = new HttpListener();
                    prefix = "http://localhost:6164/general_ws/";
                    _wsListener.Prefixes.Add(prefix);
                    _wsListener.Start();
                    _logger.LogWarning("WS Listener started on {Prefix} (localhost only - register URL ACL for remote access)", prefix);
                }
                _listenerTaskWs = Task.Run(() => StartListening(_wsListener, linkedToken), linkedToken);
            }
            catch (HttpListenerException ex)
            {
                _logger.LogError(ex, "Failed to start WS Listener");
            }
        }

        private async Task StartListeningTcpAsync(TcpListener listener, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var tcpClient = await listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                    _ = HandleTcpClientAsync(tcpClient, cancellationToken);
                }
                catch (ObjectDisposedException)
                {
                    break; // Listener stopped
                }
                catch (Exception ex)
                {
                    if (cancellationToken.IsCancellationRequested) break;
                    _logger.LogError(ex, "Error accepting TCP client connection");
                }
            }
        }

        private async Task HandleTcpClientAsync(TcpClient tcpClient, CancellationToken cancellationToken)
        {
            string remoteEndpoint = "unknown";
            try
            {
                remoteEndpoint = tcpClient.Client.RemoteEndPoint?.ToString() ?? "unknown";
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Unable to resolve remote endpoint for incoming TCP client");
            }

            _logger.LogInformation("Accepted TCP client connection from {Endpoint}", remoteEndpoint);

            try
            {
                Stream? stream = await EstablishClientStreamAsync(tcpClient, remoteEndpoint, cancellationToken).ConfigureAwait(false);
                if (stream == null)
                    return;

                var (path, secWebSocketKey) = await ParseHttpHandshakeAsync(stream, cancellationToken).ConfigureAwait(false);
                if (string.IsNullOrEmpty(path) || string.IsNullOrEmpty(secWebSocketKey) || !path.StartsWith("/general_ws/"))
                {
                    await RejectHttpUpgradeRequestAsync(tcpClient, stream, remoteEndpoint, path, cancellationToken).ConfigureAwait(false);
                    return;
                }

                await CompleteWebSocketUpgradeAsync(stream, secWebSocketKey, cancellationToken).ConfigureAwait(false);

                using var webSocket = WebSocket.CreateFromStream(stream, isServer: true, subProtocol: null, keepAliveInterval: TimeSpan.FromSeconds(30));
                _logger.LogInformation("WebSocket upgraded successfully for client {Endpoint}", remoteEndpoint);

                await ProcessWebSocketAsync(webSocket, remoteEndpoint, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Client connection handling failed for client {Endpoint}", remoteEndpoint);
                tcpClient.Close();
            }
        }

        private async Task<Stream?> EstablishClientStreamAsync(TcpClient tcpClient, string remoteEndpoint, CancellationToken cancellationToken)
        {
            Stream stream = tcpClient.GetStream();

            byte[] peekBuffer = new byte[1];
            int peeked = tcpClient.Client.Receive(peekBuffer, 0, 1, SocketFlags.Peek);
            bool isTls = peeked > 0 && peekBuffer[0] == 0x16;

            if (!isTls)
            {
                _logger.LogInformation("Plain HTTP/WebSocket connection detected for client {Endpoint}", remoteEndpoint);
                return stream;
            }

            if (_serverCertificate == null)
            {
                _logger.LogWarning("Incoming secure connection detected but no SSL certificate is loaded. Closing connection.");
                tcpClient.Close();
                return null;
            }

            var sslStream = new SslStream(stream, false);
            try
            {
                await sslStream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
                {
                    ServerCertificate = _serverCertificate,
                    ClientCertificateRequired = false,
                    EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12 | System.Security.Authentication.SslProtocols.Tls13,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck
                }, cancellationToken).ConfigureAwait(false);

                _logger.LogInformation("TLS handshake completed successfully for client {Endpoint}", remoteEndpoint);
                return sslStream;
            }
            catch (Exception tlsEx)
            {
                _logger.LogError(tlsEx, "TLS handshake failed for client {Endpoint}", remoteEndpoint);
                tcpClient.Close();
                return null;
            }
        }

        private async Task RejectHttpUpgradeRequestAsync(TcpClient tcpClient, Stream stream, string remoteEndpoint, string? path, CancellationToken cancellationToken)
        {
            _logger.LogWarning("Invalid HTTP Upgrade handshake request from client {Endpoint}. Path: {Path}", remoteEndpoint, path ?? "null");
            byte[] response = Encoding.UTF8.GetBytes("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
            await stream.WriteAsync(response, 0, response.Length, cancellationToken).ConfigureAwait(false);
            tcpClient.Close();
        }

        private static async Task CompleteWebSocketUpgradeAsync(Stream stream, string secWebSocketKey, CancellationToken cancellationToken)
        {
            string key = secWebSocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
#pragma warning disable S4790 // RFC 6455 Section 4.2.2 mandates SHA-1 for the Sec-WebSocket-Accept handshake header
            byte[] hash = System.Security.Cryptography.SHA1.HashData(Encoding.UTF8.GetBytes(key));
#pragma warning restore S4790
            string acceptKey = Convert.ToBase64String(hash);

            string responseString = "HTTP/1.1 101 Switching Protocols\r\n" +
                                   "Upgrade: websocket\r\n" +
                                   "Connection: Upgrade\r\n" +
                                   $"Sec-WebSocket-Accept: {acceptKey}\r\n\r\n";
            byte[] responseBytes = Encoding.UTF8.GetBytes(responseString);
            await stream.WriteAsync(responseBytes, 0, responseBytes.Length, cancellationToken).ConfigureAwait(false);
        }

        private async Task<(string? Path, string? SecWebSocketKey)> ParseHttpHandshakeAsync(Stream stream, CancellationToken cancellationToken)
        {
            byte[] headerBytes = await ReadRawHttpHeaderBytesAsync(stream, cancellationToken).ConfigureAwait(false);
            string headerText = Encoding.UTF8.GetString(headerBytes);
            var lines = headerText.Split(new[] { "\r\n" }, StringSplitOptions.None);
            if (lines.Length == 0) return (null, null);

            var requestParts = lines[0].Split(' ');
            if (requestParts.Length < 2 || requestParts[0] != "GET")
            {
                return (null, null);
            }
            string path = requestParts[1];

            var (secWebSocketKey, isValidUpgrade) = ParseWebSocketHeaders(lines);
            if (isValidUpgrade && !string.IsNullOrEmpty(secWebSocketKey))
            {
                return (path, secWebSocketKey);
            }

            return (null, null);
        }

        private static async Task<byte[]> ReadRawHttpHeaderBytesAsync(Stream stream, CancellationToken cancellationToken)
        {
            var headerBytes = new List<byte>();
            byte[] tempBuffer = new byte[1];
            while (!cancellationToken.IsCancellationRequested)
            {
                int read = await stream.ReadAsync(tempBuffer, 0, 1, cancellationToken).ConfigureAwait(false);
                if (read <= 0) break;
                headerBytes.Add(tempBuffer[0]);
                if (headerBytes.Count >= 4 &&
                    headerBytes[^1] == 10 && headerBytes[^2] == 13 &&
                    headerBytes[^3] == 10 && headerBytes[^4] == 13)
                {
                    break;
                }
                if (headerBytes.Count > 8192)
                {
                    throw new InvalidOperationException("HTTP headers too large");
                }
            }
            return headerBytes.ToArray();
        }

        private static (string? Key, bool IsValid) ParseWebSocketHeaders(string[] lines)
        {
            string? secWebSocketKey = null;
            bool hasUpgrade = false;
            bool hasConnection = false;

            for (int i = 1; i < lines.Length; i++)
            {
                var line = lines[i];
                int colonIndex = line.IndexOf(':');
                if (colonIndex <= 0) continue;

                string headerName = line.Substring(0, colonIndex).Trim().ToLowerInvariant();
                string headerValue = line.Substring(colonIndex + 1).Trim();

                if (headerName == "upgrade" && headerValue.Equals("websocket", StringComparison.OrdinalIgnoreCase))
                {
                    hasUpgrade = true;
                }
                else if (headerName == "connection" && headerValue.Contains("Upgrade", StringComparison.OrdinalIgnoreCase))
                {
                    hasConnection = true;
                }
                else if (headerName == "sec-websocket-key")
                {
                    secWebSocketKey = headerValue;
                }
            }

            return (secWebSocketKey, hasUpgrade && hasConnection);
        }

        public string? GetLiveIPv4(string interfaceName)
        {
            var ni = NetworkInterface.GetAllNetworkInterfaces()
                .FirstOrDefault(n =>
                    n.Name.Equals(interfaceName, StringComparison.OrdinalIgnoreCase));

            if (ni == null || ni.OperationalStatus != OperationalStatus.Up)
                return null;

            var ip = ni.GetIPProperties()
                .UnicastAddresses
                .FirstOrDefault(a => a.Address.AddressFamily == AddressFamily.InterNetwork);

            return ip?.Address.ToString();
        }
        /// <summary>
        /// Stop the server and cancel background operations.
        /// </summary>
        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("WebSocketServer is stopping.");
            if (_cts != null)
            {
                await _cts.CancelAsync().ConfigureAwait(false);
            }
            try
            {
                if (_deadSocketReaperTask != null) await _deadSocketReaperTask.ConfigureAwait(false);
            }
            catch { /* ignore */ }

            try
            {
                if (_dtsPollingTask != null) await _dtsPollingTask.ConfigureAwait(false);
            }
            catch { /* ignore */ }

            try
            {
                if (_listenerTaskWss != null) await _listenerTaskWss.ConfigureAwait(false);
            }
            catch { /* ignore */ }

            try
            {
                if (_listenerTaskWs != null) await _listenerTaskWs.ConfigureAwait(false);
            }
            catch { /* ignore */ }

            if (_tcpListener != null)
            {
                try
                {
                    _tcpListener.Stop();
                    _logger.LogInformation("WSS TCP Listener stopped.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error stopping WSS TCP Listener");
                }
            }

            if (_wsListener?.IsListening == true)
            {
                _wsListener.Stop();
                _wsListener.Close();
                _logger.LogInformation("WS Listener stopped.");
            }

            if (_wssListener?.IsListening == true)
            {
                _wssListener.Stop();
                _wssListener.Close();
                _logger.LogInformation("WSS Listener stopped.");
            }

            _cts?.Dispose();
        }

        private async Task StartListening(HttpListener listener, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var context = await listener.GetContextAsync().ConfigureAwait(false);
                    if (context.Request.IsWebSocketRequest)
                    {
                        // handle but don't await so multiple connections can be accepted
                        _ = HandleWebSocketConnection(context, cancellationToken);
                    }
                    else
                    {
                        context.Response.StatusCode = 400;
                        context.Response.Close();
                    }
                }
                catch (HttpListenerException ex)
                {
                    _logger.LogInformation("HttpListener stopped: {Message}", ex.Message);
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in HttpListener.StartListening");
                }
            }
        }

        private async Task HandleWebSocketConnection(HttpListenerContext httpListenerContext, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Incoming WebSocket connection request.");
            HttpListenerWebSocketContext? webSocketContext = null;

            try
            {
                webSocketContext = await httpListenerContext.AcceptWebSocketAsync(null).ConfigureAwait(false);
                _logger.LogInformation("WebSocket connection accepted.");

                var webSocket = webSocketContext.WebSocket;
                string remoteEndpoint = httpListenerContext.Request.RemoteEndPoint?.ToString() ?? "unknown";
                await ProcessWebSocketAsync(webSocket, remoteEndpoint, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling WebSocket connection via HttpListener");
                try
                {
                    httpListenerContext.Response.StatusCode = 500;
                    httpListenerContext.Response.Close();
                }
                catch (Exception cleanupEx)
                {
                    _logger.LogDebug(cleanupEx, "Failed to send error response during WebSocket cleanup");
                }
            }
        }

        private async Task ProcessWebSocketAsync(WebSocket webSocket, string remoteEndpoint, CancellationToken cancellationToken)
        {
            _connectedSockets.TryAdd(webSocket, remoteEndpoint);
            _currentWebSocket = webSocket;
            _connectedClientSocket = webSocket;

            const int BufferSize = 1024 * 4;
            byte[]? buffer = null;
            var messageBuilder = new StringBuilder();

            try
            {
                buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

                while (webSocket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
                {
                    bool shouldContinue = await ReceiveNextWebSocketChunkAsync(
                        webSocket, buffer, messageBuilder, remoteEndpoint, cancellationToken).ConfigureAwait(false);

                    if (!shouldContinue)
                        break;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling WebSocket connection for {Endpoint}", remoteEndpoint);
            }
            finally
            {
                if (buffer != null)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }

                CleanupClosedWebSocket(webSocket, remoteEndpoint);
            }
        }

        private async Task<bool> ReceiveNextWebSocketChunkAsync(
            WebSocket webSocket, byte[] buffer, StringBuilder messageBuilder, string remoteEndpoint, CancellationToken cancellationToken)
        {
            WebSocketReceiveResult result;
            try
            {
                result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken).ConfigureAwait(false);
            }
            catch (WebSocketException wsEx)
            {
                _logger.LogWarning("WebSocket disconnected ({Endpoint}): {Message}", remoteEndpoint, wsEx.Message);
                return false;
            }

            if (result.MessageType == WebSocketMessageType.Close)
            {
                _logger.LogInformation("WebSocket connection closed by client ({Endpoint}).", remoteEndpoint);
                await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", cancellationToken).ConfigureAwait(false);
                return false;
            }

            messageBuilder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));

            if (!result.EndOfMessage)
                return true;

            var messageJson = messageBuilder.ToString();
            messageBuilder.Clear();
            _logger.LogInformation("Received WebSocket message from {Endpoint}: {Message}", remoteEndpoint, messageJson);

            await DeserializeAndDispatchMessageAsync(messageJson, webSocket, cancellationToken).ConfigureAwait(false);
            return true;
        }

        private async Task DeserializeAndDispatchMessageAsync(string messageJson, WebSocket webSocket, CancellationToken cancellationToken)
        {
            WebSocketMessage? message = null;
            try
            {
                message = JsonSerializer.Deserialize<WebSocketMessage>(messageJson);
                if (message?.Payload.ValueKind != JsonValueKind.Undefined)
                {
                    message.Payload = message.Payload.Clone();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to deserialize incoming WebSocket message.");
            }

            if (message != null)
            {
                await HandleWebSocketMessage(message, webSocket, cancellationToken).ConfigureAwait(false);
            }
        }

        private void CleanupClosedWebSocket(WebSocket webSocket, string remoteEndpoint)
        {
            _connectedSockets.TryRemove(webSocket, out _);

            _activeSessions.TryRemove(webSocket, out var removedSession);
            if (removedSession != null)
            {
                _logger.LogInformation("Session {SessionId} for user {Username} removed due to connection close ({Endpoint})",
                    removedSession.SessionId, removedSession.Username, remoteEndpoint);
            }

            if (_perClientBroadcastLocks.TryRemove(webSocket, out var sendLockToDispose))
            {
                sendLockToDispose.Dispose();
            }

            _channelSubscriptions.TryRemove(webSocket, out _);

            if (_currentWebSocket == webSocket) _currentWebSocket = null;
            if (_connectedClientSocket == webSocket) _connectedClientSocket = null;

            CheckDisableStreamingIfAllDisconnected();

            try
            {
                webSocket.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error disposing WebSocket during cleanup for {Endpoint}", remoteEndpoint);
            }
        }

        private Dictionary<string, Func<WebSocketMessage, WebSocket, CancellationToken, Task>>? _messageHandlers;

        private Dictionary<string, Func<WebSocketMessage, WebSocket, CancellationToken, Task>> GetMessageHandlers()
        {
            return _messageHandlers ??= new Dictionary<string, Func<WebSocketMessage, WebSocket, CancellationToken, Task>>(StringComparer.Ordinal)
            {
                ["login"] = HandleLoginCommand,
                ["refresh_token"] = HandleRefreshTokenCommand,
                ["add_user"] = HandleAddUserCommand,
                ["delete_user"] = HandleDeleteUserCommand,
                ["list_users"] = HandleListUsersCommand,
                ["get_audit_trail"] = async (msg, ws, ct) =>
                {
                    _logger.LogDebug("Routing to HandleGetAuditTrailCommand");
                    await HandleGetAuditTrailCommand(msg, ws, ct);
                    _logger.LogDebug("HandleGetAuditTrailCommand completed");
                },
                ["get_event_history"] = HandleGetEventHistoryCommand,
                ["SetPersistentIp"] = HandleSetPersistentIpAsync,
                ["GetFRMCVersion"] = HandleGetFRMCVersionCommandAsync,
                ["GetNetra1IP"] = (_, ws, ct) => HandleGetIpAsync(ROLE_NETRA1, "GetNetra1IP_response", ws, ct),
                ["SetNetra1IP"] = (msg, ws, ct) => HandleSetIpAsync(ROLE_NETRA1, "SetNetra1IP_response", msg, ws, ct),
                ["GetNetra2IP"] = (_, ws, ct) => HandleGetIpAsync(ROLE_NETRA2, "GetNetra2IP_response", ws, ct),
                ["SetNetra2IP"] = (msg, ws, ct) => HandleSetIpAsync(ROLE_NETRA2, "SetNetra2IP_response", msg, ws, ct),
                ["GetAvailableInterfaces"] = (_, ws, ct) => HandleGetAvailableInterfacesAsync(ws, ct),
                ["ConfigureEthernetRoles"] = HandleConfigureEthernetRolesAsync,
                ["GetDtsConnection"] = (_, ws, ct) => HandleGetDtsConnectionAsync(ws, ct),
                ["UpdateDtsConnection"] = HandleUpdateDtsConnectionAsync,
                ["GetFRMCTime"] = HandleGetFRMCTimeCommandAsync,
                ["SetFRMCTime"] = HandleSetFRMCTimeCommandAsync,
                ["GetZoneConfiguration"] = HandleGetZoneConfigurationCommandAsync,
                ["GetAvailableRelays"] = HandleGetAvailableRelaysCommandAsync,
                ["SetZoneConfiguration"] = HandleSetZoneConfigurationCommandAsync,
                ["UpdateZone"] = HandleUpdateZoneCommandAsync,
                ["GetChannelConfig"] = HandleGetChannelConfigCommandAsync,
                ["GetDtsCalibration"] = HandleGetDtsCalibrationCommandAsync,
                ["SetDtsCalibration"] = HandleSetDtsCalibrationCommandAsync,
                ["GetZoneState"] = HandleGetZoneStateCommandAsync,
                ["GetAllChannels"] = HandleGetAllChannelsCommandAsync,
                ["SetChannelConfig"] = HandleSetChannelConfigCommandAsync,
                ["GetChannelCount"] = HandleGetChannelCountCommandAsync,
                ["GetZoneConfig"] = HandleGetZoneConfigCommandAsync,
                ["GetZonePoints"] = HandleGetZonePointsCommandAsync,
                ["GetChannelPoints"] = HandleGetChannelPointsCommandAsync,
                ["start_measurement"] = HandleStartMeasurementCommandAsync,
                ["stop_measurement"] = HandleStopMeasurementCommandAsync,
                ["ReloadChannels"] = HandleReloadChannelsCommandAsync,
                ["get_active_alarm"] = HandleGetActiveAlarmCommandAsync,
                ["GetSystemHealth"] = HandleGetSystemHealthCommandAsync,
                ["GetSystemConfig"] = HandleGetSystemConfigCommandAsync,
                ["SetSystemConfig"] = HandleSetSystemConfigCommandAsync,
                ["USBConfigurationCommand"] = (msg, ws, ct) => UsbConfigHandler.HandleCommandAsync(msg, ws, SendResponse, _logger, ct),
                ["ResetAllConfiguration"] = HandleResetAllConfigurationCommandAsync,
                ["GetRelayStates"] = HandleGetRelayStatesCommandAsync,
                ["SetRelayState"] = HandleSetRelayStateCommandAsync,
                ["GetHealthParameters"] = HandleGetHealthParametersCommandAsync,
                ["GetActiveHealthFaults"] = HandleGetActiveHealthFaultsCommandAsync,
                ["reset_active_alarm"] = HandleResetActiveAlarmCommandAsync,
                ["subscribe_channel"] = HandleSubscribeChannelCommandAsync,
                ["unsubscribe_channel"] = HandleUnsubscribeChannelCommandAsync,
                ["SetDecimationConfig"] = HandleSetDecimationConfigCommandAsync,
                ["GetDecimationConfig"] = HandleGetDecimationConfigCommandAsync,
            };
        }

        private async Task HandleWebSocketMessage(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            _logger.LogInformation("⚡ HandleWebSocketMessage called with MessageType: {MessageType}", message.MessageType);

            if (message.MessageType != null && GetMessageHandlers().TryGetValue(message.MessageType, out var handler))
            {
                await handler(message, webSocket, cancellationToken).ConfigureAwait(false);
                return;
            }

            _logger.LogWarning("Unhandled message type: {MessageType}", message.MessageType);
        }

        private async Task HandleGetFRMCVersionCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            _logger.LogInformation("FRMC software version requested");

            var systemConfig = await _configurationManager.GetSystemConfigurationAsync();
            string version = systemConfig?.SoftwareVersion ?? "Unknown";

            await SendResponse(webSocket, new WebSocketMessage
            {
                MessageType = "GetFRMCVersion_response",
                Payload = JsonSerializer.SerializeToElement(new
                {
                    Success = true,
                    Version = version
                })
            }, cancellationToken).ConfigureAwait(false);
        }

        private async Task HandleGetFRMCTimeCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            var currentTime = DateTime.Now;

            _logger.LogInformation("FRMC time requested. Current time: {Time:yyyy-MM-dd HH:mm:ss}", currentTime);

            await SendResponse(webSocket, new WebSocketMessage
            {
                MessageType = "GetFRMCTime_response",
                Payload = JsonSerializer.SerializeToElement(new
                {
                    CurrentTime = currentTime.ToString("yyyy-MM-dd HH:mm:ss")
                })
            }, cancellationToken).ConfigureAwait(false);
        }

        private async Task HandleSetFRMCTimeCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
                                    // Admin authentication required
                                    if (!IsAdmin(webSocket))
                                    {
                                        await SendResponse(webSocket, new WebSocketMessage
                                        {
                                            MessageType = "SetFRMCTime_response",
                                            Payload = JsonSerializer.SerializeToElement(new
                                            {
                                                Success = false,
                                                Message = "Admin authentication required"
                                            })
                                        }, cancellationToken);
                                        return;
                                    }

                                    string? newTimeStr = null;

                                    if (message.Payload.TryGetProperty("DeviceTime", out var deviceTimeProp))
                                        newTimeStr = deviceTimeProp.GetString();
                                    else if (message.Payload.TryGetProperty("NewTime", out var newTimeProp))
                                        newTimeStr = newTimeProp.GetString();

                                    if (string.IsNullOrWhiteSpace(newTimeStr))
                                    {
                                        await SendResponse(webSocket, new WebSocketMessage
                                        {
                                            MessageType = "SetFRMCTime_response",
                                            Payload = JsonSerializer.SerializeToElement(new
                                            {
                                                Success = false,
                                                Message = "Missing DeviceTime / NewTime"
                                            })
                                        }, cancellationToken);
                                        return;
                                    }

                                    if (!DateTime.TryParse(newTimeStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out var newTime))
                                    {
                                        await SendResponse(webSocket, new WebSocketMessage
                                        {
                                            MessageType = "SetFRMCTime_response",
                                            Payload = JsonSerializer.SerializeToElement(new
                                            {
                                                Success = false,
                                                Message = "Invalid time format"
                                            })
                                        }, cancellationToken);
                                        return;
                                    }

                                    try
                                    {
            #if WINDOWS
                                        SYSTEMTIME st = new SYSTEMTIME
                                        {
                                            Year = (ushort)newTime.Year,
                                            Month = (ushort)newTime.Month,
                                            Day = (ushort)newTime.Day,
                                            Hour = (ushort)newTime.Hour,
                                            Minute = (ushort)newTime.Minute,
                                            Second = (ushort)newTime.Second,
                                            Milliseconds = (ushort)newTime.Millisecond
                                        };

                                        if (!SetLocalTime(ref st))
                                        {
                                            int error = Marshal.GetLastWin32Error();
                                            throw new InvalidOperationException($"Windows SetLocalTime failed. Error={error}");
                                        }
            #else
                                        // Disable NTP first, otherwise set-time might fail
                                        using (var ntpProc = Process.Start(new ProcessStartInfo
                                        {
                                            FileName = "timedatectl",
                                            Arguments = "set-ntp false",
                                            UseShellExecute = false,
                                            CreateNoWindow = true
                                        }))
                                        {
                                            if (ntpProc != null)
                                                await ntpProc.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
                                        }

                                        string formatted = newTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                                        using (var setTimeProc = Process.Start(new ProcessStartInfo
                                        {
                                            FileName = "timedatectl",
                                            Arguments = $"set-time \"{formatted}\"",
                                            UseShellExecute = false,
                                            CreateNoWindow = true
                                        }))
                                        {
                                            if (setTimeProc != null)
                                                await setTimeProc.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
                                        }
            #endif

                                        await SendResponse(webSocket, new WebSocketMessage
                                        {
                                            MessageType = "SetFRMCTime_response",
                                            Payload = JsonSerializer.SerializeToElement(new
                                            {
                                                Success = true,
                                                UpdatedTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                                            })
                                        }, cancellationToken);
                                    }
                                    catch (Exception ex)
                                    {
                                        _logger.LogError(ex, "SetFRMCTime failed");

                                        await SendResponse(webSocket, new WebSocketMessage
                                        {
                                            MessageType = "SetFRMCTime_response",
                                            Payload = JsonSerializer.SerializeToElement(new
                                            {
                                                Success = false,
                                                Message = ex.Message
                                            })
                                        }, cancellationToken);
                                    }
        }

        private async Task HandleGetZoneConfigurationCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                // Optional ChannelId filter: if provided, return only that channel's zones
                int? filterChannelId = null;
                if (message.Payload.ValueKind == JsonValueKind.Object &&
                    message.Payload.TryGetProperty("ChannelId", out var chIdElem) &&
                    chIdElem.ValueKind == JsonValueKind.Number)
                {
                    filterChannelId = chIdElem.GetInt32();
                }

                _logger.LogInformation("📥 GetZoneConfiguration (ChannelId filter: {Filter})",
                    filterChannelId?.ToString() ?? "all");

                var channelConfigs = _configurationManager.GetChannels();

                // If filtering by channel, only pass that one channel to the mapper
                var channelsToMap = filterChannelId.HasValue
                    ? channelConfigs.Where(c => c.Configuration.ChannelId == filterChannelId.Value)
                    : channelConfigs;

                var zoneMap = ZoneMapper.ConvertToDtscmZoneMap(channelsToMap);

                foreach (var channel in zoneMap)
                {
                    _logger.LogInformation("🔍 Channel: {Channel}, Zones: {Count}",
                        channel.Key, channel.Value.Count);
                }

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetZoneConfiguration_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Zones = zoneMap
                    })
                }, cancellationToken).ConfigureAwait(false);

                _logger.LogInformation("✔ GetZoneConfiguration completed successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ GetZoneConfiguration failed");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetZoneConfiguration_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleGetAvailableRelaysCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            var relayManager =
                _serviceProvider.GetRequiredService<IRelayControlManager>();

            // Extract optional exclude parameters for relay reassignment
            // When editing a zone, pass its ChannelId and ZoneId to include its currently assigned relay
            int? excludeChannelId = null;
            int? excludeZoneId = null;

            if (message.Payload.TryGetProperty("ExcludeChannelId", out var excludeChProp) &&
                excludeChProp.TryGetInt32(out var chId))
            {
                excludeChannelId = chId;
            }

            if (message.Payload.TryGetProperty("ExcludeZoneId", out var excludeZProp) &&
                excludeZProp.TryGetInt32(out var zId))
            {
                excludeZoneId = zId;
            }

            var availableRelays = relayManager.GetAvailableRelays(excludeChannelId, excludeZoneId);

            var payload = JsonSerializer.SerializeToElement(new
            {
                Success = true,
                AvailableRelays = availableRelays
            });

            await SendResponse(
                webSocket,
                new WebSocketMessage("GetAvailableRelays_response", payload),
                cancellationToken);
        }

        private async Task HandleSetZoneConfigurationCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            // Authentication required
            if (!IsAdmin(webSocket))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetZoneConfiguration_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Admin authentication required"
                    })
                }, cancellationToken);
                return;
            }

            try
            {
                _logger.LogInformation("📥 Received WebSocket message: SetZoneConfiguration");

                if (!message.Payload.TryGetProperty("Zones", out var zonesProp))
                    throw new ArgumentException("Missing 'Zones' in payload");

                TryGetSession(webSocket, out var session);
                string username = session?.Username ?? "Unknown";

                var zoneMap = JsonSerializer.Deserialize<Dictionary<string, List<ZoneInfo>>>(zonesProp.GetRawText())
                    ?? throw new InvalidOperationException("Failed to parse zone configuration payload");

                var frmcZones = ZoneMapper.ConvertToFrmcZoneList(zoneMap);
                var systemConfig = await _configurationManager.GetSystemConfigurationAsync();
                ValidateZoneRelayAssignments(frmcZones, systemConfig);

                foreach (var g in frmcZones.GroupBy(z => z.ChannelId))
                {
                    await _configurationManager.ReplaceZoneConfigurationAsync(g.Key, g.ToList(), username);
                }

                var disabledZones = frmcZones.Where(z => !z.Enabled).ToList();
                if (disabledZones.Count > 0)
                {
                    foreach (var dz in disabledZones)
                    {
                        await ClearDisabledZoneAlarmsAsync(dz.ChannelId, dz.ZoneId);
                    }
                    _logger.LogInformation("SetZoneConfiguration: cleared alarms for {Count} disabled zones", disabledZones.Count);
                }

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetZoneConfiguration_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = true })
                }, cancellationToken).ConfigureAwait(false);

                _logger.LogInformation("✔ SetZoneConfiguration responded. Reloading channels in background...");
                TriggerBackgroundChannelReload(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ SetZoneConfiguration failed");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetZoneConfiguration_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = false, Error = ex.Message })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleUpdateZoneCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            // Authentication required
            if (!IsAdmin(webSocket))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "UpdateZone_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Admin authentication required"
                    })
                }, cancellationToken);
                return;
            }

            try
            {
                _logger.LogInformation("📥 Received WebSocket message: UpdateZone");

                if (!message.Payload.TryGetProperty("ChannelId", out var channelIdProp))
                    throw new ArgumentException("Missing 'ChannelId' in payload");

                if (!message.Payload.TryGetProperty("Zone", out var zoneProp))
                    throw new ArgumentException("Missing 'Zone' in payload");

                int channelId = channelIdProp.GetInt32();
                TryGetSession(webSocket, out var session);
                string username = session?.Username ?? "Unknown";

                var auditTrailService = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Managers.SystemConfigurationManager.AuditTrailService>();
                var zoneConfig = ParseZoneConfigurationFromPayload(channelId, zoneProp);
                int zoneId = zoneConfig.ZoneId;

                var oldZone = _configurationManager.GetZoneConfigurations()
                    .FirstOrDefault(z => z.ChannelId == channelId && z.ZoneId == zoneId);

                if (!await ValidateUpdateZoneParametersAsync(zoneConfig, webSocket, cancellationToken))
                {
                    return;
                }

                bool success = await _configurationManager.UpdateSingleZoneAsync(channelId, zoneId, zoneConfig, username);
                if (!success)
                {
                    throw new InvalidOperationException($"Failed to update Zone {zoneId} in Channel {channelId}");
                }

                string key = $"Channel {channelId}, Zone {zoneId}";
                await auditTrailService.LogConfigurationChangeAsync(
                    FireRakshak.FRMC.Core.Enums.ConfigurationType.Zone,
                    username,
                    key,
                    oldZone != null ? (object)oldZone : "New Zone",
                    zoneConfig
                );

                if (!zoneConfig.Enabled)
                {
                    _logger.LogWarning("Zone {ZoneId} in Channel {ChannelId} disabled — clearing alarms", zoneId, channelId);
                    int clearedCount = await ClearDisabledZoneAlarmsAsync(channelId, zoneId);
                    _logger.LogInformation("Zone {ZoneId} in Channel {ChannelId}: cleared {Count} alarms and reset state",
                        zoneId, channelId, clearedCount);
                }

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "UpdateZone_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = true })
                }, cancellationToken).ConfigureAwait(false);

                _logger.LogInformation("✔ UpdateZone responded for Zone {ZoneId} in Channel {ChannelId}. Reloading channels in background...", zoneId, channelId);
                TriggerBackgroundChannelReload(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ UpdateZone failed");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "UpdateZone_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = false, Error = ex.Message })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private static void ValidateZoneRelayAssignments(List<ZoneConfiguration> frmcZones, SystemConfiguration? systemConfig)
        {
            foreach (var zone in frmcZones)
            {
                if (!zone.AssignedRelayId.HasValue)
                    continue;

                int relayId = zone.AssignedRelayId.Value;
                if (systemConfig == null || relayId < 1 || relayId > systemConfig.NumberOfRelays)
                {
                    throw new ArgumentException(
                        $"Invalid relay ID {relayId} for Channel {zone.ChannelId}, Zone {zone.ZoneId}. " +
                        $"Must be between 1 and {systemConfig?.NumberOfRelays ?? 0}");
                }
            }
        }

        private static ZoneConfiguration ParseZoneConfigurationFromPayload(int channelId, JsonElement zoneProp)
        {
            int zoneId = zoneProp.GetProperty("ZoneId").GetInt32();
            return new ZoneConfiguration
            {
                ChannelId = channelId,
                ZoneId = zoneId,
                Name = zoneProp.TryGetProperty("Name", out var nameProp) ? nameProp.GetString() ?? $"Zone {zoneId}" : $"Zone {zoneId}",
                Enabled = zoneProp.TryGetProperty("Enabled", out var enabledProp) && enabledProp.GetBoolean(),
                StartPoint = GetDoublePropertyOrDefault(zoneProp, "StartPoint"),
                EndPoint = GetDoublePropertyOrDefault(zoneProp, "EndPoint"),
                MaxTemp = GetDoublePropertyOrDefault(zoneProp, "MaxTemp"),
                MinTemp = GetDoublePropertyOrDefault(zoneProp, "MinTemp"),
                PreAlarm = GetDoublePropertyOrDefault(zoneProp, "PreAlarm"),
                RoRThreshold = GetDoublePropertyOrDefault(zoneProp, "RoRThreshold"),
                DeviationThreshold = GetDoublePropertyOrDefault(zoneProp, "DeviationThreshold"),
                IsMaxTempEnabled = GetDualCaseBool(zoneProp, "isMaxTempEnabled", "IsMaxTempEnabled"),
                IsMinTempEnabled = GetDualCaseBool(zoneProp, "isMinTempEnabled", "IsMinTempEnabled"),
                IsPreAlarmEnabled = GetDualCaseBool(zoneProp, "isPreAlarmEnabled", "IsPreAlarmEnabled"),
                IsRateOfRiseEnabled = GetDualCaseBool(zoneProp, "isRateOfRiseEnabled", "IsRateOfRiseEnabled"),
                IsDeviationEnabled = GetDualCaseBool(zoneProp, "isDeviationEnabled", "IsDeviationEnabled"),
                AssignedRelayId = GetNullableIntDualCase(zoneProp, "assignedRelayId", "AssignedRelayId")
            };
        }

        private static double GetDoublePropertyOrDefault(JsonElement element, string name) =>
            element.TryGetProperty(name, out var prop) ? prop.GetDouble() : 0;

        private static bool GetDualCaseBool(JsonElement element, string camelName, string pascalName) =>
            (element.TryGetProperty(camelName, out var prop) || element.TryGetProperty(pascalName, out prop)) && prop.GetBoolean();

        private static int? GetNullableIntDualCase(JsonElement element, string camelName, string pascalName)
        {
            if ((element.TryGetProperty(camelName, out var prop) || element.TryGetProperty(pascalName, out prop)) &&
                prop.ValueKind != JsonValueKind.Null)
            {
                return prop.GetInt32();
            }
            return null;
        }

        private async Task<bool> ValidateUpdateZoneParametersAsync(ZoneConfiguration zoneConfig, WebSocket webSocket, CancellationToken cancellationToken)
        {
            string? errorMessage = GetUpdateZoneValidationError(zoneConfig);

            if (errorMessage == null && zoneConfig.AssignedRelayId.HasValue)
            {
                var systemConfig = await _configurationManager.GetSystemConfigurationAsync();
                if (systemConfig == null || zoneConfig.AssignedRelayId.Value < 1 || zoneConfig.AssignedRelayId.Value > systemConfig.NumberOfRelays)
                {
                    errorMessage = $"Invalid relay ID {zoneConfig.AssignedRelayId.Value}. Must be between 1 and {systemConfig?.NumberOfRelays ?? 64}";
                }
            }

            if (errorMessage != null)
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "UpdateZone_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = false, Error = errorMessage })
                }, cancellationToken);
                return false;
            }

            return true;
        }

        private static string? GetUpdateZoneValidationError(ZoneConfiguration zoneConfig)
        {
            if (zoneConfig.StartPoint >= zoneConfig.EndPoint && !(zoneConfig.StartPoint == 0 && zoneConfig.EndPoint == 0))
                return $"StartPoint ({zoneConfig.StartPoint}) must be less than EndPoint ({zoneConfig.EndPoint})";

            if (zoneConfig.IsMaxTempEnabled && zoneConfig.IsMinTempEnabled && zoneConfig.MaxTemp <= zoneConfig.MinTemp)
                return $"MaxTemp ({zoneConfig.MaxTemp}) must be greater than MinTemp ({zoneConfig.MinTemp})";

            if (zoneConfig.IsPreAlarmEnabled && zoneConfig.IsMaxTempEnabled && zoneConfig.PreAlarm >= zoneConfig.MaxTemp)
                return $"PreAlarm ({zoneConfig.PreAlarm}) must be less than MaxTemp ({zoneConfig.MaxTemp})";

            if (zoneConfig.RoRThreshold < 0 || zoneConfig.DeviationThreshold < 0)
                return "RoR threshold and Deviation threshold must be non-negative";

            return null;
        }

        private async Task<int> ClearDisabledZoneAlarmsAsync(int channelId, int zoneId)
        {
            var alarmMgr = _serviceProvider.GetRequiredService<IAlarmManager>();
            var activeAlarms = await alarmMgr.GetActiveAlarmsForChannelAsync(channelId, zoneId);
            foreach (var alarm in activeAlarms)
            {
                await alarmMgr.ClearAlarmAsync(alarm.AlarmId);
            }

            var channels = _configurationManager.GetChannels();
            var channel = channels.FirstOrDefault(c => c.Configuration.ChannelId == channelId);
            var zone = channel?.Zones.FirstOrDefault(z => z.Configuration.ZoneId == zoneId);
            if (zone != null)
            {
                ResetZoneAlarmStateFlags(zone.State);
            }

            return activeAlarms.Count;
        }

        private static void ResetZoneAlarmStateFlags(ZoneState state)
        {
            state.MaxAlarm = false;
            state.MinAlarm = false;
            state.PreAlarm = false;
            state.RorAlarm = false;
            state.DeviationAlarm = false;
            state.MaxAlarmBreachValue = null;
            state.MaxAlarmBreachPosition = null;
            state.MinAlarmBreachValue = null;
            state.MinAlarmBreachPosition = null;
            state.PreAlarmBreachValue = null;
            state.PreAlarmBreachPosition = null;
            state.RorAlarmBreachValue = null;
            state.RorAlarmBreachPosition = null;
            state.DeviationAlarmBreachValue = null;
            state.DeviationAlarmBreachPosition = null;
        }

        private void TriggerBackgroundChannelReload(CancellationToken cancellationToken)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    var cm = _serviceProvider.GetService(typeof(ChannelsManager)) as ChannelsManager;
                    if (cm != null)
                        await cm.InitializeChannelsAsync();
                    _logger.LogInformation("✔ Channel reload completed (background).");

                    var relayMgr = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Managers.RelayManager.IRelayControlManager>();
                    await relayMgr.CleanupUnassignedRelaysAsync();
                }
                catch (Exception bgEx)
                {
                    _logger.LogError(bgEx, "❌ Background channel reload failed");
                }
            }, cancellationToken);
        }

        private async Task HandleGetChannelConfigCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                int requestedId = message.Payload.GetProperty("ChannelId").GetInt32();


                int internalId = requestedId;

                var channels = _configurationManager.GetChannels();

                ChannelConfiguration? foundConfig = null;

                foreach (var ch in channels)
                {
                    ChannelConfiguration? cfg = null;


                    var configProp = ch.GetType()
                        .GetProperties()
                        .FirstOrDefault(p =>
                            string.Equals(p.Name, "Configuration", StringComparison.OrdinalIgnoreCase));

                    object? rawCfg = configProp?.GetValue(ch);


                    if (rawCfg == null)
                        continue;


                    if (rawCfg is JsonElement je)
                    {
                        cfg = JsonSerializer.Deserialize<ChannelConfiguration>(je.GetRawText());
                    }
                    else
                    {
                        cfg = rawCfg as ChannelConfiguration;
                    }

                    if (cfg == null)
                        continue;


                    int channelId = cfg.ChannelId;


                    if (channelId == internalId)
                    {
                        foundConfig = cfg;
                        break;
                    }
                }


                if (foundConfig == null)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetChannelConfig_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = $"Channel {requestedId} not found"
                        })
                    }, cancellationToken);

                    return;
                }
                var confg = await _configurationManager.GetChannelConfigurationAsync(foundConfig.ChannelId);
                CorrectionLength = confg?.CorrectionLength;

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetChannelConfig_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Config = foundConfig
                    })
                }, cancellationToken);

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GetChannelConfig failed");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetChannelConfig_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleGetDtsCalibrationCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                var dtsManager = _serviceProvider.GetService<IDtsManager>();
                if (dtsManager == null)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetDtsCalibration_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = "DTS Manager service is not available."
                        })
                    }, cancellationToken).ConfigureAwait(false);
                    return;
                }

                var (success, iniPayload, error) = await dtsManager.ReadConfigurationIniAsync();

                if (success && iniPayload != null)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetDtsCalibration_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = true,
                            IniPayload = iniPayload,
                            Message = "DTS configuration read successfully"
                        })
                    }, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetDtsCalibration_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = error ?? "Failed to read configuration from DTS hardware."
                        })
                    }, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GetDtsCalibration failed");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetDtsCalibration_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleSetDtsCalibrationCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                var dtsManager = _serviceProvider.GetService<IDtsManager>();
                if (dtsManager == null)
                {
                    await SendSetDtsCalibrationErrorAsync(webSocket, "DTS Manager service is not available.", cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (!ExtractDtsCalibrationPayload(message.Payload, out string iniPayload, out byte? channelIndex, out string? extractError))
                {
                    await SendSetDtsCalibrationErrorAsync(webSocket, extractError ?? "Invalid INI payload", cancellationToken).ConfigureAwait(false);
                    return;
                }

                var (success, readBackIni, error) = await dtsManager.ApplyCalibrationAndReadBackAsync(iniPayload, channelIndex);

                if (success)
                {
                    var updatedPoints = GetUpdatedChannelPoints(channelIndex);
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "SetDtsCalibration_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = true,
                            IniPayload = readBackIni ?? string.Empty,
                            ChannelId = channelIndex.HasValue ? channelIndex.Value + 1 : 0,
                            Points = updatedPoints,
                            Message = "DTS configuration written and read back successfully"
                        })
                    }, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await SendSetDtsCalibrationErrorAsync(webSocket, error ?? "Failed to apply calibration configuration to DTS hardware.", cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SetDtsCalibration failed");
                await SendSetDtsCalibrationErrorAsync(webSocket, ex.Message, cancellationToken).ConfigureAwait(false);
            }
        }

        private static bool ExtractDtsCalibrationPayload(JsonElement payload, out string iniPayload, out byte? channelIndex, out string? errorMessage)
        {
            iniPayload = string.Empty;
            channelIndex = null;
            errorMessage = null;

            if (payload.ValueKind == JsonValueKind.Object && payload.TryGetProperty("IniPayload", out var iniElement))
            {
                iniPayload = iniElement.GetString() ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(iniPayload))
            {
                errorMessage = "Invalid or empty INI configuration payload provided.";
                return false;
            }

            if (payload.TryGetProperty("ChannelId", out var chProp) && chProp.TryGetInt32(out int chId) && chId >= 1)
            {
                channelIndex = (byte)(chId - 1);
            }
            else if (payload.TryGetProperty("ChannelIndex", out var chIdxProp) && chIdxProp.TryGetInt32(out int chIdx) && chIdx >= 0)
            {
                channelIndex = (byte)chIdx;
            }

            return true;
        }

        private List<object>? GetUpdatedChannelPoints(byte? channelIndex)
        {
            if (!channelIndex.HasValue)
                return null;

            var channels = _configurationManager.GetChannels();
            var targetChannel = channels.FirstOrDefault(c => c.Configuration.ChannelId == channelIndex.Value + 1);
            if (targetChannel?.State?.CurrentData == null || targetChannel.State.CurrentData.Length == 0)
                return null;

            var validData = targetChannel.State.CurrentData
                .Where(p => p.Position <= targetChannel.State.ActualLength && p.Temperature > -200)
                .ToList();

            if (validData.Count == 0)
                return null;

            return validData
                .Select(p => (object)new { Position = p.Position, Temperature = p.Temperature })
                .ToList();
        }

        private async Task SendSetDtsCalibrationErrorAsync(WebSocket webSocket, string error, CancellationToken cancellationToken)
        {
            await SendResponse(webSocket, new WebSocketMessage
            {
                MessageType = "SetDtsCalibration_response",
                Payload = JsonSerializer.SerializeToElement(new
                {
                    Success = false,
                    Error = error
                })
            }, cancellationToken).ConfigureAwait(false);
        }

        private async Task HandleGetZoneStateCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            int channelId = message.Payload.GetProperty("ChannelId").GetInt32();

            var channels = _configurationManager.GetChannels();
            var channel = channels.FirstOrDefault(c => c.Configuration.ChannelId == channelId);

            if (channel == null)
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetZoneState_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = $"Channel {channelId} not found"
                    })
                }, cancellationToken);

                return;
            }


            await SendResponse(webSocket, new WebSocketMessage
            {
                MessageType = "GetZoneState_response",
                Payload = JsonSerializer.SerializeToElement(new
                {
                    Success = true,
                    ChannelId = channel.Configuration.ChannelId,
                    Zones = channel.Zones.Select(z => new
                    {
                        z.State.ZoneId,
                        z.State.Name,
                        z.State.StartPoint,
                        z.State.EndPoint,
                        Enabled = z.State.Enabled,
                        //z.State.AverageTemperature,
                        //z.State.MaxTemperature,
                        //z.State.MinTemperature,
                        //z.State.RateOfRise,
                        //z.State.Deviation,
                        RateOfRise = double.IsFinite(z.State.RateOfRise) ? z.State.RateOfRise : 0,
                        Deviation = double.IsFinite(z.State.Deviation) ? z.State.Deviation : 0,
                        AverageTemperature = double.IsFinite(z.State.AverageTemperature) ? z.State.AverageTemperature : 0,
                        MaxTemperature = double.IsFinite(z.State.MaxTemperature) ? z.State.MaxTemperature : 0,
                        MinTemperature = double.IsFinite(z.State.MinTemperature) ? z.State.MinTemperature : 0,

                        z.State.ActiveAlarm,
                        z.State.PreAlarm,
                        z.State.MaxAlarm,
                        z.State.MinAlarm,
                        z.State.RorAlarm,
                        z.State.DeviationAlarm,
                        z.State.LastUpdate
                    })
                })
            }, cancellationToken);
        }

        private async Task HandleGetAllChannelsCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                var channels = _configurationManager.GetChannels()
                    .Select(c =>
                    {

                        var prop = c.GetType().GetProperty("Configuration");
                        if (prop != null)
                        {
                            return prop.GetValue(c);
                        }
                        return c;
                    })
                    .ToList();

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetAllChannels_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = true, Channels = channels })
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GetAllChannels failed");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetAllChannels_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = false, Error = ex.Message })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleSetChannelConfigCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            // Authentication required
            if (!IsAdmin(webSocket))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetChannelConfig_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Admin authentication required"
                    })
                }, cancellationToken);
                return;
            }

            try
            {

                if (!message.Payload.TryGetProperty("Config", out var cfgProp))
                {
                    throw new ArgumentException("Payload missing Config property");
                }

                var cfgJson = cfgProp.GetRawText();
                var newCfg = JsonSerializer.Deserialize<ChannelConfiguration>(cfgJson);

                if (newCfg == null)
                    throw new InvalidOperationException("Failed to deserialize ChannelConfiguration");

                // Validate channel ID - must be >= 1
                if (newCfg.ChannelId < 1)
                {
                    throw new ArgumentException($"Invalid Channel ID {newCfg.ChannelId}. Channel IDs must be 1 or greater.");
                }

                // Get username for audit trail
                TryGetSession(webSocket, out var session);
                string username = session?.Username ?? "Unknown";

                // Get audit trail service
                var auditTrailService = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Managers.SystemConfigurationManager.AuditTrailService>();

                // Get old configuration for audit trail
                var oldCfg = await _configurationManager.GetChannelConfigurationAsync(newCfg.ChannelId);

                // Save new configuration
                await _configurationManager.AddOrUpdateChannelConfigurationAsync(newCfg, username).ConfigureAwait(false);
                // Log audit trail
                string key = $"Channel {newCfg.ChannelId}";
                await auditTrailService.LogConfigurationChangeAsync(
                    FireRakshak.FRMC.Core.Enums.ConfigurationType.Channel,
                    username,
                    key,
                    oldCfg != null ? (object)oldCfg : "New Channel",
                    newCfg
                );


                if (_serviceProvider != null)
                {
                    await ReloadChannelsAndHandleDisabledChannelAsync(newCfg).ConfigureAwait(false);
                }

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetChannelConfig_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = true })
                }, cancellationToken).ConfigureAwait(false);

                // Broadcast channel config update to all clients
                var broadcastPayload = new
                {
                    ChannelId = newCfg.ChannelId,
                    ChannelLength = newCfg.ChannelLength,
                    Name = newCfg.Name,
                    IsEnabled = newCfg.IsEnabled,
                    ScanPeriod = newCfg.ScanPeriod,
                    NumberOfZones = newCfg.NumberOfZones,
                    Timestamp = DateTime.UtcNow.ToString("o")
                };
                var broadcastMsg = new WebSocketMessage(
                    "channel_config_updated",
                    JsonSerializer.SerializeToElement(broadcastPayload));
                await BroadcastMessageAsync(broadcastMsg).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SetChannelConfig failed");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetChannelConfig_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = false, Error = ex.Message })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task ReloadChannelsAndHandleDisabledChannelAsync(ChannelConfiguration newCfg)
        {
            try
            {
                var cm = _serviceProvider.GetService(typeof(ChannelsManager)) as ChannelsManager;
                var initMethod = cm?.GetType().GetMethod("InitializeChannelsAsync");
                if (initMethod != null && initMethod.Invoke(cm, null) is Task task)
                {
                    await task.ConfigureAwait(false);
                }

                if (!newCfg.IsEnabled)
                {
                    await ClearDisabledChannelAlarmsAsync(newCfg.ChannelId).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to invoke ChannelsManager.InitializeChannelsAsync after config update");
            }
        }

        private async Task ClearDisabledChannelAlarmsAsync(int channelId)
        {
            _logger.LogWarning("Channel {ChannelId} disabled — clearing alarms and relay states", channelId);

            var alarmMgr = _serviceProvider.GetRequiredService<IAlarmManager>();
            var activeAlarms = await alarmMgr.GetActiveAlarmsForChannelAsync(channelId);
            foreach (var alarm in activeAlarms)
            {
                await alarmMgr.ClearAlarmAsync(alarm.AlarmId);
            }

            var channel = _configurationManager.GetChannels()
                .FirstOrDefault(c => c.Configuration.ChannelId == channelId);
            if (channel != null)
            {
                channel.State.IsActive = false;
                foreach (var zone in channel.Zones)
                {
                    ResetZoneAlarmStateFlags(zone.State);
                }
            }

            _logger.LogInformation("Channel {ChannelId}: cleared {Count} alarms and reset zone states",
                channelId, activeAlarms.Count);
        }

        private async Task HandleGetChannelCountCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                // Get channel count from configuration manager
                int count = _configurationManager.GetChannels().Count;

                // Log it
                _logger.LogInformation("📤 Sending GetChannelCountReply: Count = {Count}", count);

                // Send reply
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetChannelCount_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Count = count
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ GetChannelCount failed");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetChannelCount_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleGetZoneConfigCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                int channelId = message.Payload.GetProperty("ChannelId").GetInt32();
                int index = message.Payload.GetProperty("Index").GetInt32();

                _logger.LogInformation("📥 GetZoneConfig received → Channel {Ch}, ZoneIndex {Idx}", channelId, index);

                // Extract fresh config (after reload)
                var channels = _configurationManager.GetChannels();
                var channel = channels.FirstOrDefault(c => c.Configuration.ChannelId == channelId);

                if (channel == null || channel.Zones.Count == 0)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetZoneConfig_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = $"No zones defined for channel {channelId}"
                        })
                    }, cancellationToken);

                    return;
                }

                if (index < 0 || index >= channel.Zones.Count)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetZoneConfig_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = $"Zone index {index} out of range for channel {channelId}"
                        })
                    }, cancellationToken);

                    return;
                }

                var zone = channel.Zones[index].Configuration;

                // Return full zone configuration including relay fields
                var res = new
                {
                    ChannelId = zone.ChannelId,
                    ZoneId = zone.ZoneId,
                    Name = zone.Name,
                    StartPoint = zone.StartPoint,
                    EndPoint = zone.EndPoint,
                    MaxTemp = zone.MaxTemp,
                    MinTemp = zone.MinTemp,
                    PreAlarm = zone.PreAlarm,
                    RoRThreshold = zone.RoRThreshold,
                    DeviationThreshold = zone.DeviationThreshold,
                    Enabled = zone.Enabled,

                    isMaxTempEnabled = zone.IsMaxTempEnabled,
                    isMinTempEnabled = zone.IsMinTempEnabled,
                    isPreAlarmEnabled = zone.IsPreAlarmEnabled,
                    isRateOfRiseEnabled = zone.IsRateOfRiseEnabled,
                    isDeviationEnabled = zone.IsDeviationEnabled,

                    // Relay assignment - relay triggers on ANY enabled alarm for this zone
                    assignedRelayId = zone.AssignedRelayId
                };

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetZoneConfig_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Zone = res
                    })
                }, cancellationToken);

                _logger.LogInformation("📤 GetZoneConfig_response sent successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ GetZoneConfig failed");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetZoneConfig_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleGetZonePointsCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                int channelId = message.Payload.GetProperty("ChannelId").GetInt32();
                int zoneId = message.Payload.GetProperty("ZoneId").GetInt32();

                _logger.LogInformation("📥 GetZonePoints received → Channel {Ch}, Zone {Zone}", channelId, zoneId);

                var channels = _configurationManager.GetChannels();
                var channel = channels.FirstOrDefault(c => c.Configuration.ChannelId == channelId);

                if (channel == null)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetZonePoints_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = $"Channel {channelId} not found"
                        })
                    }, cancellationToken);
                    return;
                }

                var zone = channel.Zones.FirstOrDefault(z => z.Configuration.ZoneId == zoneId);
                if (zone == null)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetZonePoints_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = $"Zone {zoneId} not found"
                        })
                    }, cancellationToken);
                    return;
                }

                double start = zone.Configuration.StartPoint;
                double end = zone.Configuration.EndPoint;

                // Skip zone if it starts beyond ActualLength (fiber break)
                if (start >= channel.State.ActualLength)
                {
                    _logger.LogWarning(
                        "Zone {ZoneId} starts at {Start}m but ActualLength is {ActualLength}m - returning empty data",
                        zoneId, start, channel.State.ActualLength);

                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetZonePoints_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = true,
                            ChannelId = channelId,
                            ZoneId = zoneId,
                            Points = new List<object>(),
                            Warning = "Zone is beyond fiber break position"
                        })
                    }, cancellationToken);
                    return;
                }

                // Clamp zone end to ActualLength if zone spans fiber break
                double effectiveEnd = Math.Min(end, channel.State.ActualLength);

                if (effectiveEnd < end)
                {
                    _logger.LogWarning(
                        "Zone {ZoneId} EndPoint {End}m clamped to ActualLength {ActualLength}m for graph data",
                        zoneId, end, channel.State.ActualLength);
                }

                // Get latest DTS data
                var samples = channel.State.CurrentData;

                // ✨ EPSILON FIX – ensures 90.80000000001 is included
                const double EPS = 1e-6;

                var filtered = samples.Where(p => (p.Position + EPS) >= start && (p.Position - EPS) <= effectiveEnd).Select(p => new { p.Position, p.Temperature }).ToList();

                // No decimation for zone points — zones are small subsets
                // requested on-demand (user click), full resolution is fine.

                _logger.LogInformation("📤 Sending {Count} zone points for Zone {ZoneId}", filtered.Count,
                    zoneId);

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetZonePoints_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        ChannelId = channelId,
                        ZoneId = zoneId,
                        Points = filtered
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ GetZonePoints failed");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetZonePoints_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleGetChannelPointsCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                int channelId = message.Payload.GetProperty("ChannelId").GetInt32();

                _logger.LogInformation("📥 GetChannelPoints received → Channel {Ch}", channelId);

                var channels = _configurationManager.GetChannels();
                var channel = channels.FirstOrDefault(c => c.Configuration.ChannelId == channelId);

                if (channel == null)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetChannelPoints_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = $"Channel {channelId} not found"
                        })
                    }, cancellationToken);
                    return;
                }

                // 🚫 BLOCK DATA BEFORE START MEASUREMENT only if there is no existing or cached data
                if (!_isStreamingActive && (channel.State.CurrentData == null || channel.State.CurrentData.Length == 0))
                {
                    _logger.LogInformation(
                        "🚫 GetChannelPoints blocked – measurement not started and no cached data. Channel={ChannelId}",
                        channelId);

                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetChannelPoints_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = true,
                            ChannelId = channelId,
                            Points = new List<object>()   // EMPTY
                        })
                    }, cancellationToken);

                    return;
                }

                // Get all temperature data for the channel (up to ActualLength)
                var samples = channel.State.CurrentData;
                var filtered = samples
                    .Where(p => p.Position <= channel.State.ActualLength)
                    .Select(p => new { p.Position, p.Temperature })
                    .ToList();

                // Apply decimation
                var liveDataSvc = _serviceProvider.GetService(typeof(LiveDataStreamingService)) as LiveDataStreamingService;
                if (liveDataSvc != null)
                {
                    var decConfig = liveDataSvc.GetDecimationConfig();
                    double channelLengthKm = channel.Configuration.Length / 1000.0;
                    int factor = 1;
                    if (channelLengthKm >= decConfig.LargeThresholdKm)
                        factor = decConfig.LargeFactor;
                    else if (channelLengthKm >= decConfig.MediumThresholdKm)
                        factor = decConfig.MediumFactor;

                    if (factor > 1)
                    {
                        int originalCount = filtered.Count;
                        filtered = filtered.Where((_, i) => i % factor == 0).ToList();
                        _logger.LogDebug("Decimated channel points: {Original} -> {Decimated} (1:{Factor})",
                            originalCount, filtered.Count, factor);
                    }
                }

                _logger.LogInformation("📤 Sending {Count} channel points for Channel {ChannelId}", filtered.Count, channelId);

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetChannelPoints_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        ChannelId = channelId,
                        Points = filtered
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ GetChannelPoints failed");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetChannelPoints_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleStartMeasurementCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Received start_measurement request from client.");

            // BUGFIX: Add authentication check for critical command
            if (!IsAuthenticated(webSocket))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "start_measurement_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Status = "ERROR",
                        Error = "Authentication required"
                    })
                }, cancellationToken);
                return;
            }

            try
            {
                // Enable live streaming on both control and data stream servers
                _isStreamingActive = true;
                _dataStreamServer.EnableStreaming();
                _logger.LogInformation("Live streaming ENABLED (Control + Data Stream servers)");

                // 🔁 PUSH ACTIVE ALARMS SNAPSHOT
                var alarmManager = _serviceProvider.GetRequiredService<IAlarmManager>();
                var alarms = await alarmManager.GetActiveAlarmsAsync();

                var alarmList = alarms.Select(a => new
                {
                    ChannelId = a.ChannelId,
                    ZoneId = a.ZoneId,
                    Reason = a.Type.ToString(),
                    Value = a.TriggerValue,
                    Position = a.Position,
                    Timestamp = a.TriggeredAt.ToString("O")
                }).ToList();

                await BroadcastMessageAsync(new WebSocketMessage
                {
                    MessageType = "active_alarm_snapshot",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Alarms = alarmList
                    })
                });

                bool started = false;

                try
                {

                    started = true;
                }
                catch (Exception startEx)
                {
                    _logger.LogError(startEx, "Failed while starting measurement subsystem.");
                    started = false;
                }


                if (started)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "start_measurement_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Status = "OK"
                        })
                    }, cancellationToken).ConfigureAwait(false);

                    _logger.LogInformation("start_measurement_response (OK) sent.");


                    await BroadcastMessageAsync(new WebSocketMessage
                    {
                        MessageType = "measurement_started",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }).ConfigureAwait(false);
                }
                else
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "start_measurement_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Status = "ERROR",
                            Error = "FRMC failed to start measurement"
                        })
                    }, cancellationToken).ConfigureAwait(false);

                    _logger.LogWarning("start_measurement_response (ERROR) sent.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception handling start_measurement");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "start_measurement_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Status = "ERROR",
                        Error = ex.Message
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleStopMeasurementCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Received stop_measurement request from client.");

            // BUGFIX: Add authentication check for critical command
            if (!IsAuthenticated(webSocket))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "stop_measurement_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Status = "ERROR",
                        Error = "Authentication required"
                    })
                }, cancellationToken);
                return;
            }

            try
            {
                // Disable live streaming on both control and data stream servers
                _isStreamingActive = false;
                _dataStreamServer.DisableStreaming();
                _logger.LogInformation("Live streaming DISABLED (Control + Data Stream servers)");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "stop_measurement_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Status = "OK"
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception handling stop_measurement");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "stop_measurement_response",
                    Payload = JsonSerializer.SerializeToElement(new { Status = "ERROR", Error = ex.Message })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleReloadChannelsCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {

                var reloadMethod = _configurationManager.GetType().GetMethod("ReloadAsync") ??
                                   _configurationManager.GetType().GetMethod("Reload");

                if (reloadMethod != null)
                {
                    var ret = reloadMethod.Invoke(_configurationManager, null);
                    if (ret is Task rt) await rt.ConfigureAwait(false);
                }


                try
                {
                    var cm = _serviceProvider.GetService(typeof(ChannelsManager)) as ChannelsManager;
                    if (cm != null)
                    {
                        var initMethod = cm.GetType().GetMethod("InitializeChannelsAsync");
                        if (initMethod != null)
                        {
                            var task = initMethod.Invoke(cm, null) as Task;
                            if (task != null) await task.ConfigureAwait(false);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to reinitialize ChannelsManager after reload");
                }

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "ReloadChannels_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = true })
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ReloadChannels failed");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "ReloadChannels_response",
                    Payload = JsonSerializer.SerializeToElement(new { Success = false, Error = ex.Message })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleGetActiveAlarmCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                var alarmManager = (IAlarmManager)_serviceProvider.GetRequiredService(typeof(IAlarmManager));
                var alarms = await alarmManager.GetActiveAlarmsAsync();

                var alarmList = alarms.Select(a => new
                {
                    ChannelId = a.ChannelId,
                    ZoneId = a.ZoneId,
                    Reason = a.Type.ToString(),
                    Value = a.TriggerValue,
                    Position = a.Position,
                    Timestamp = a.TriggeredAt.ToString("O")
                }).ToList();

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "active_alarm_response",
                    Payload = JsonSerializer.SerializeToElement(alarmList)
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to get active alarms");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "active_alarm_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken);


            }
        }

        private async Task HandleGetSystemHealthCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            var healthManager = _serviceProvider.GetRequiredService<ISystemHealthManager>();
            var payload = healthManager.CurrentHealthPayload;

            var response = new WebSocketMessage("SystemHealthUpdate", payload);

            await BroadcastMessageAsync(response);
        }

        private async Task HandleGetSystemConfigCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            var config = await _configurationManager.GetSystemConfigurationAsync();
            var responsePayload = JsonSerializer.SerializeToElement(new
            {
                Success = true,
                Config = config
            });

            var response = new WebSocketMessage("GetSystemConfig_response", responsePayload);
            await SendResponse(webSocket, response, cancellationToken);
        }

        private async Task HandleSetSystemConfigCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            if (!IsAdmin(webSocket))
            {
                await SendSetSystemConfigErrorAsync(webSocket, "Admin privileges required", cancellationToken).ConfigureAwait(false);
                return;
            }

            if (!message.Payload.TryGetProperty("Config", out var configElement))
            {
                await SendSetSystemConfigErrorAsync(webSocket, "Config property missing", cancellationToken).ConfigureAwait(false);
                return;
            }

            var newConfig = JsonSerializer.Deserialize<SystemConfiguration>(
                configElement.GetRawText(),
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (newConfig == null)
            {
                await SendSetSystemConfigErrorAsync(webSocket, "Invalid configuration format", cancellationToken).ConfigureAwait(false);
                return;
            }

            if (!ValidateSystemConfigLimits(newConfig, out string? limitError))
            {
                await SendSetSystemConfigErrorAsync(webSocket, limitError!, cancellationToken).ConfigureAwait(false);
                return;
            }

            var currentConfig = await _configurationManager.GetSystemConfigurationAsync().ConfigureAwait(false);
            if (CheckForOrphanedRelayZones(newConfig, currentConfig, out string? orphanError))
            {
                await SendSetSystemConfigErrorAsync(webSocket, orphanError!, cancellationToken).ConfigureAwait(false);
                return;
            }

            TryGetSession(webSocket, out var session);
            string username = session?.Username ?? "Unknown";
            bool success = await _configurationManager.UpdateSystemConfigurationAsync(newConfig, username).ConfigureAwait(false);

            if (success)
            {
                await SendResponse(webSocket, new WebSocketMessage("SetSystemConfig_response",
                    JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Message = "System configuration updated successfully"
                    })), cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await SendSetSystemConfigErrorAsync(webSocket, "Failed to update system configuration in database", cancellationToken).ConfigureAwait(false);
            }
        }

        private static bool ValidateSystemConfigLimits(SystemConfiguration newConfig, out string? error)
        {
            error = null;
            if (newConfig.NumberOfRelays < 1 || newConfig.NumberOfRelays > 64)
            {
                error = "NumberOfRelays must be between 1 and 64";
                return false;
            }

            if (newConfig.MaxZonesPerChannel < 1 || newConfig.MaxZonesPerChannel > 1000)
            {
                error = "MaxZonesPerChannel must be between 1 and 1000";
                return false;
            }

            return true;
        }

        private bool CheckForOrphanedRelayZones(SystemConfiguration newConfig, SystemConfiguration? currentConfig, out string? orphanError)
        {
            orphanError = null;
            if (currentConfig != null && newConfig.NumberOfRelays < currentConfig.NumberOfRelays)
            {
                var allZones = _configurationManager.GetZoneConfigurations();
                var orphanedZones = allZones
                    .Where(z => z.AssignedRelayId.HasValue && z.AssignedRelayId.Value > newConfig.NumberOfRelays)
                    .ToList();

                if (orphanedZones.Any())
                {
                    var zoneList = string.Join(", ", orphanedZones.Select(z => $"Zone {z.ZoneId} (Relay {z.AssignedRelayId})"));
                    orphanError = $"Cannot reduce relays to {newConfig.NumberOfRelays}. The following zones are assigned to relays that would be removed: {zoneList}. Please reassign these zones first.";
                    return true;
                }
            }
            return false;
        }

        private async Task SendSetSystemConfigErrorAsync(WebSocket webSocket, string error, CancellationToken cancellationToken)
        {
            var errorResponse = new WebSocketMessage("SetSystemConfig_response",
                JsonSerializer.SerializeToElement(new
                {
                    Success = false,
                    Error = error
                }));
            await SendResponse(webSocket, errorResponse, cancellationToken).ConfigureAwait(false);
        }

        private async Task HandleResetAllConfigurationCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            // Authentication required - Admin only
            if (!IsAdmin(webSocket))
            {
                var errorResponse = new WebSocketMessage("ResetAllConfiguration_response",
                    JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Admin privileges required"
                    }));
                await SendResponse(webSocket, errorResponse, cancellationToken);
                return;
            }

            try
            {
                _logger.LogInformation("ResetAllConfiguration: Resetting all channels to default configuration");

                // Get username for audit trail
                TryGetSession(webSocket, out var session);
                string username = session?.Username ?? "Unknown";
                // Reset all 8 channels to default configuration
                for (int channelId = 1; channelId <= 8; channelId++)
                {
                    var defaultConfig = new ChannelConfiguration
                    {
                        ChannelId = channelId,
                        Name = $"Channel {channelId}",
                        ChannelLength = 0,
                        CorrectionLength = 0,
                        IsEnabled = false,
                        ScanPeriod = 6,
                        NumberOfZones = 0
                    };

                    // Update channel configuration
                    await _configurationManager.UpdateChannelConfigurationAsync(defaultConfig, username);

                    var channel = _configurationManager.GetChannels().FirstOrDefault(c => c.Configuration.ChannelId == channelId);

                    if (channel != null)
                    {
                        channel.State.IsActive = false;
                    }

                    // Clear all zones for this channel
                    await _configurationManager.ReplaceZoneConfigurationAsync(channelId, new List<ZoneConfiguration>(), username);
                    _logger.LogInformation("ResetAllConfiguration: Channel {ChannelId} reset to default", channelId);
                }

                // Save configurations to file
                await _configurationManager.SaveConfigurationsAsync();

                _logger.LogInformation("ResetAllConfiguration: All channels reset successfully");

                var responsePayload = JsonSerializer.SerializeToElement(new
                {
                    Success = true,
                    Message = "All configurations reset to default"
                });

                var response = new WebSocketMessage("ResetAllConfiguration_response", responsePayload);
                await SendResponse(webSocket, response, cancellationToken);

                // Broadcast configuration change to all clients
                var broadcastMsg = new WebSocketMessage("ConfigurationReset",
                    JsonSerializer.SerializeToElement(new { Timestamp = DateTime.UtcNow }));
                await BroadcastMessageAsync(broadcastMsg).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ResetAllConfiguration failed");
                var errorResponse = new WebSocketMessage("ResetAllConfiguration_response",
                    JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    }));
                await SendResponse(webSocket, errorResponse, cancellationToken);
            }
        }

        private async Task HandleGetRelayStatesCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            var relayManager = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Managers.RelayManager.IRelayControlManager>();
            var states = await relayManager.GetAllRelayStatesAsync();

            var responsePayload = JsonSerializer.SerializeToElement(new
            {
                Success = true,
                States = states
            });

            var response = new WebSocketMessage("GetRelayStates_response", responsePayload);
            await SendResponse(webSocket, response, cancellationToken);
        }

        private async Task HandleSetRelayStateCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            // Authentication required
            if (!IsAuthenticated(webSocket))
            {
                var errorResponse = new WebSocketMessage("SetRelayState_response",
                    JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Authentication required"
                    }));
                await SendResponse(webSocket, errorResponse, cancellationToken);
                return;
            }

            if (!message.Payload.TryGetProperty("RelayId", out var relayIdElement) ||
                !message.Payload.TryGetProperty("TurnOn", out var turnOnElement))
            {
                var errorResponse = new WebSocketMessage("SetRelayState_response",
                    JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "RelayId or TurnOn property missing"
                    }));
                await SendResponse(webSocket, errorResponse, cancellationToken);
                return;
            }

            int relayId = relayIdElement.GetInt32();
            bool turnOn = turnOnElement.GetBoolean();

            // FIX #1: Validate relay ID range (1-based, 1 to NumberOfRelays)
            var systemConfig = await _configurationManager.GetSystemConfigurationAsync();
            if (systemConfig == null || relayId < 1 || relayId > systemConfig.NumberOfRelays)
            {
                var errorResponse = new WebSocketMessage("SetRelayState_response",
                    JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = $"Invalid relay ID {relayId}. Must be between 1 and {systemConfig?.NumberOfRelays ?? 0}"
                    }));
                await SendResponse(webSocket, errorResponse, cancellationToken);
                return;
            }

            // BUGFIX: Use TryGetValue to avoid race condition / KeyNotFoundException
            if (!TryGetSession(webSocket, out var session) || session == null)
            {
                var errorResponse = new WebSocketMessage("SetRelayState_response",
                    JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Session not found"
                    }));
                await SendResponse(webSocket, errorResponse, cancellationToken);
                return;
            }

            var relayManager = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Managers.RelayManager.IRelayControlManager>();

            bool success = turnOn
                ? await relayManager.TurnRelayOnAsync(relayId, RelayControlSource.Manual, session.Username)
                : await relayManager.TurnRelayOffAsync(relayId, RelayControlSource.Manual, session.Username);

            var responsePayload = JsonSerializer.SerializeToElement(new
            {
                Success = success,
                Error = success ? null : $"Failed to set relay {relayId} state"
            });

            var response = new WebSocketMessage("SetRelayState_response", responsePayload);
            await SendResponse(webSocket, response, cancellationToken);
        }

        private async Task HandleGetHealthParametersCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            var healthManager = _serviceProvider.GetRequiredService<ISystemHealthManager>();

            // Call GetCurrentHealthAsync to refresh CPU/memory before returning
            await healthManager.GetCurrentHealthAsync();
            var health = healthManager.CurrentHealthPayload;

            var responsePayload = JsonSerializer.SerializeToElement(new
            {
                Success = true,
                Health = health
            });

            var response = new WebSocketMessage("GetHealthParameters_response", responsePayload);
            await SendResponse(webSocket, response, cancellationToken);
        }

        private async Task HandleGetActiveHealthFaultsCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            var healthManager = _serviceProvider.GetRequiredService<ISystemHealthManager>();
            var activeFaults = await healthManager.GetActiveHealthFaultsAsync();

            var responsePayload = JsonSerializer.SerializeToElement(new
            {
                Success = true,
                Faults = activeFaults
            });

            var response = new WebSocketMessage("GetActiveHealthFaults_response", responsePayload);
            await SendResponse(webSocket, response, cancellationToken);
        }

        private async Task HandleResetActiveAlarmCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Received reset_active_alarm request from client.");

            // BUGFIX: Add authentication check for critical command
            if (!IsAuthenticated(webSocket))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "reset_active_alarm_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Status = "ERROR",
                        Error = "Authentication required"
                    })
                }, cancellationToken);
                return;
            }

            try
            {
                var eventAggregator = _serviceProvider.GetRequiredService<IEventAggregator>();

                // Get username for audit trail
                TryGetSession(webSocket, out var alarmSession);
                string alarmUsername = alarmSession?.Username ?? "Unknown";

                await eventAggregator.PublishAsync(
                    new AlarmResetRequestedEvent(alarmUsername, DateTime.UtcNow)
                );

                _logger.LogInformation("Published AlarmResetRequestedEvent from DTSCM reset.");

                await SendResponse(
                    webSocket,
                    new WebSocketMessage
                    {
                        MessageType = "reset_active_alarm_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Status = "OK"
                        })
                    },
                    cancellationToken
                ).ConfigureAwait(false);

                _logger.LogInformation("reset_active_alarm_response (OK) sent.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception handling reset_active_alarm");

                await SendResponse(
                    webSocket,
                    new WebSocketMessage
                    {
                        MessageType = "reset_active_alarm_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Status = "ERROR",
                            Error = ex.Message
                        })
                    },
                    cancellationToken
                ).ConfigureAwait(false);
            }
        }

        private async Task HandleSubscribeChannelCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            // Client wants to receive live temperature data for a specific channel
            try
            {
                int channelId = message.Payload.GetProperty("channelId").GetInt32();

                var subscriptions = _channelSubscriptions.GetOrAdd(webSocket, _ => new System.Collections.Concurrent.ConcurrentBag<int>());

                // Add subscription if not already subscribed
                if (!subscriptions.Contains(channelId))
                {
                    subscriptions.Add(channelId);
                    _logger.LogInformation("Client subscribed to channel {ChannelId} live data", channelId);
                }

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "subscribe_channel_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        ChannelId = channelId,
                        Message = $"Subscribed to channel {channelId} live data"
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling subscribe_channel message");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "subscribe_channel_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Message = "Failed to subscribe to channel"
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleUnsubscribeChannelCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            // Client wants to stop receiving live temperature data for a specific channel
            try
            {
                int channelId = message.Payload.GetProperty("channelId").GetInt32();

                if (_channelSubscriptions.TryGetValue(webSocket, out var subscriptions))
                {
                    // Remove subscription by creating new bag without this channel
                    var newSubscriptions = new System.Collections.Concurrent.ConcurrentBag<int>(
                        subscriptions.Where(id => id != channelId)
                    );
                    _channelSubscriptions[webSocket] = newSubscriptions;

                    _logger.LogInformation("Client unsubscribed from channel {ChannelId} live data", channelId);
                }

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "unsubscribe_channel_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        ChannelId = channelId,
                        Message = $"Unsubscribed from channel {channelId} live data"
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling unsubscribe_channel message");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "unsubscribe_channel_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Message = "Failed to unsubscribe from channel"
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleSetDecimationConfigCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                var payload = message.Payload;
                double mediumThresholdKm = payload.GetProperty("MediumThresholdKm").GetDouble();
                double largeThresholdKm = payload.GetProperty("LargeThresholdKm").GetDouble();
                int mediumFactor = payload.GetProperty("MediumFactor").GetInt32();
                int largeFactor = payload.GetProperty("LargeFactor").GetInt32();

                // Get LiveDataStreamingService from DI container
                var liveDataService = _serviceProvider.GetService(typeof(LiveDataStreamingService)) as LiveDataStreamingService;
                if (liveDataService != null)
                {
                    await liveDataService.UpdateDecimationConfigAsync(mediumThresholdKm, largeThresholdKm, mediumFactor, largeFactor);

                    _logger.LogInformation(
                        "Decimation config updated and persisted: Medium >= {MediumKm}km (1:{MediumFactor}), Large >= {LargeKm}km (1:{LargeFactor})",
                        mediumThresholdKm, mediumFactor, largeThresholdKm, largeFactor);

                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "SetDecimationConfig_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = true,
                            MediumThresholdKm = mediumThresholdKm,
                            LargeThresholdKm = largeThresholdKm,
                            MediumFactor = mediumFactor,
                            LargeFactor = largeFactor
                        })
                    }, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "SetDecimationConfig_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = "LiveDataStreamingService not available"
                        })
                    }, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error setting decimation config");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetDecimationConfig_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleGetDecimationConfigCommandAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                var liveDataService = _serviceProvider.GetService(typeof(LiveDataStreamingService)) as LiveDataStreamingService;
                if (liveDataService != null)
                {
                    var config = liveDataService.GetDecimationConfig();

                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetDecimationConfig_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = true,
                            MediumThresholdKm = config.MediumThresholdKm,
                            LargeThresholdKm = config.LargeThresholdKm,
                            MediumFactor = config.MediumFactor,
                            LargeFactor = config.LargeFactor
                        })
                    }, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "GetDecimationConfig_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = "LiveDataStreamingService not available"
                        })
                    }, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting decimation config");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetDecimationConfig_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }


        private async Task HandleGetIpAsync(string role, string responseType, WebSocket webSocket, CancellationToken cancellationToken)
        {
            var ethConfig = _configurationManager.GetEthernetConfig();

            // 1️⃣ Find port by Role
            var port = ethConfig.EthernetPorts
                .FirstOrDefault(p =>
                    p.Role.Equals(role, StringComparison.OrdinalIgnoreCase));

            string? resolvedInterface = null;
            string? macAddress = null;

            if (port != null && !string.IsNullOrWhiteSpace(port.MacAddress) && port.MacAddress != "XX-XX-XX-XX-XX-XX")
            {
                macAddress = port.MacAddress;
                // 2️⃣ Resolve OS interface using MAC
                resolvedInterface = _networkService.ResolveInterfaceByMac(macAddress);
            }

            // 🔄 Fallback: If no port/MAC configured, try to find interface by Role Name directly (e.g. if the OS naming matches)
            // or just take the first available if none assigned yet
            if (string.IsNullOrEmpty(resolvedInterface))
            {
                var allInterfaces = _networkService.GetAvailableInterfaces();
                
                // Try to find by name match
                resolvedInterface = allInterfaces.FirstOrDefault(i => 
                    i.InterfaceName.Contains(role, StringComparison.OrdinalIgnoreCase))?.InterfaceName;

                // If still nothing, and it's a fresh setup, we might not have a mapping yet
                if (string.IsNullOrEmpty(resolvedInterface))
                {
                     await SendError($"No network interface mapped to role '{role}'. Please configure ports first.");
                     return;
                }
                
                macAddress = allInterfaces.FirstOrDefault(i => i.InterfaceName == resolvedInterface)?.MacAddress;
            }

            // 3️⃣ Get LIVE IP + Subnet + Gateway (INLINE LOGIC)
            var ni = NetworkInterface.GetAllNetworkInterfaces()
                .FirstOrDefault(n =>
                    n.Name.Equals(resolvedInterface, StringComparison.OrdinalIgnoreCase));

            string? liveIp = null;
            string? liveSubnet = null;
            string? liveGateway = null;

            if (ni != null)
            {
                var props = ni.GetIPProperties();

                var ipv4 = props.UnicastAddresses
                    .FirstOrDefault(a => a.Address.AddressFamily == AddressFamily.InterNetwork);

                liveIp = ipv4?.Address.ToString();
                liveSubnet = ipv4?.IPv4Mask?.ToString();

                var gateway = props.GatewayAddresses
                    .FirstOrDefault(g => g.Address.AddressFamily == AddressFamily.InterNetwork);

                liveGateway = gateway?.Address.ToString();
                
                if (string.IsNullOrEmpty(macAddress))
                {
                    macAddress = ni.GetPhysicalAddress().ToString();
                }
            }

            bool hasValidIp =
                !string.IsNullOrWhiteSpace(liveIp) &&
                liveIp != "0.0.0.0";

            // 4️⃣ Send response
            await SendResponse(webSocket, new WebSocketMessage
            {
                MessageType = responseType,
                Payload = JsonSerializer.SerializeToElement(new
                {
                    Success = true, // Set to true to allow UI to display what it found
                    Role = role,
                    InterfaceName = resolvedInterface,
                    CurrentIP = liveIp ?? "Not assigned",
                    SubnetMask = liveSubnet ?? port?.SubnetMask ?? "",
                    Gateway = liveGateway ?? port?.Gateway ?? "",
                    ConfiguredIP = port?.Ip_Address ?? "",
                    MacAddress = macAddress ?? "",
                    Message = hasValidIp
                        ? "IP retrieved successfully"
                        : "Interface found but has no valid IPv4 address",
                    EthernetPorts = ethConfig.EthernetPorts
                })
            }, cancellationToken);

            // 🔹 Local helper
            async Task SendError(string errorMessage)
            {
                if (webSocket.State == WebSocketState.Open)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = responseType,
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = errorMessage
                        })
                    }, cancellationToken);
                }
            }
        }

        private async Task HandleSetIpAsync(string role, string responseType, WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            if (!IsAdmin(webSocket))
            {
                await SendIpConfigErrorAsync(webSocket, responseType, "Admin authentication required", cancellationToken);
                return;
            }

            if (!ValidateIpParameters(message.Payload, out var newIp, out var newMask, out var newGateway, out var parsedIp, out var subnetMask, out var validationError))
            {
                await SendIpConfigErrorAsync(webSocket, responseType, validationError ?? "Invalid IP configuration", cancellationToken);
                return;
            }

            var ethConfig = _configurationManager.GetEthernetConfig();
            var existing = ethConfig.EthernetPorts.FirstOrDefault(p => p.Role.Equals(role, StringComparison.OrdinalIgnoreCase));
            if (existing == null)
            {
                await SendIpConfigErrorAsync(webSocket, responseType, $"No ethernet port configured for role '{role}'", cancellationToken);
                return;
            }

            string? resolvedInterface = _networkService.ResolveInterfaceByMac(existing.MacAddress);
            if (string.IsNullOrEmpty(resolvedInterface))
            {
                await SendIpConfigErrorAsync(webSocket, responseType, $"Cannot find network interface with MAC '{existing.MacAddress}'", cancellationToken);
                return;
            }

            if (!ValidateGatewaySubnet(parsedIp!, subnetMask!, newGateway, out var subnetError))
            {
                await SendIpConfigErrorAsync(webSocket, responseType, subnetError ?? "Subnet mismatch", cancellationToken);
                return;
            }

            if (await CheckIfIpInUseAsync(newIp!).ConfigureAwait(false))
            {
                await SendIpConfigErrorAsync(webSocket, responseType, $"IP {newIp} is already in use on the network.", cancellationToken);
                return;
            }

            var updated = new EthernetConfiguration
            {
                Role = existing.Role,
                InterfaceName = resolvedInterface,
                MacAddress = existing.MacAddress,
                Ip_Address = newIp,
                SubnetMask = newMask,
                Gateway = newGateway,
                DhcpEnabled = false
            };

            await ApplyIpConfigurationAsync(responseType, existing, updated, webSocket, cancellationToken).ConfigureAwait(false);
        }

        private static bool ValidateIpParameters(JsonElement payload, out string? newIp, out string? newMask, out string? newGateway, out IPAddress? parsedIp, out IPAddress? subnetMask, out string? errorMessage)
        {
            parsedIp = null;
            subnetMask = null;
            newIp = null;
            newMask = null;
            newGateway = null;
            errorMessage = null;

            if (!payload.TryGetProperty("NewIP", out var ipProp) ||
                !payload.TryGetProperty("SubnetMask", out var maskProp) ||
                !payload.TryGetProperty("Gateway", out var gatewayProp))
            {
                errorMessage = "Missing NewIP / SubnetMask / Gateway in payload";
                return false;
            }

            newIp = ipProp.GetString()?.Trim();
            newMask = maskProp.GetString()?.Trim();
            newGateway = gatewayProp.GetString()?.Trim();

            if (string.IsNullOrWhiteSpace(newIp))
            {
                errorMessage = "NewIP cannot be empty";
                return false;
            }

            if (!IPAddress.TryParse(newIp, out parsedIp) || parsedIp.AddressFamily != AddressFamily.InterNetwork)
            {
                errorMessage = "Invalid IPv4 address format.";
                return false;
            }

            if (!IPAddress.TryParse(newMask, out subnetMask))
            {
                errorMessage = "Invalid subnet mask configuration.";
                return false;
            }

            if (!string.IsNullOrWhiteSpace(newGateway) &&
                (!IPAddress.TryParse(newGateway, out var parsedGateway) || parsedGateway.AddressFamily != AddressFamily.InterNetwork))
            {
                errorMessage = "Invalid gateway";
                return false;
            }

            return true;
        }

        private static bool ValidateGatewaySubnet(IPAddress parsedIp, IPAddress subnetMask, string? newGateway, out string? errorMessage)
        {
            errorMessage = null;
            if (string.IsNullOrWhiteSpace(newGateway))
                return true;

            byte[] newIpBytes = parsedIp.GetAddressBytes();
            byte[] maskBytes = subnetMask.GetAddressBytes();
            byte[] gwBytes = IPAddress.Parse(newGateway).GetAddressBytes();

            for (int i = 0; i < 4; i++)
            {
                if ((newIpBytes[i] & maskBytes[i]) != (gwBytes[i] & maskBytes[i]))
                {
                    errorMessage = "Gateway must be in same subnet as IP";
                    return false;
                }
            }

            return true;
        }

        private static async Task<bool> CheckIfIpInUseAsync(string ip)
        {
            try
            {
                using var ping = new Ping();
                var reply = await ping.SendPingAsync(ip, 1000).ConfigureAwait(false);
                return reply.Status == IPStatus.Success;
            }
            catch
            {
                return false;
            }
        }

        private async Task ApplyIpConfigurationAsync(string responseType, EthernetConfiguration existing, EthernetConfiguration updated, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                _configurationManager.UpdateEthernetPort(updated);
                _logger.LogInformation("Initiating IP change for {Role} ({Interface}) → {IP}", updated.Role, updated.InterfaceName, updated.Ip_Address);

                if (webSocket.State == WebSocketState.Open)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = responseType,
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = true,
                            Role = updated.Role,
                            InterfaceName = updated.InterfaceName,
                            NewIP = updated.Ip_Address,
                            Message = "IP change initiated. Reconnect using new IP."
                        })
                    }, cancellationToken);
                }

                await Task.Delay(300, cancellationToken);

                _networkService.ApplyStaticIp(
                    updated.InterfaceName,
                    updated.Ip_Address,
                    updated.SubnetMask,
                    updated.Gateway,
                    "8.8.8.8");

                _logger.LogInformation("IP configuration successfully applied for {Role}", updated.Role);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to apply IP configuration for {Role} ({Interface})", updated.Role, updated.InterfaceName);
                _configurationManager.UpdateEthernetPort(existing);
                await SendIpConfigErrorAsync(webSocket, responseType, $"Failed to apply IP configuration: {ex.Message}", cancellationToken);
            }
        }

        private async Task SendIpConfigErrorAsync(WebSocket webSocket, string responseType, string errorMessage, CancellationToken cancellationToken)
        {
            if (webSocket.State == WebSocketState.Open)
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = responseType,
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = errorMessage
                    })
                }, cancellationToken);
            }
        }

        /// <summary>
        /// Returns all available ethernet interfaces for admin to configure NETRA1/NETRA2 roles
        /// </summary>
        private async Task HandleGetAvailableInterfacesAsync(WebSocket webSocket, CancellationToken cancellationToken)
        {
            var interfaces = _networkService.GetAvailableInterfaces();
            var ethConfig = _configurationManager.GetEthernetConfig();

            // Include current role assignments
            var netra1Config = ethConfig.EthernetPorts.FirstOrDefault(p => p.Role == ROLE_NETRA1);
            var netra2Config = ethConfig.EthernetPorts.FirstOrDefault(p => p.Role == ROLE_NETRA2);

            await SendResponse(webSocket, new WebSocketMessage
            {
                MessageType = "GetAvailableInterfaces_response",
                Payload = JsonSerializer.SerializeToElement(new
                {
                    Success = true,
                    Interfaces = interfaces,
                    CurrentConfig = new
                    {
                        Netra1Mac = netra1Config?.MacAddress ?? "",
                        Netra2Mac = netra2Config?.MacAddress ?? ""
                    }
                })
            }, cancellationToken);
        }

        /// <summary>
        /// Configures which MAC addresses are assigned to NETRA1 and NETRA2 roles
        /// </summary>
        private async Task HandleConfigureEthernetRolesAsync(
     WebSocketMessage message,
     WebSocket webSocket,
     CancellationToken cancellationToken)
        {
            if (!IsAdmin(webSocket))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "ConfigureEthernetRoles_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Admin authentication required"
                    })
                }, cancellationToken);
                return;
            }

            if (!message.Payload.TryGetProperty("Netra1Mac", out var netra1MacProp) ||
                !message.Payload.TryGetProperty("Netra2Mac", out var netra2MacProp))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "ConfigureEthernetRoles_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Missing Netra1Mac or Netra2Mac in payload"
                    })
                }, cancellationToken);
                return;
            }

            string netra1Mac = netra1MacProp.GetString() ?? "";
            string netra2Mac = netra2MacProp.GetString() ?? "";

            if (string.IsNullOrWhiteSpace(netra1Mac) || string.IsNullOrWhiteSpace(netra2Mac))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "ConfigureEthernetRoles_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Both Netra1Mac and Netra2Mac must be provided"
                    })
                }, cancellationToken);
                return;
            }

            if (netra1Mac.Equals(netra2Mac, StringComparison.OrdinalIgnoreCase))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "ConfigureEthernetRoles_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "NETRA1 and NETRA2 cannot use the same MAC address"
                    })
                }, cancellationToken);
                return;
            }

            // Verify MAC addresses exist on system
            var interfaces = _networkService.GetAvailableInterfaces();

            var netra1Interface = interfaces.FirstOrDefault(i =>
                i.MacAddress.Equals(netra1Mac, StringComparison.OrdinalIgnoreCase));

            var netra2Interface = interfaces.FirstOrDefault(i =>
                i.MacAddress.Equals(netra2Mac, StringComparison.OrdinalIgnoreCase));

            if (netra1Interface == null || netra2Interface == null)
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "ConfigureEthernetRoles_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Selected MAC address not found on this system"
                    })
                }, cancellationToken);
                return;
            }

            try
            {
                // 🔥 Get existing config
                var ethConfig = _configurationManager.GetEthernetConfig();

                // 🔥 Update NETRA1
                var netra1Port = ethConfig.EthernetPorts
                    .FirstOrDefault(p => p.Role.Equals(ROLE_NETRA1, StringComparison.OrdinalIgnoreCase));

                if (netra1Port != null)
                {
                    netra1Port.MacAddress = netra1Mac;
                    netra1Port.InterfaceName = netra1Interface.InterfaceName;
                    netra1Port.Ip_Address = netra1Interface.IpAddress;
                }

                // 🔥 Update NETRA2
                var netra2Port = ethConfig.EthernetPorts
                    .FirstOrDefault(p => p.Role.Equals(ROLE_NETRA2, StringComparison.OrdinalIgnoreCase));

                if (netra2Port != null)
                {
                    netra2Port.MacAddress = netra2Mac;
                    netra2Port.InterfaceName = netra2Interface.InterfaceName;
                    netra2Port.Ip_Address = netra2Interface.IpAddress;
                }

                // 🔥 IMPORTANT: SAVE TO FILE
                _configurationManager.SaveEthernetConfig();


                _logger.LogInformation(
                    "Ethernet roles configured and saved: NETRA1={Netra1Mac}, NETRA2={Netra2Mac}",
                    netra1Mac, netra2Mac);

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "ConfigureEthernetRoles_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Message = "Ethernet roles configured successfully",
                        Netra1 = new { netra1Mac, netra1Interface.InterfaceName },
                        Netra2 = new { netra2Mac, netra2Interface.InterfaceName }
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to configure ethernet roles");

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "ConfigureEthernetRoles_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = $"Failed to save configuration: {ex.Message}"
                    })
                }, cancellationToken);
            }
        }


        private async Task HandleLoginCommand(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                // Extract username and password from payload
                if (!message.Payload.TryGetProperty("username", out var usernameElement) ||
                    !message.Payload.TryGetProperty("password", out var passwordElement))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "login_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Missing username or password"
                        })
                    }, cancellationToken);
                    return;
                }

                string? username = usernameElement.GetString();
                string? password = passwordElement.GetString();

                if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "login_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Username and password cannot be empty"
                        })
                    }, cancellationToken);
                    return;
                }

                // Get authentication services from service provider
                var userStore = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Infrastructure.Security.UserStore>();
                var jwtTokenHandler = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Infrastructure.Security.JwtTokenHandler>();

                // Validate credentials
                bool isValid = await userStore.ValidateCredentialsAsync(username, password);
                if (!isValid)
                {
                    _logger.LogWarning("Failed login attempt for user: {Username}", username);
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "login_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Invalid username or password"
                        })
                    }, cancellationToken);
                    return;
                }

                // Get user role
                var role = await userStore.GetUserRoleAsync(username);
                if (role == null)
                {
                    _logger.LogError("User {Username} authenticated but role not found", username);
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "login_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Authentication error"
                        })
                    }, cancellationToken);
                    return;
                }

                // Generate JWT token
                string token = jwtTokenHandler.GenerateToken(username, role.Value.ToString());

                // Create session
                var session = new UserSession(Guid.NewGuid().ToString(), username, role.Value, DateTime.UtcNow);
                _activeSessions[webSocket] = session;

                _logger.LogInformation("✅ User {Username} logged in successfully with role {Role}. Session stored. Active sessions: {Count}",
                    username, role.Value, _activeSessions.Count);

                // Send success response with token
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "login_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Token = token,
                        Username = username,
                        Role = role.Value.ToString(),
                        SessionId = session.SessionId
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling login command");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "login_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Message = "Internal server error"
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleRefreshTokenCommand(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                if ((!TryGetSession(webSocket, out var session) || session == null) &&
                    message.Payload.TryGetProperty("token", out var tokenProp))
                {
                        var jwtTokenHandler = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Infrastructure.Security.JwtTokenHandler>();
                        var principal = jwtTokenHandler.ValidateToken(tokenProp.GetString() ?? "");
                        if (principal != null)
                        {
                            var username = principal.Identity?.Name ?? "Unknown";
                            var userStore = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Infrastructure.Security.UserStore>();
                            var role = await userStore.GetUserRoleAsync(username);
                            if (role != null)
                            {
                                session = new UserSession(Guid.NewGuid().ToString(), username, role.Value, DateTime.UtcNow);
                                _activeSessions[webSocket] = session;
                            }
                        }
                    }

                if (session == null)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "refresh_token_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Invalid or expired session. Please log in again."
                        })
                    }, cancellationToken);
                    return;
                }

                var jwtHandler = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Infrastructure.Security.JwtTokenHandler>();
                string newToken = jwtHandler.GenerateToken(session.Username, session.Role.ToString());
                _activeSessions[webSocket] = new UserSession(session.SessionId, session.Username, session.Role, DateTime.UtcNow);

                _logger.LogInformation("✅ [TOKEN RENEWAL] Successfully refreshed JWT token for user {Username} (Role: {Role})",
                    session.Username, session.Role);

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "refresh_token_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Token = newToken,
                        Username = session.Username,
                        Role = session.Role.ToString(),
                        SessionId = session.SessionId
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling refresh_token command");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "refresh_token_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Message = "Internal server error during token refresh"
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleAddUserCommand(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                // Check if user is admin
                if (!IsAdmin(webSocket))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "add_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Admin authentication required"
                        })
                    }, cancellationToken);
                    return;
                }

                // Extract username, password, and role from payload
                if (!message.Payload.TryGetProperty("username", out var usernameElement) ||
                    !message.Payload.TryGetProperty("password", out var passwordElement) ||
                    !message.Payload.TryGetProperty("role", out var roleElement))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "add_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Missing username, password, or role"
                        })
                    }, cancellationToken);
                    return;
                }

                string? username = usernameElement.GetString();
                string? password = passwordElement.GetString();
                string? roleStr = roleElement.GetString();

                if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password) || string.IsNullOrEmpty(roleStr))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "add_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Username, password, and role cannot be empty"
                        })
                    }, cancellationToken);
                    return;
                }

                // Parse role
                if (!Enum.TryParse<UserRole>(roleStr, true, out var role))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "add_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Invalid role. Must be 'Manufacturer', 'Admin' or 'User'"
                        })
                    }, cancellationToken);
                    return;
                }

                // Get UserStore
                var userStore = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Infrastructure.Security.UserStore>();

                // Check if user already exists
                if (userStore.UserExists(username))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "add_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = $"User '{username}' already exists"
                        })
                    }, cancellationToken);
                    return;
                }

                // Add user
                await userStore.AddUserAsync(username, password, role);

                // Get admin username for logging
                TryGetSession(webSocket, out var session);
                string adminUsername = session?.Username ?? "Unknown";

                _logger.LogInformation("Admin '{AdminUser}' created new user '{NewUser}' with role '{Role}'",
                    adminUsername, username, role);

                // Publish admin activity event
                await _eventAggregator.PublishAsync(
                    new FireRakshak.FRMC.Core.Events.AdminActivityEvent(
                        "AddUser",
                        adminUsername,
                        "WebSocket",
                        new Dictionary<string, object>
                        {
                            { "NewUsername", username },
                            { "Role", role.ToString() }
                        },
                        success: true));

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "add_user_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Message = $"User '{username}' created successfully with role '{role}'"
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling add_user command");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "add_user_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Message = "Internal server error"
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleDeleteUserCommand(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                // Check if user is admin
                if (!IsAdmin(webSocket))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "delete_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Admin authentication required"
                        })
                    }, cancellationToken);
                    return;
                }

                // Extract username from payload
                if (!message.Payload.TryGetProperty("username", out var usernameElement))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "delete_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Missing username"
                        })
                    }, cancellationToken);
                    return;
                }

                string? username = usernameElement.GetString();

                if (string.IsNullOrEmpty(username))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "delete_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Username cannot be empty"
                        })
                    }, cancellationToken);
                    return;
                }

                // Get UserStore
                var userStore = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Infrastructure.Security.UserStore>();

                // Prevent deleting yourself
                TryGetSession(webSocket, out var session);
                if (session != null && session.Username.Equals(username, StringComparison.OrdinalIgnoreCase))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "delete_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Cannot delete your own account"
                        })
                    }, cancellationToken);
                    return;
                }

                // Protect Manufacturer accounts
                var targetRole = await userStore.GetUserRoleAsync(username);
                if (targetRole == UserRole.Manufacturer && session?.Role != UserRole.Manufacturer)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "delete_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Access Denied. Only a Manufacturer can delete Manufacturer accounts"
                        })
                    }, cancellationToken);
                    return;
                }

                // Enforce: At least one Admin must always exist in the system
                if (targetRole == UserRole.Admin)
                {
                    var allUsers = await userStore.GetAllUsersAsync();
                    int adminCount = allUsers.Count(u => u.Role == UserRole.Admin);
                    if (adminCount <= 1)
                    {
                        await SendResponse(webSocket, new WebSocketMessage
                        {
                            MessageType = "delete_user_response",
                            Payload = JsonSerializer.SerializeToElement(new
                            {
                                Success = false,
                                Message = "Cannot delete the only Admin. At least one Admin must always exist in the system."
                            })
                        }, cancellationToken);
                        return;
                    }
                }

                // Delete user
                bool deleted = await userStore.DeleteUserAsync(username);

                if (!deleted)
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "delete_user_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = $"User '{username}' not found"
                        })
                    }, cancellationToken);
                    return;
                }

                string adminUsername = session?.Username ?? "Unknown";

                _logger.LogInformation("Admin '{AdminUser}' deleted user '{DeletedUser}'",
                    adminUsername, username);

                // Publish admin activity event
                await _eventAggregator.PublishAsync(
                    new FireRakshak.FRMC.Core.Events.AdminActivityEvent(
                        "DeleteUser",
                        adminUsername,
                        "WebSocket",
                        new Dictionary<string, object>
                        {
                            { "DeletedUsername", username }
                        },
                        success: true));

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "delete_user_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Message = $"User '{username}' deleted successfully"
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling delete_user command");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "delete_user_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Message = "Internal server error"
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleListUsersCommand(WebSocketMessage _, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                _logger.LogInformation("🔍 HandleListUsersCommand called");

                // Check if user is admin
                bool isAdmin = IsAdmin(webSocket);
                _logger.LogInformation("Admin check result: {IsAdmin}", isAdmin);

                if (!isAdmin)
                {
                    _logger.LogWarning("❌ User is not admin, denying access to list_users");
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "list_users_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Admin authentication required"
                        })
                    }, cancellationToken);
                    return;
                }

                // Get UserStore
                var userStore = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Infrastructure.Security.UserStore>();

                // Get all users
                var users = await userStore.GetAllUsersAsync();

                _logger.LogInformation("Admin requested user list, returning {Count} users", users.Count);

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "list_users_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Users = users
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling list_users command");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "list_users_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Message = "Internal server error"
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleGetAuditTrailCommand(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            _logger.LogDebug("HandleGetAuditTrailCommand processing audit trail request");

            try
            {
                _logger.LogInformation("🔍 HandleGetAuditTrailCommand called");

                // Check if user is admin
                _logger.LogInformation("Checking if user is admin...");
                bool isAdmin = IsAdmin(webSocket);
                _logger.LogInformation("IsAdmin result: {IsAdmin}", isAdmin);

                if (!isAdmin)
                {
                    _logger.LogWarning("❌ User is not admin, denying access to get_audit_trail");
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "get_audit_trail_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Message = "Admin authentication required"
                        })
                    }, cancellationToken);
                    return;
                }

                // Extract date range from payload (default to last 7 days)
                _logger.LogInformation("Extracting date range from payload...");
                DateTime to = DateTime.UtcNow;
                DateTime from = to.AddDays(-7);

                if (message.Payload.TryGetProperty("From", out var fromProp))
                {
                    from = fromProp.GetDateTime();
                    _logger.LogInformation("From date: {From}", from);
                }

                if (message.Payload.TryGetProperty("To", out var toProp))
                {
                    to = toProp.GetDateTime();
                    _logger.LogInformation("To date: {To}", to);
                }

                // Get AuditTrailService
                _logger.LogInformation("Getting AuditTrailService from service provider...");
                var auditTrailService = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Managers.SystemConfigurationManager.AuditTrailService>();
                _logger.LogInformation("AuditTrailService retrieved successfully");

                // Retrieve audit trail (limited to 1000 entries)
                _logger.LogInformation("Retrieving audit trail entries...");
                const int maxEntries = 1000;
                var auditEntries = await auditTrailService.GetAuditTrailAsync(from, to, maxEntries);

                _logger.LogInformation("Admin requested audit trail, returning {Count} entries from {From} to {To}",
                    auditEntries.Count, from, to);

                _logger.LogInformation("Sending audit trail response...");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "get_audit_trail_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        From = from,
                        To = to,
                        Count = auditEntries.Count,
                        MaxEntries = maxEntries,
                        IsLimited = auditEntries.Count >= maxEntries,
                        AuditTrail = auditEntries
                    })
                }, cancellationToken);
                _logger.LogInformation("✅ Audit trail response sent successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling get_audit_trail command: {Message}", ex.Message);
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "get_audit_trail_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Message = $"Internal server error: {ex.Message}"
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleGetEventHistoryCommand(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                _logger.LogInformation("🔍 HandleGetEventHistoryCommand called");

                // Allow both admin and regular users to view event history
                // (events are not sensitive configuration data)

                // Extract date range from payload (default to last 7 days)
                DateTime to = DateTime.UtcNow;
                DateTime from = to.AddDays(-7);

                if (message.Payload.TryGetProperty("From", out var fromProp))
                {
                    from = fromProp.GetDateTime();
                }

                if (message.Payload.TryGetProperty("To", out var toProp))
                {
                    to = toProp.GetDateTime();
                }

                // Get max entries limit (default 1000)
                int maxEntries = 1000;
                if (message.Payload.TryGetProperty("MaxEntries", out var maxEntriesProp))
                {
                    maxEntries = maxEntriesProp.GetInt32();
                }

                // Get EventHistoryService
                var eventHistoryService = _serviceProvider.GetRequiredService<FireRakshak.FRMC.Managers.SystemConfigurationManager.EventHistoryService>();

                // Retrieve event history with limit
                var events = await eventHistoryService.GetEventHistoryAsync(from, to, maxEntries);

                _logger.LogInformation("User requested event history, returning {Count} events from {From} to {To}",
                    events.Count, from, to);

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "get_event_history_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        From = from,
                        To = to,
                        Count = events.Count,
                        MaxEntries = maxEntries,
                        IsLimited = events.Count >= maxEntries,
                        Events = events
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling get_event_history command");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "get_event_history_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Message = "Internal server error"
                    })
                }, cancellationToken);
            }
        }

        private bool TryGetSession(WebSocket webSocket, out UserSession? session)
        {
            return _activeSessions.TryGetValue(webSocket, out session);
        }

        private bool IsAuthenticated(WebSocket webSocket)
        {
            return _activeSessions.ContainsKey(webSocket);
        }

        private bool IsAdmin(WebSocket webSocket)
        {
            if (_activeSessions.TryGetValue(webSocket, out var session))
            {
                _logger.LogInformation("Session found: Username={Username}, Role={Role}", session.Username, session.Role);
                return session.Role == UserRole.Admin || session.Role == UserRole.Manufacturer;
            }
            _logger.LogWarning("No session found for WebSocket. Active sessions count: {Count}", _activeSessions.Count);
            return false;
        }

        private async Task SendResponse(WebSocket webSocket, WebSocketMessage response, CancellationToken cancellationToken)
        {
            // ⛔ Do not send if socket is not ready
            if (webSocket == null ||
                webSocket.State != WebSocketState.Open)
            {
                _logger.LogWarning("⚠ Cannot send WS response: WebSocket not open (State={State})", webSocket?.State);
                return;
            }

            var json = JsonSerializer.Serialize(response);
            var bytes = Encoding.UTF8.GetBytes(json);

            _logger.LogInformation("📤 Sending WS response: MessageType={MessageType}, JSON={Json}",
                response.MessageType, json);

            // CRITICAL FIX: Use the SAME lock as broadcasts to prevent concurrent SendAsync calls
            // WebSocket contract: Only ONE SendAsync can be active at a time per socket
            // Using separate locks caused "InvalidOperationException: There is already one outstanding 'SendAsync'"
            var clientLock = _perClientBroadcastLocks.GetOrAdd(webSocket, _ => new SemaphoreSlim(1, 1));

            // PRIORITY DESIGN: Responses get 10-second timeout (high priority, must complete)
            // This is longer than broadcast timeout (2s), ensuring responses succeed even during heavy broadcasts
            if (!await clientLock.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken))
            {
                _logger.LogWarning("⚠ Timeout waiting for send lock for response {MessageType} - broadcast may be blocking", response.MessageType);
                return;
            }

            try
            {
                await webSocket.SendAsync(
                    new ArraySegment<byte>(bytes),
                    WebSocketMessageType.Text,
                    true,
                    cancellationToken
                );

                _logger.LogInformation("✅ WS response sent ({Length} bytes)", bytes.Length);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ WS Send failed!");
            }
            finally
            {
                clientLock.Release();        // 🔓 Release lock immediately for next operation
            }
        }

        // CRITICAL FIX: Single lock per WebSocket to prevent concurrent SendAsync calls
        // WebSocket.SendAsync contract: "at most one outstanding operation allowed at the same time"
        // Using separate locks for broadcast/response caused "InvalidOperationException: There is already one outstanding 'SendAsync'"
        //
        // PRIORITY DESIGN (Option 1 - Chunked Broadcasts):
        // - Responses: 10-second timeout (high priority, must succeed)
        // - Broadcasts: 2-second timeout (low priority, can skip and retry next cycle)
        // - This allows responses to "slip in" between broadcasts without blocking or crashing
        private readonly System.Collections.Concurrent.ConcurrentDictionary<WebSocket, SemaphoreSlim> _perClientBroadcastLocks = new();

        /// <summary>
        /// FM-05: Determines if a WebSocket message is a critical safety alarm or fault event.
        /// Under NFPA 72 & EN 54-2 life-safety standards, fire alarms must NEVER be dropped or delayed by routine telemetry.
        /// </summary>
        private static bool IsCriticalSafetyMessage(WebSocketMessage message)
        {
            if (string.IsNullOrEmpty(message?.MessageType)) return false;
            var type = message.MessageType;
            return type.StartsWith("alarm_", StringComparison.OrdinalIgnoreCase)
                || type.Equals("all_alarms_cleared", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("active_alarm", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("fault_", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("system_health_", StringComparison.OrdinalIgnoreCase)
                || type.Contains("alarm", StringComparison.OrdinalIgnoreCase);
        }

        // FM-05: Counter of active in-flight critical safety alarm dispatches to preempt routine telemetry
        private int _activeCriticalAlarmDispatches = 0;

        public virtual async Task BroadcastMessageAsync(WebSocketMessage message)
        {
            // Check if there are any connected clients
            if (_activeSessions.Count == 0)
            {
                _logger.LogDebug("No connected clients to broadcast to, skipping broadcast");
                return;
            }

            bool isCriticalAlarm = IsCriticalSafetyMessage(message);

            // FM-05 QoS: If this is routine telemetry, but critical fire alarms are currently dispatching,
            // immediately yield the socket so alarms get 100% bandwidth and zero latency.
            if (!isCriticalAlarm && Volatile.Read(ref _activeCriticalAlarmDispatches) > 0)
            {
                _logger.LogDebug("[FM-05 QoS] Telemetry broadcast '{MessageType}' skipped to prioritize active fire alarm dispatch", message.MessageType);
                return;
            }

            if (isCriticalAlarm)
            {
                Interlocked.Increment(ref _activeCriticalAlarmDispatches);
            }

            try
            {
                string messageString = JsonSerializer.Serialize(message);
                byte[] messageBytes = Encoding.UTF8.GetBytes(messageString);
                var segment = new ArraySegment<byte>(messageBytes);
                var disconnectedSockets = new ConcurrentBag<WebSocket>();

                // OPTIMIZATION: Parallel broadcast to all clients using Task.WhenAll
                // This reduces latency by 50-80% when multiple clients are connected
                var broadcastTasks = _activeSessions.Select(kvp =>
                    SendBroadcastToClientAsync(kvp.Key, segment, message.MessageType, isCriticalAlarm, disconnectedSockets));

                await Task.WhenAll(broadcastTasks);
                CleanupDisconnectedBroadcastSockets(disconnectedSockets);
            }
            finally
            {
                if (isCriticalAlarm)
                {
                    Interlocked.Decrement(ref _activeCriticalAlarmDispatches);
                }
            }
        }

        private async Task SendBroadcastToClientAsync(
            WebSocket clientSocket,
            ArraySegment<byte> segment,
            string messageType,
            bool isCriticalAlarm,
            ConcurrentBag<WebSocket> disconnectedSockets)
        {
            if (clientSocket.State != WebSocketState.Open)
            {
                disconnectedSockets.Add(clientSocket);
                return;
            }

            // CRITICAL FIX: Use shared lock to prevent concurrent SendAsync (same lock as SendResponse)
            var clientLock = _perClientBroadcastLocks.GetOrAdd(clientSocket, _ => new SemaphoreSlim(1, 1));

            // FM-05 QoS: Critical alarms wait up to 15s (guaranteed delivery, never dropped).
            // Routine broadcasts use 2s timeout and skip if busy.
            TimeSpan lockTimeout = isCriticalAlarm ? TimeSpan.FromSeconds(15) : TimeSpan.FromSeconds(2);

            if (!await clientLock.WaitAsync(lockTimeout, CancellationToken.None))
            {
                if (isCriticalAlarm)
                {
                    _logger.LogError("🚨 [FM-05 QoS] Lock wait timed out (15s) for critical alarm '{MessageType}' on client", messageType);
                }
                else
                {
                    _logger.LogDebug("Broadcast skipped for client (response likely in progress) - will retry next cycle");
                }
                return; // Skip this client, will broadcast on next temperature data cycle
            }

            try
            {
                await clientSocket.SendAsync(
                    segment,
                    WebSocketMessageType.Text,
                    endOfMessage: true,
                    cancellationToken: CancellationToken.None
                );

                if (isCriticalAlarm)
                {
                    _logger.LogInformation("🚨 [FM-05 QoS] Critical alarm dispatched to client: {MessageType}", messageType);
                }
            }
            catch (WebSocketException)
            {
                // Client disconnected - this is expected during shutdown, don't log as error
                disconnectedSockets.Add(clientSocket);
            }
            catch (OperationCanceledException)
            {
                // Client disconnected or operation cancelled - expected during shutdown
                disconnectedSockets.Add(clientSocket);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "WebSocket SendAsync failed for client, marking for removal");
                disconnectedSockets.Add(clientSocket);
            }
            finally
            {
                // Safely release the lock - may be disposed if client was removed by another thread
                try
                {
                    clientLock.Release();
                }
                catch (ObjectDisposedException)
                {
                    // Lock was already disposed by cleanup - this is expected during client disconnect
                }
            }
        }

        private void CleanupDisconnectedBroadcastSockets(ConcurrentBag<WebSocket> disconnectedSockets)
        {
            foreach (var socket in disconnectedSockets)
            {
                _activeSessions.TryRemove(socket, out _);
                _channelSubscriptions.TryRemove(socket, out _); // Clean up subscriptions
                // FIX: Remove and dispose send lock to prevent memory leak
                if (_perClientBroadcastLocks.TryRemove(socket, out var sendLockToDispose))
                {
                    sendLockToDispose.Dispose();
                }
                _logger.LogInformation("Removed disconnected client from active sessions. Remaining: {Count}", _activeSessions.Count);
            }

            // If no clients remain, disable streaming on both servers
            if (_activeSessions.Count == 0 && _isStreamingActive)
            {
                _isStreamingActive = false;
                _dataStreamServer.DisableStreaming();
                _logger.LogInformation("All clients disconnected - Live streaming DISABLED (Control + Data Stream servers)");
            }
        }

        /// <summary>
        /// Broadcasts pre-serialized binary data to all connected control WebSocket clients.
        /// Used for zone batch updates (binary protocol) — avoids JSON serialization entirely.
        /// Reuses the same per-client locking and cleanup pattern as BroadcastMessageAsync.
        /// </summary>
        public virtual async Task BroadcastBinaryMessageAsync(byte[] data)
        {
            if (_activeSessions.Count == 0 || Volatile.Read(ref _activeCriticalAlarmDispatches) > 0)
                return;

            var segment = new ArraySegment<byte>(data);
            var disconnectedSockets = new ConcurrentBag<WebSocket>();

            var broadcastTasks = _activeSessions.Select(kvp =>
                SendBinaryBroadcastToClientAsync(kvp.Key, segment, disconnectedSockets));

            await Task.WhenAll(broadcastTasks);
            CleanupDisconnectedBroadcastSockets(disconnectedSockets);
        }

        private async Task SendBinaryBroadcastToClientAsync(WebSocket clientSocket, ArraySegment<byte> segment, ConcurrentBag<WebSocket> disconnectedSockets)
        {
            if (clientSocket.State != WebSocketState.Open)
            {
                disconnectedSockets.Add(clientSocket);
                return;
            }

            var clientLock = _perClientBroadcastLocks.GetOrAdd(clientSocket, _ => new SemaphoreSlim(1, 1));
            if (!await clientLock.WaitAsync(TimeSpan.FromSeconds(2), CancellationToken.None))
                return;

            try
            {
                await clientSocket.SendAsync(
                    segment,
                    WebSocketMessageType.Binary,
                    endOfMessage: true,
                    cancellationToken: CancellationToken.None);
            }
            catch (WebSocketException)
            {
                disconnectedSockets.Add(clientSocket);
            }
            catch (OperationCanceledException)
            {
                disconnectedSockets.Add(clientSocket);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Binary SendAsync failed for client");
                disconnectedSockets.Add(clientSocket);
            }
            finally
            {
                try
                {
                    clientLock.Release();
                }
                catch (ObjectDisposedException ex)
                {
                    _logger.LogDebug(ex, "Send lock already disposed during binary broadcast cleanup");
                }
            }
        }

        /// <summary>
        /// Broadcasts a WebSocket message only to clients subscribed to the specified channel.
        /// Used for selective live temperature data streaming to reduce bandwidth.
        /// </summary>
        /// <param name="channelId">The channel ID to broadcast to</param>
        /// <param name="message">The WebSocket message to broadcast</param>
        public virtual async Task BroadcastToChannelSubscribersAsync(int channelId, WebSocketMessage message)
        {
            // ... (rest of method)
        }

        private async Task HandleSetPersistentIpAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            if (!IsAdmin(webSocket))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetPersistentIp_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Admin authentication required"
                    })
                }, cancellationToken);
                return;
            }

            try
            {
                var payload = message.Payload;
                string iface = payload.TryGetProperty("InterfaceName", out var ifaceProp) ? ifaceProp.GetString() ?? "" : "";
                string ip = payload.TryGetProperty("IpAddress", out var ipProp) ? ipProp.GetString() ?? "" : "";
                string mask = payload.TryGetProperty("SubnetMask", out var maskProp) ? maskProp.GetString() ?? "" : "";
                string gateway = payload.TryGetProperty("Gateway", out var gwProp) ? gwProp.GetString() ?? "" : "";

                if (string.IsNullOrWhiteSpace(iface) || string.IsNullOrWhiteSpace(ip) || string.IsNullOrWhiteSpace(mask))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "SetPersistentIp_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = "InterfaceName, IpAddress, and SubnetMask are required"
                        })
                    }, cancellationToken);
                    return;
                }

                _logger.LogInformation("Initiating persistent IP change for {Interface} → {IP}", iface, ip);

                // Run in background task to avoid blocking WebSocket
                _ = Task.Run(() =>
                {
                    try
                    {
                        _networkService.ApplyPersistentIp(iface, ip, mask, gateway);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to apply persistent IP for {Interface}", iface);
                    }
                }, cancellationToken);

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetPersistentIp_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Message = "Persistent IP configuration initiated. Connection may be interrupted."
                    })
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling SetPersistentIp");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "SetPersistentIp_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken);
            }
        }

        private async Task HandleGetDtsConnectionAsync(WebSocket webSocket, CancellationToken cancellationToken)
        {
            try
            {
                var dtsOptions = _serviceProvider.GetService(typeof(Microsoft.Extensions.Options.IOptions<DtsOptions>)) 
                    as Microsoft.Extensions.Options.IOptions<DtsOptions>;
                
                string ip = dtsOptions?.Value.IpAddress ?? "";
                int port = dtsOptions?.Value.Port ?? 3001;

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetDtsConnection_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        IpAddress = ip,
                        Port = port
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling GetDtsConnection");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "GetDtsConnection_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task HandleUpdateDtsConnectionAsync(WebSocketMessage message, WebSocket webSocket, CancellationToken cancellationToken)
        {
            if (!IsAdmin(webSocket))
            {
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "UpdateDtsConnection_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = "Admin authentication required"
                    })
                }, cancellationToken).ConfigureAwait(false);
                return;
            }

            try
            {
                var payload = message.Payload;
                string ip = payload.TryGetProperty("IpAddress", out var ipProp) ? ipProp.GetString() ?? "" : "";
                int port = payload.TryGetProperty("Port", out var portProp) ? portProp.GetInt32() : 3001;

                if (string.IsNullOrWhiteSpace(ip))
                {
                    await SendResponse(webSocket, new WebSocketMessage
                    {
                        MessageType = "UpdateDtsConnection_response",
                        Payload = JsonSerializer.SerializeToElement(new
                        {
                            Success = false,
                            Error = "IP Address is required"
                        })
                    }, cancellationToken).ConfigureAwait(false);
                    return;
                }

                _logger.LogInformation("Updating DTS connection settings: IP = {IP}, Port = {Port}", ip, port);

                // 1. Save to appsettings.json on disk
                string configPath = System.IO.Path.Combine(AppContext.BaseDirectory, "appsettings.json");
                if (System.IO.File.Exists(configPath))
                {
                    var json = await System.IO.File.ReadAllTextAsync(configPath, cancellationToken).ConfigureAwait(false);
                    var jObj = Newtonsoft.Json.Linq.JObject.Parse(json);
                    
                    if (jObj["Dts"] == null)
                    {
                        jObj["Dts"] = new Newtonsoft.Json.Linq.JObject();
                    }
                    
                    jObj["Dts"]["IpAddress"] = ip;
                    jObj["Dts"]["Port"] = port;
                    
                    await System.IO.File.WriteAllTextAsync(
                        configPath,
                        jObj.ToString(Newtonsoft.Json.Formatting.Indented),
                        cancellationToken).ConfigureAwait(false);
                }

                // 2. Update in-memory options and reconnect
                var dtsManager = _serviceProvider.GetService(typeof(FireRakshak.FRMC.Managers.DtsManager.IDtsManager)) as FireRakshak.FRMC.Managers.DtsManager.IDtsManager;
                if (dtsManager != null)
                {
                    dtsManager.UpdateDtsConnection(ip, port);
                }

                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "UpdateDtsConnection_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = true,
                        Message = "DTS Connection configuration updated successfully."
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling UpdateDtsConnection");
                await SendResponse(webSocket, new WebSocketMessage
                {
                    MessageType = "UpdateDtsConnection_response",
                    Payload = JsonSerializer.SerializeToElement(new
                    {
                        Success = false,
                        Error = ex.Message
                    })
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        #region FM-08 Dead Socket Reaper

        /// <summary>
        /// FM-08: Background Dead Socket Reaper.
        /// Runs every 15 seconds to verify connection liveness of all connected clients.
        /// If a client is disconnected or fails to respond within 5 seconds of connection loss,
        /// the socket is forcefully closed, memory is freed, and handles are recycled.
        /// </summary>
        private async Task RunDeadSocketReaperAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("FM-08: Dead Socket Reaper background task running.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(15), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                await ExecuteReaperCycleAsync(cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("FM-08: Dead Socket Reaper background task stopped.");
        }

        private async Task ExecuteReaperCycleAsync(CancellationToken cancellationToken)
        {
            try
            {
                if (_connectedSockets.IsEmpty && _activeSessions.IsEmpty)
                {
                    return;
                }

                var socketsToCheck = _connectedSockets.Keys
                    .Union(_activeSessions.Keys)
                    .Distinct()
                    .ToArray();

                var deadSockets = await FindDeadSocketsAsync(socketsToCheck, cancellationToken).ConfigureAwait(false);

                foreach (var deadSocket in deadSockets)
                {
                    ReapDeadSocket(deadSocket);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FM-08: Error in Dead Socket Reaper cycle");
            }
        }

        private async Task<List<WebSocket>> FindDeadSocketsAsync(WebSocket[] socketsToCheck, CancellationToken cancellationToken)
        {
            var deadSockets = new List<WebSocket>();
            foreach (var socket in socketsToCheck)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                if (socket.State != WebSocketState.Open)
                {
                    _logger.LogWarning("FM-08: Dead Socket Reaper detected non-Open socket (State={State}). Marking for cleanup.", socket.State);
                    deadSockets.Add(socket);
                    continue;
                }

                bool isAlive = await ProbeSocketLivenessAsync(socket, cancellationToken).ConfigureAwait(false);
                if (!isAlive)
                {
                    _logger.LogWarning("FM-08: Dead Socket Reaper detected unresponsive client (no response within 5s). Marking for cleanup.");
                    deadSockets.Add(socket);
                }
            }
            return deadSockets;
        }

        private async Task<bool> ProbeSocketLivenessAsync(WebSocket socket, CancellationToken cancellationToken)
        {
            using var probeCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, probeCts.Token);

            var clientLock = _perClientBroadcastLocks.GetOrAdd(socket, _ => new SemaphoreSlim(1, 1));
            bool lockAcquired = false;

            try
            {
                lockAcquired = await clientLock.WaitAsync(TimeSpan.FromSeconds(5), linkedCts.Token).ConfigureAwait(false);
                if (!lockAcquired)
                {
                    _logger.LogWarning("FM-08: Probe lock wait timed out after 5s - client send channel blocked");
                    return false;
                }

                if (socket.State != WebSocketState.Open)
                    return false;

                var pingPayload = JsonSerializer.SerializeToElement(new { timestamp = DateTime.UtcNow });
                var pingMsg = new WebSocketMessage("ping", pingPayload);
                var json = JsonSerializer.Serialize(pingMsg);
                var bytes = Encoding.UTF8.GetBytes(json);

                await socket.SendAsync(
                    new ArraySegment<byte>(bytes),
                    WebSocketMessageType.Text,
                    true,
                    linkedCts.Token
                ).ConfigureAwait(false);

                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (WebSocketException ex)
            {
                _logger.LogWarning("FM-08: Probe WebSocketException: {Message}", ex.Message);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "FM-08: Unexpected error during liveness probe");
                return false;
            }
            finally
            {
                if (lockAcquired)
                {
                    try
                    {
                        clientLock.Release();
                    }
                    catch (ObjectDisposedException ex)
                    {
                        _logger.LogDebug(ex, "FM-08: Send lock already disposed during liveness probe cleanup");
                    }
                }
            }
        }

        private void ReapDeadSocket(WebSocket deadSocket)
        {
            try
            {
                _logger.LogInformation("FM-08: Reaping dead socket connection...");
                AbortDeadSocket(deadSocket);
                RemoveDeadSocketFromRegistries(deadSocket);
                DisposeDeadSocket(deadSocket);
                CheckDisableStreamingIfAllDisconnected();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FM-08: Error while reaping dead socket");
            }
        }

        private void AbortDeadSocket(WebSocket deadSocket)
        {
            try
            {
                deadSocket.Abort();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "FM-08: Error aborting dead socket");
            }
        }

        private void RemoveDeadSocketFromRegistries(WebSocket deadSocket)
        {
            _connectedSockets.TryRemove(deadSocket, out _);

            _activeSessions.TryRemove(deadSocket, out var removedSession);
            if (removedSession != null)
            {
                _logger.LogInformation("FM-08: Reaped session {SessionId} for user {Username}",
                    removedSession.SessionId, removedSession.Username);
            }

            _channelSubscriptions.TryRemove(deadSocket, out _);

            if (_perClientBroadcastLocks.TryRemove(deadSocket, out var sendLockToDispose))
            {
                try
                {
                    sendLockToDispose.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "FM-08: Error disposing send lock during dead socket reap");
                }
            }

            if (_currentWebSocket == deadSocket) _currentWebSocket = null;
            if (_connectedClientSocket == deadSocket) _connectedClientSocket = null;
        }

        private void DisposeDeadSocket(WebSocket deadSocket)
        {
            try
            {
                deadSocket.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "FM-08: Error disposing dead socket");
            }
        }

        private void CheckDisableStreamingIfAllDisconnected()
        {
            if (_activeSessions.Count == 0 && _isStreamingActive)
            {
                _isStreamingActive = false;
                _dataStreamServer.DisableStreaming();
                _logger.LogInformation("FM-08: All clients disconnected - Live streaming DISABLED (Control + Data Stream servers)");
            }
        }

        #endregion
    }
}


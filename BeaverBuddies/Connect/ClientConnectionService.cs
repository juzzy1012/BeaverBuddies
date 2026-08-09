using BeaverBuddies.IO;
using BeaverBuddies.Steam;
using BeaverBuddies.Util;
using Steamworks;
using System;
using System.IO;
using System.IO.Compression;
using Timberborn.CoreUI;
using Timberborn.GameSaveRepositorySystem;
using Timberborn.GameSceneLoading;
using Timberborn.Localization;
using Timberborn.SingletonSystem;
using Timberborn.WebNavigation;
using TimberNet;
using System.Linq;
using Timberborn.SettlementNameSystem;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace BeaverBuddies.Connect
{
    public class ClientConnectionService : IUpdatableSingleton
    {
        private const int InitialReconnectDelayMilliseconds = 250;
        private const int MaximumReconnectDelayMilliseconds = 2000;
        private static readonly TimeSpan ReconnectTimeout = TimeSpan.FromMinutes(2);
        private static readonly object ReconnectLock = new object();
        private static readonly ConcurrentQueue<PendingConnectionError> PendingErrors =
            new ConcurrentQueue<PendingConnectionError>();

        // These are static because Timberborn replaces scene singletons while a
        // save is loading. The reconnect target must survive that replacement.
        private static Func<ISocketStream> reconnectSocketFactory;
        private static bool reconnectActive;
        private static bool reconnectAttemptInProgress;
        private static bool reconnectDisconnectCurrent;
        private static long reconnectDeadlineTimestamp;
        private static long reconnectNextAttemptTimestamp;
        private static int reconnectAttempts;
        private static long nextConnectionGeneration;
        private static long activeConnectionGeneration;

        private sealed class PendingConnectionError
        {
            public long Generation;
            public string ReasonKey;
            public string Details;
        }

        private GameSceneLoader _gameSceneLoader;
        private GameSaveRepository _gameSaveRepository;
        private DialogBoxShower _dialogBoxShower;
        private UrlOpener _urlOpener;
        private ClientEventIO client;
        private Settings _settings;

        public ClientConnectionService(
            GameSceneLoader gameSceneLoader,
            GameSaveRepository gameSaveRepository,
            DialogBoxShower dialogBoxShower,
            UrlOpener urlOpener,
            Settings settings
        )
        {
            _gameSceneLoader = gameSceneLoader;
            _gameSaveRepository = gameSaveRepository;
            _dialogBoxShower = dialogBoxShower;
            _urlOpener = urlOpener;
            _settings = settings;
        }

        public bool TryToConnect(CSteamID friendID)
        {
            Func<ISocketStream> socketFactory = () => new SteamSocket(friendID);
            PrepareManualConnection(socketFactory);
            return TryToConnect(socketFactory(), false);
        }

        public bool TryToConnect(string address)
        {
            int port = _settings.DefaultPort.Value;
            Plugin.Log("Connecting to address: " + address);
            // Parse address and port
            if (TryParseHostAndPort(address, out string parsedAddress, out int? parsedPort))
            {
                address = parsedAddress;
            }
            else
            {
                ShowError("BeaverBuddies.JoinCoopGame.Error.InvalidFormat");
                return false;
            }

            // Set port if provided
            if (parsedPort.HasValue)
            {
                port = parsedPort.Value;
            }
            Plugin.Log($"Parsed address: {address}, port: {port}");


            string finalAddress = address;
            int finalPort = port;
            Func<ISocketStream> socketFactory = () => new TCPClientWrapper(finalAddress, finalPort);
            PrepareManualConnection(socketFactory);
            return TryToConnect(socketFactory(), false);
        }

        private static void PrepareManualConnection(Func<ISocketStream> socketFactory)
        {
            lock (ReconnectLock)
            {
                activeConnectionGeneration = ++nextConnectionGeneration;
                reconnectSocketFactory = socketFactory;
                reconnectActive = false;
                reconnectAttemptInProgress = false;
                reconnectDisconnectCurrent = false;
                reconnectAttempts = 0;
            }
        }

        private static long TimestampAfter(TimeSpan delay)
        {
            return Stopwatch.GetTimestamp() +
                (long)(delay.TotalSeconds * Stopwatch.Frequency);
        }

        private static void ScheduleReconnectLocked(bool failedAttempt)
        {
            if (failedAttempt)
            {
                reconnectAttempts = Math.Min(reconnectAttempts + 1, 16);
            }

            int exponent = Math.Min(reconnectAttempts, 3);
            int delayMilliseconds = Math.Min(
                MaximumReconnectDelayMilliseconds,
                InitialReconnectDelayMilliseconds * (1 << exponent));
            reconnectNextAttemptTimestamp = TimestampAfter(
                TimeSpan.FromMilliseconds(delayMilliseconds));
        }

        private static void RequestSessionReconnect(long generation, bool failedAttempt)
        {
            lock (ReconnectLock)
            {
                if (generation != activeConnectionGeneration)
                {
                    return;
                }
                if (reconnectSocketFactory == null)
                {
                    Plugin.LogError("A session reload was requested without a reconnect target");
                    return;
                }

                if (!reconnectActive)
                {
                    reconnectDeadlineTimestamp = TimestampAfter(ReconnectTimeout);
                    reconnectAttempts = 0;
                    Plugin.Log("Session is reloading; reconnecting automatically");
                }
                reconnectActive = true;
                reconnectDisconnectCurrent = true;
                ScheduleReconnectLocked(failedAttempt);
            }
        }

        private static bool FinishSessionReconnect(long generation)
        {
            lock (ReconnectLock)
            {
                if (generation != activeConnectionGeneration)
                {
                    return false;
                }
                reconnectActive = false;
                reconnectAttemptInProgress = false;
                reconnectDisconnectCurrent = false;
                reconnectAttempts = 0;
                return true;
            }
        }

        private static void ExtendReconnectForMap(long generation, int mapLength)
        {
            // Steam's legacy reliable transport is deliberately throttled. Once a
            // valid map header arrives, base the deadline on the announced size so
            // a healthy large transfer is not mistaken for a failed reconnect.
            const double conservativeBytesPerSecond = 128 * 1024;
            double expectedSeconds = mapLength / conservativeBytesPerSecond;
            TimeSpan transferBudget = TimeSpan.FromSeconds(
                Math.Max(TimeSpan.FromMinutes(5).TotalSeconds,
                    expectedSeconds * 2 + 30));

            lock (ReconnectLock)
            {
                if (!reconnectActive || generation != activeConnectionGeneration)
                {
                    return;
                }

                long extendedDeadline = TimestampAfter(transferBudget);
                if (extendedDeadline > reconnectDeadlineTimestamp)
                {
                    reconnectDeadlineTimestamp = extendedDeadline;
                }
            }
        }

        private bool TryToConnect(ISocketStream socket, bool automatic)
        {
            Plugin.Log("Connecting client");
            long generation;
            lock (ReconnectLock)
            {
                generation = ++nextConnectionGeneration;
                activeConnectionGeneration = generation;
            }

            ClientEventIO newClient = ClientEventIO.Create(socket,
                mapBytes => LoadMap(mapBytes, generation), error =>
            {
                if (automatic || error == TimberNetBase.SESSION_RESTART_REQUIRED)
                {
                    RequestSessionReconnect(generation, automatic);
                }
                else
                {
                    PendingErrors.Enqueue(new PendingConnectionError
                    {
                        Generation = generation,
                        ReasonKey = "BeaverBuddies.JoinCoopGame.Error.CouldNotConnect",
                        Details = error,
                    });
                }
            }, () => RequestSessionReconnect(generation, false),
                reason => RequestSessionReconnect(generation, true),
                mapLength => ExtendReconnectForMap(generation, mapLength));

            client = newClient;
            
            if (client == null)
            {
                Plugin.Log("Client creation failed.");
                return false;
            }

            EventIO.Set(client);
            return true;
        }

        public void ConnectOrShowFailureMessage()
        {
            ConnectOrShowFailureMessage(_settings.ClientConnectionAddress.Value);
        }

        public void ConnectOrShowFailureMessage(string address)
        {
            TryToConnect(address);
        }

        public void ShowConnectionMessage(bool success)
        {
            if (success)
            {
                _dialogBoxShower.Create()
                    .SetLocalizedMessage("BeaverBuddies.JoinCoopGame.Success")
                    .Show();
            }
            else
            {
                ShowError("BeaverBuddies.JoinCoopGame.ConnectionFailedMessage");
            }
        }

        private void ShowError(string reasonKey, string details = null)
        {
            string messageKey;
            if (reasonKey != null)
            {
                messageKey = "BeaverBuddies.JoinCoopGame.ConnectionFailedMessageWithError";
            }
            else
            {
                messageKey = "BeaverBuddies.JoinCoopGame.ConnectionFailedMessage";
            }

            ILoc _loc = _dialogBoxShower._loc;
            string reasonMessage = null;
            if (reasonKey != null)
            {
                reasonMessage = _loc.T(reasonKey);
            }

            if (details != null)
            {
                if (reasonMessage != null)
                {
                    reasonMessage += "\n";
                }
                else
                {
                    reasonMessage = "";
                }
                reasonMessage += "\"" + details + "\"";
            }

            var action = () =>
            {
                _urlOpener.OpenUrl(LinkHelper.TroubleshootingUrl);
            };

            string message = _loc.T(messageKey, reasonMessage);
            _dialogBoxShower.Create()
                .SetMessage(message)
                .SetConfirmButton(action)
                .SetDefaultCancelButton()
                .Show();
        }

        private static bool IsValidMap(byte[] mapBytes)
        {
            // Timberborn saves are ZIP archives. Reject connection/control data
            // before handing it to the asynchronous game loader, where a bad
            // archive would otherwise crash with an EOCD exception.
            if (mapBytes == null || mapBytes.Length < 4 ||
                mapBytes[0] != 0x50 || mapBytes[1] != 0x4B ||
                mapBytes[2] != 0x03 || mapBytes[3] != 0x04)
            {
                return false;
            }

            try
            {
                using (var stream = new MemoryStream(mapBytes, writable: false))
                using (var archive = new ZipArchive(stream, ZipArchiveMode.Read))
                {
                    // Opening the archive validates its central directory, which
                    // catches truncated payloads that still have the PK prefix.
                    return archive.Entries.Count > 0;
                }
            }
            catch (InvalidDataException)
            {
                return false;
            }
        }

        private void LoadMap(byte[] mapBytes, long generation)
        {
            if (!FinishSessionReconnect(generation))
            {
                Plugin.Log("Ignoring a map from a superseded connection attempt");
                return;
            }

            if (!IsValidMap(mapBytes))
            {
                Plugin.LogError($"Received invalid map data ({mapBytes?.Length ?? 0} bytes); " +
                    "the host likely disconnected. Aborting load instead of crashing.");
                ShowError(null);
                EventIO.Reset();
                client = null;
                return;
            }

            // Clean up our current co-op state before loading,
            // so we don't, for example, end up ticking the client before
            // it's actually loaded.
            SingletonManager.Reset();

            try
            {
                Plugin.Log("Loading map");
                string saveName = TimberNetBase.GetHashCode(mapBytes).ToString("X8");
                SaveReference saveRef = new SaveReference("Online Games",
                    new SettlementReference(saveName, _gameSaveRepository.DefaultSaveDirectory));
                using (Stream stream = _gameSaveRepository.CreateSaveSkippingNameValidation(saveRef))
                {
                    stream.Write(mapBytes);
                }

                // Set the RNG seed before loading the map. The server does the same.
                DeterminismService.InitGameStartState(mapBytes);
                _gameSceneLoader.StartSaveGame(saveRef);
            }
            catch (Exception exception)
            {
                Plugin.LogError($"Could not store or load the multiplayer save: {exception}");
                ShowError(null);
                EventIO.Reset();
                client = null;
            }
        }

        public void UpdateSingleton()
        {
            while (PendingErrors.TryDequeue(out PendingConnectionError error))
            {
                bool isCurrent;
                lock (ReconnectLock)
                {
                    isCurrent = error.Generation == activeConnectionGeneration;
                }
                if (!isCurrent)
                {
                    continue;
                }
                EventIO.Reset();
                client = null;
                ShowError(error.ReasonKey, error.Details);
            }

            // This instance owns a connection while joining from the menu. Once
            // the game scene loads, ReplayService updates the shared EventIO.
            client?.Update();

            Func<ISocketStream> socketFactory = null;
            bool disconnectCurrent = false;
            bool timedOut = false;
            lock (ReconnectLock)
            {
                if (!reconnectActive || reconnectAttemptInProgress)
                {
                    return;
                }

                long now = Stopwatch.GetTimestamp();
                if (now >= reconnectDeadlineTimestamp)
                {
                    reconnectActive = false;
                    reconnectDisconnectCurrent = false;
                    timedOut = true;
                }
                else
                {
                    disconnectCurrent = reconnectDisconnectCurrent;
                    reconnectDisconnectCurrent = false;

                    if (now >= reconnectNextAttemptTimestamp && client == null)
                    {
                        socketFactory = reconnectSocketFactory;
                        reconnectAttemptInProgress = true;
                    }
                }
            }

            if (disconnectCurrent)
            {
                EventIO.Reset();
                client = null;
            }

            if (timedOut)
            {
                EventIO.Reset();
                client = null;
                ShowError("BeaverBuddies.JoinCoopGame.Error.CouldNotConnect",
                    "The host did not finish reloading before the connection deadline.");
                return;
            }

            if (socketFactory == null)
            {
                return;
            }

            bool connected = false;
            try
            {
                connected = TryToConnect(socketFactory(), true);
            }
            catch (Exception exception)
            {
                Plugin.LogError($"Automatic reconnect failed: {exception}");
            }
            finally
            {
                lock (ReconnectLock)
                {
                    reconnectAttemptInProgress = false;
                    if (!connected && reconnectActive)
                    {
                        ScheduleReconnectLocked(true);
                    }
                }
            }
        }

        /// <summary>
        /// Tries to parse a host:port or [IPv6]:port string.
        /// Supports IPv4, IPv6, and hostnames.
        /// Returns true if parsing succeeded.
        /// </summary>
        public static bool TryParseHostAndPort(
            string input,
            out string host,
            out int? port)
        {
            host = null;
            port = null;

            if (string.IsNullOrWhiteSpace(input))
                return false;

            // Uri requires a scheme, so we prepend a dummy one
            var uriString = input.Contains("://") ? input : "tcp://" + input;

            if (!Uri.TryCreate(uriString, UriKind.Absolute, out var uri))
            {
                if (!input.StartsWith("[") && input.Count(c => c == ':') >= 2)
                {
                    Plugin.Log("Attempting to wrap likely IPv6 address");
                    return TryParseHostAndPort($"[{input}]", out host, out port);
                }
                return false;
            }

            // Hostname or IP string (IPv6 brackets stripped)
            host = uri.Host;

            // Port: Uri.Port returns -1 if missing
            if (uri.Port != -1)
                port = uri.Port;

            return true;
        }

    }
}

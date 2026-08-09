using BeaverBuddies.IO;
using BeaverBuddies.Steam;
using BeaverBuddies.Util;
using Steamworks;
using System;
using System.IO;
using System.Net.Sockets;
using System.Net;
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

namespace BeaverBuddies.Connect
{
    public class ClientConnectionService : IUpdatableSingleton
    {
        private const int ReconnectRetryDelayFrames = 30;
        private static readonly TimeSpan ReconnectTimeout = TimeSpan.FromMinutes(2);
        private static readonly object ReconnectLock = new object();
        private static readonly ConcurrentQueue<Tuple<string, string>> PendingErrors =
            new ConcurrentQueue<Tuple<string, string>>();

        // These are static because Timberborn replaces scene singletons while a
        // save is loading. The reconnect target must survive that replacement.
        private static Func<ISocketStream> reconnectSocketFactory;
        private static bool reconnectActive;
        private static bool reconnectAttemptInProgress;
        private static bool reconnectDisconnectCurrent;
        private static DateTime reconnectDeadlineUtc;
        private static int reconnectDelayFrames;

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
            Plugin.Log("Try to resolve address: " + address);
            // Parse address and port
            if (TryParseHostAndPort(address, out string parsedAddress, out int? parsedPort))
            {
                address = parsedAddress;
                Plugin.Log($"Parsed address: {address}, port: {port}");
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


            // If it's not an IP address, resolve the hostname
            if (!IPAddress.TryParse(address, out _))
            {

                // Resolve the address if it's a hostname
                if (ResolveHostnameIfNecessary(parsedAddress, out string resolvedAddress))
                {
                    address = resolvedAddress;
                }
                else
                {
                    ShowError("BeaverBuddies.JoinCoopGame.Error.InvalidAddress");
                    return false;
                }
            }

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
                reconnectSocketFactory = socketFactory;
                reconnectActive = false;
                reconnectAttemptInProgress = false;
                reconnectDisconnectCurrent = false;
            }
        }

        private static void RequestSessionReconnect()
        {
            lock (ReconnectLock)
            {
                if (reconnectSocketFactory == null)
                {
                    Plugin.LogError("A session reload was requested without a reconnect target");
                    return;
                }

                if (!reconnectActive)
                {
                    reconnectDeadlineUtc = DateTime.UtcNow + ReconnectTimeout;
                    Plugin.Log("Session is reloading; reconnecting automatically");
                }
                reconnectActive = true;
                reconnectDisconnectCurrent = true;
                reconnectDelayFrames = ReconnectRetryDelayFrames;
            }
        }

        private static void FinishSessionReconnect()
        {
            lock (ReconnectLock)
            {
                reconnectActive = false;
                reconnectAttemptInProgress = false;
                reconnectDisconnectCurrent = false;
            }
        }

        private bool TryToConnect(ISocketStream socket, bool automatic)
        {
            Plugin.Log("Connecting client");
            client = ClientEventIO.Create(socket, LoadMap, (error) =>
            {
                client = null;
                if (automatic || error == TimberNetBase.SESSION_RESTART_REQUIRED)
                {
                    RequestSessionReconnect();
                }
                else
                {
                    PendingErrors.Enqueue(Tuple.Create(
                        "BeaverBuddies.JoinCoopGame.Error.CouldNotConnect", error));
                }
            }, RequestSessionReconnect);
            
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
            return mapBytes != null && mapBytes.Length >= 4 &&
                mapBytes[0] == 0x50 && mapBytes[1] == 0x4B &&
                mapBytes[2] == 0x03 && mapBytes[3] == 0x04;
        }

        private void LoadMap(byte[] mapBytes)
        {
            if (!IsValidMap(mapBytes))
            {
                Plugin.LogError($"Received invalid map data ({mapBytes?.Length ?? 0} bytes); " +
                    "the host likely disconnected. Aborting load instead of crashing.");
                ShowError(null);
                FinishSessionReconnect();
                EventIO.Reset();
                client = null;
                return;
            }

            FinishSessionReconnect();

            // Clean up our current co-op state before loading,
            // so we don't, for example, end up ticking the client before
            // it's actually loaded.
            SingletonManager.Reset();

            Plugin.Log("Loading map");
            //string saveName = Guid.NewGuid().ToString();
            string saveName = TimberNetBase.GetHashCode(mapBytes).ToString("X8");
            SaveReference saveRef = new SaveReference("Online Games", new SettlementReference(saveName, _gameSaveRepository.DefaultSaveDirectory));
            Stream stream = _gameSaveRepository.CreateSaveSkippingNameValidation(saveRef);
            stream.Write(mapBytes);
            stream.Close();

            // Set the RNG seed before loading the map
            // The server does the same
            DeterminismService.InitGameStartState(mapBytes);
            _gameSceneLoader.StartSaveGame(saveRef);
        }

        public void UpdateSingleton()
        {
            while (PendingErrors.TryDequeue(out Tuple<string, string> error))
            {
                ShowError(error.Item1, error.Item2);
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

                if (DateTime.UtcNow >= reconnectDeadlineUtc)
                {
                    reconnectActive = false;
                    reconnectDisconnectCurrent = false;
                    timedOut = true;
                }
                else
                {
                    disconnectCurrent = reconnectDisconnectCurrent;
                    reconnectDisconnectCurrent = false;

                    if (reconnectDelayFrames > 0)
                    {
                        reconnectDelayFrames--;
                    }
                    else if (client == null)
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
                    "The host did not finish reloading within two minutes.");
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
                        reconnectDelayFrames = ReconnectRetryDelayFrames;
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

        private bool ResolveHostnameIfNecessary(string address, out string resolvedAddress)
        {
            resolvedAddress = null;

            try
            {
                // Otherwise, try to resolve it
                IPHostEntry hostEntry = Dns.GetHostEntry(address);
                if (hostEntry.AddressList.Length > 0)
                {
                    resolvedAddress = hostEntry.AddressList[0].ToString();
                    Plugin.Log(address + " resolved to " + resolvedAddress);
                    return true;
                }
            }
            catch (Exception ex)
            {
                Plugin.LogError("Could not resolve hostname: " + ex.ToString());
            }

            return false;
        }
    }
}

using System;
using TimberNet;
using static TimberNet.TimberNetBase;

namespace BeaverBuddies.IO
{
    public class ClientEventIO : NetIOBase<TimberClient>
    {
        // If the client receives an event to replay, no matter where it
        // originated, it shouldn't send it *back* to the server, since the
        // server is what sent the event.
        public override bool RecordReplayedEvents => false;

        // Clients don't need to send heartbeats
        public override bool ShouldSendHeartbeat => false;

        // The client doesn't get to do anything from the user directly.
        // The client should send user-initiated events to the server.
        // It has to wait until an event is received from the server.
        public override UserEventBehavior UserEventBehavior => UserEventBehavior.Send;

        private MapReceived mapReceivedCallback;
        private Action sessionRestartCallback;
        private MessageReceived errorCallback;
        private MessageReceived disconnectedCallback;
        private MapTransferStarted mapTransferStartedCallback;
        private readonly object cleanupLock = new object();
        private volatile bool failedToConnect;

        public void NotifyLoaded()
        {
            NetBase?.NotifyLoaded();
        }

        private ClientEventIO(ISocketStream socket, MapReceived mapReceivedCallback,
            Action<string> onError, Action onSessionRestart, Action<string> onDisconnected,
            Action<int> onMapTransferStarted)
        {
            this.mapReceivedCallback = mapReceivedCallback;
            sessionRestartCallback = onSessionRestart;

            TryRegisterSteamPacketReceiver(socket);

            NetBase = new TimberClient(socket);
            NetBase.OnMapReceived += mapReceivedCallback;
            mapTransferStartedCallback = mapLength => onMapTransferStarted(mapLength);
            NetBase.OnMapTransferStarted += mapTransferStartedCallback;
            NetBase.OnSessionRestart += sessionRestartCallback;
            NetBase.OnLog += Plugin.Log;
            errorCallback = error =>
            {
                Plugin.LogError(error);
                failedToConnect = true;
                CleanUp();
                onError(error);
            };
            disconnectedCallback = reason =>
            {
                Plugin.LogWarning($"Client disconnected: {reason}");
                CleanUp();
                onDisconnected(reason);
            };
            NetBase.OnError += errorCallback;
            NetBase.OnDisconnected += disconnectedCallback;
            try
            {
                NetBase.Start();
            }
            catch (Exception ex)
            {
                onError(ex.Message);
                Plugin.LogError(ex.ToString());
                failedToConnect = true;
                CleanUp();
            }
        }

        private void CleanUp()
        {
            lock (cleanupLock)
            {
                TimberClient netBase = NetBase;
                if (netBase == null) return;
                NetBase = null;
                netBase.OnMapReceived -= mapReceivedCallback;
                netBase.OnMapTransferStarted -= mapTransferStartedCallback;
                netBase.OnSessionRestart -= sessionRestartCallback;
                netBase.OnLog -= Plugin.Log;
                netBase.OnError -= errorCallback;
                netBase.OnDisconnected -= disconnectedCallback;
                netBase.Close();
            }
        }

        public override void Close()
        {
            CleanUp();
        }

        public static ClientEventIO Create(ISocketStream socket, MapReceived mapReceivedCallback,
            Action<string> onError, Action onSessionRestart, Action<string> onDisconnected,
            Action<int> onMapTransferStarted)
        {
            ClientEventIO eventIO = new ClientEventIO(
                socket, mapReceivedCallback, onError, onSessionRestart, onDisconnected,
                onMapTransferStarted);
            if (eventIO.failedToConnect) return null;
            return eventIO;
        }
    }
}

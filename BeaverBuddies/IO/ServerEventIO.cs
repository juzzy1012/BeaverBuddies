using Newtonsoft.Json.Linq;
using System;
using TimberNet;
using System.Threading.Tasks;
using BeaverBuddies.Events;
using BeaverBuddies.Steam;
using System.Net.Sockets;
using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace BeaverBuddies.IO
{
    // A client joining a running session causes an on-demand rehost. Every peer
    // then loads the same checkpoint, avoiding Timberborn state that is not fully
    // represented by a save until all peers reconstruct it through the same load.
    public class ServerEventIO : NetIOBase<TimberServer>
    {
        // Anything that happens on the server should be recorded and
        // sent to the clients.
        public override bool RecordReplayedEvents => true;

        // Servers need to send heartbeats so clients know to progress.
        public override bool ShouldSendHeartbeat => true;

        // The server should wait until the next update to play a
        // user-initiated event, to make sure that the events
        // happen in the same order for the server and clients.
        public override UserEventBehavior UserEventBehavior => UserEventBehavior.QueuePlay;

        public ISocketListener SocketListener { get; private set; }

        public bool AreAllClientsReady => NetBase == null || NetBase.AreAllClientsReady;
        public int ClientCount => NetBase?.ClientCount ?? 0;

        private int lateJoinRequested;

        // The map remains static for this synchronization epoch. A late join
        // creates a new checkpoint and therefore a new ServerEventIO instance.
        public void Start(byte[] mapBytes, int minimumReadyClients = 0)
        {
            try
            {
                List<ISocketListener> listeners = [
                    new TCPListenerWrapper(Settings.Port)
                ];
                if (SteamOverlayConnectionService.IsSteamEnabled && Settings.EnableSteam)
                {
                    listeners.Add(new SteamListener());
                }
                SocketListener = new MultiSocketListener(listeners.ToArray());
                if (SocketListener is MultiSocketListener)
                {
                    foreach (ISocketListener child in ((MultiSocketListener)SocketListener).Listeners)
                    {
                        TryRegisterSteamPacketReceiver(child);
                    }
                }
                else
                {
                    TryRegisterSteamPacketReceiver(SocketListener);
                }
                NetBase = new TimberServer(
                    SocketListener,
                    () =>
                    {
                        // TODO: Probably don't need to hold it in memory after the first tick...
                        Task<byte[]> task = new Task<byte[]>(() => mapBytes);
                        task.Start();
                        return task;
                    },
                    CreateInitEvent(),
                    minimumReadyClients
                );
            }
            catch (Exception e)
            {
                Plugin.Log("Failed to start server");
                Plugin.Log(e.ToString());
                return;
            }
            //netBase = new TimberServer(port, mapProvider, null);
            NetBase.OnLog += Plugin.Log;
            NetBase.OnMapReceived += NetBase_OnClientConnected;
            NetBase.OnLateJoinRequested += () => Interlocked.Exchange(ref lateJoinRequested, 1);
            NetBase.Start();
        }

        public bool TryConsumeLateJoinRequest()
        {
            return Interlocked.Exchange(ref lateJoinRequested, 0) == 1;
        }

        public void MarkGameStarted()
        {
            NetBase?.MarkGameStarted();
        }

        public void NotifySessionRestart()
        {
            NetBase?.NotifySessionRestart();
        }

        public void CancelSessionRestart()
        {
            NetBase?.CancelSessionRestart();
        }

        private Func<JObject> CreateInitEvent()
        {
            // It should be ok to send an init event even if the client is joining before
            // the server, since a) it won't do much on the Host (just set the random seed)
            // and b) the client will overwrite these values later whent he Host finished
            // loading the map.
            return () =>
            {
                var message = InitializeClientEvent.Create();
                message.ticksSinceLoad = 0;
                Plugin.Log($"Sending start state: {JsonSettings.Serialize(message)}");
                return JObject.Parse(JsonSettings.Serialize(message));
            };
        }

        private void NetBase_OnClientConnected(byte[] mapBytes)
        {

        }
    }
}

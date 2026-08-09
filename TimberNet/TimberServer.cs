using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TimberNet
{
    public class TimberServer : TimberNetBase
    {
        private readonly List<ISocketStream> clients = new List<ISocketStream>();
        private readonly ConcurrentDictionary<ISocketStream, ConcurrentQueue<JObject>> queuedMessages =
            new ConcurrentDictionary<ISocketStream, ConcurrentQueue<JObject>>();
        private readonly Dictionary<ISocketStream, string> clientReadyTokens =
            new Dictionary<ISocketStream, string>();
        private readonly HashSet<string> readyClients = new HashSet<string>();
        private readonly int minimumReadyClients;

        private readonly ISocketListener listener;
        private Func<Task<byte[]>> mapProvider;
        private Func<JObject>? initEventProvider;
        private int nextReadyToken;
        private int coordinatedRestartRequested;
        private bool gameStarted;

        private string? errorMessage;

        public event Action? OnLateJoinRequested;

        public int ClientCount
        {
            get
            {
                lock (queuedMessages)
                {
                    RemoveDisconnectedClients();
                    return clients.Count;
                }
            }
        }

        public bool AreAllClientsReady
        {
            get
            {
                lock (queuedMessages)
                {
                    RemoveDisconnectedClients();
                    return clients.Count >= minimumReadyClients && clients.All(client =>
                        clientReadyTokens.TryGetValue(client, out string? token) &&
                        readyClients.Contains(token));
                }
            }
        }

        public bool IsAcceptingClients => errorMessage == null;

        public TimberServer(
            ISocketListener listener,
            Func<Task<byte[]>> mapProvider,
            Func<JObject>? initEventProvider,
            int minimumReadyClients = 0)
        {
            this.listener = listener;
            this.mapProvider = mapProvider;
            this.initEventProvider = initEventProvider;
            this.minimumReadyClients = minimumReadyClients;
        }

        public List<string?> GetConnectedClients()
        {
            lock (queuedMessages)
            {
                RemoveDisconnectedClients();
                return clients.Select(client => client.Name).ToList();
            }
        }

        public void UpdateProviders(
            Func<Task<byte[]>> mapProvider,
            Func<JObject>? initEventProvider)
        {
            this.mapProvider = mapProvider;
            this.initEventProvider = initEventProvider;
        }

        public void MarkGameStarted()
        {
            lock (queuedMessages)
            {
                gameStarted = true;
            }
            Log("Session is running; future joins will create a coordinated checkpoint");
        }

        public void CancelSessionRestart()
        {
            Interlocked.Exchange(ref coordinatedRestartRequested, 0);
        }

        public void NotifySessionRestart()
        {
            JObject message = new JObject
            {
                [TICKS_KEY] = TickCount,
                [TYPE_KEY] = SESSION_RESTART_EVENT,
            };

            lock (queuedMessages)
            {
                RemoveDisconnectedClients();
                clients.ForEach(client => SendEvent(client, message));
            }
        }

        protected override void ReceiveEvent(JObject message)
        {
            if (GetType(message) == CLIENT_READY_EVENT)
            {
                string? token = message[READY_TOKEN_KEY]?.ToObject<string>();
                lock (queuedMessages)
                {
                    if (token != null && clientReadyTokens.Values.Contains(token))
                    {
                        readyClients.Add(token);
                        Log($"Client finished loading ({readyClients.Count}/{clients.Count})");
                    }
                    else
                    {
                        Log("Ignoring client-ready message with an invalid token");
                    }
                }
                return;
            }

            message[TICKS_KEY] = TickCount;
            base.ReceiveEvent(message);
        }

        public override void Start()
        {
            base.Start();
            listener.Start();
            Log("Server started listening");

            Task.Run(() =>
            {
                while (!IsStopped)
                {
                    ISocketStream client;
                    try
                    {
                        Log("Accepting client...");
                        client = listener.AcceptClient();
                    }
                    catch (Exception exception)
                    {
                        if (!IsStopped)
                        {
                            Log($"Error accepting client: {exception}");
                        }
                        continue;
                    }

                    Task.Run(async () => await SetUpClient(client));
                }
            });
        }

        private async Task SetUpClient(ISocketStream client)
        {
            bool clientRegistered = false;
            try
            {
                if (!IsAcceptingClients)
                {
                    SendErrorMessage(client, errorMessage!);
                    return;
                }

                if (!TryStartQueuing(client))
                {
                    RequestCoordinatedRestart();
                    SendErrorMessage(client, SESSION_RESTART_REQUIRED);
                    return;
                }
                clientRegistered = true;

                await SendMap(client);
                SendState(client);
                if (initEventProvider != null)
                {
                    // Broadcast the initialization event so every peer advances
                    // the event hash in the same order.
                    DoUserInitiatedEvent(initEventProvider(), true);
                }
                FinishQueuing(client);

                // This blocks until the client disconnects.
                StartListening(client, false);
            }
            catch (Exception exception)
            {
                Log($"Client setup failed: {exception}");
            }
            finally
            {
                if (clientRegistered)
                {
                    lock (queuedMessages)
                    {
                        RemoveClient(client);
                    }
                }
                client.Close();
            }
        }

        private void RequestCoordinatedRestart()
        {
            if (Interlocked.CompareExchange(ref coordinatedRestartRequested, 1, 0) != 0)
            {
                return;
            }

            Log("Late join requested; scheduling a coordinated checkpoint reload");
            try
            {
                OnLateJoinRequested?.Invoke();
            }
            catch (Exception exception)
            {
                Interlocked.Exchange(ref coordinatedRestartRequested, 0);
                Log($"Late-join callback failed: {exception}");
            }
        }

        public void StopAcceptingClients(string errorMessage)
        {
            this.errorMessage = errorMessage;
        }

        private bool TryStartQueuing(ISocketStream client)
        {
            lock (queuedMessages)
            {
                // This check and registration are atomic with MarkGameStarted.
                // A client either joins the original load barrier or triggers a
                // new checkpoint; it can never slip into a running simulation.
                if (gameStarted)
                {
                    return false;
                }

                queuedMessages.TryAdd(client, new ConcurrentQueue<JObject>());
                clients.Add(client);
                clientReadyTokens.Add(client, (++nextReadyToken).ToString());
                return true;
            }
        }

        private void RemoveClient(ISocketStream client)
        {
            if (clientReadyTokens.TryGetValue(client, out string? token))
            {
                readyClients.Remove(token);
                clientReadyTokens.Remove(client);
            }
            queuedMessages.TryRemove(client, out _);
            clients.Remove(client);
        }

        private void RemoveDisconnectedClients()
        {
            for (int index = clients.Count - 1; index >= 0; index--)
            {
                ISocketStream client = clients[index];
                if (!client.Connected)
                {
                    RemoveClient(client);
                }
            }
        }

        private void FinishQueuing(ISocketStream client)
        {
            lock (queuedMessages)
            {
                if (!queuedMessages.TryGetValue(client, out ConcurrentQueue<JObject> queue))
                {
                    Log("Warning! Missing client queue");
                    return;
                }

                while (queue.TryDequeue(out JObject message))
                {
                    SendEvent(client, message);
                }
                queuedMessages.TryRemove(client, out _);
            }
        }

        private void SendErrorMessage(ISocketStream client, string message)
        {
            SendLength(client, 0);
            SendDataWithLength(client, MessageToBuffer(message));
        }

        private async Task SendMap(ISocketStream client)
        {
            Log("Waiting for map...");
            byte[] mapBytes = await mapProvider();
            Log($"Sending map with length {mapBytes.Length}");
            SendDataWithLength(client, mapBytes);
            Log($"Sent map with length {mapBytes.Length} and Hash: {GetHashCode(mapBytes):X8}");
        }

        private void SendState(ISocketStream client)
        {
            JObject message = new JObject
            {
                [TICKS_KEY] = 0,
                [TYPE_KEY] = SET_STATE_EVENT,
                ["hash"] = Hash,
            };
            lock (queuedMessages)
            {
                message[READY_TOKEN_KEY] = clientReadyTokens[client];
            }
            SendEvent(client, message);
        }

        private void DoUserInitiatedEvent(JObject message, bool sendNow)
        {
            base.DoUserInitiatedEvent(message);
            lock (queuedMessages)
            {
                RemoveDisconnectedClients();
                clients.ForEach(client =>
                {
                    if (sendNow)
                    {
                        SendEvent(client, message);
                    }
                    else
                    {
                        QueueOrSendToClient(client, message);
                    }
                });
            }
        }

        public override void DoUserInitiatedEvent(JObject message)
        {
            DoUserInitiatedEvent(message, false);
        }

        private void QueueOrSendToClient(ISocketStream client, JObject message)
        {
            if (!client.Connected)
            {
                return;
            }

            if (queuedMessages.TryGetValue(client, out ConcurrentQueue<JObject> queue))
            {
                queue.Enqueue(message);
            }
            else
            {
                SendEvent(client, message);
            }
        }

        public override void Close()
        {
            base.Close();
            lock (queuedMessages)
            {
                clients.ToList().ForEach(client => client.Close());
                clients.Clear();
                queuedMessages.Clear();
                clientReadyTokens.Clear();
                readyClients.Clear();
            }
            try
            {
                listener.Stop();
            }
            catch (Exception exception)
            {
                Log(exception.ToString());
            }
        }

        public void SendHeartbeat()
        {
            JObject message = new JObject
            {
                [TICKS_KEY] = TickCount,
                [TYPE_KEY] = HEARTBEAT_EVENT,
            };
            DoUserInitiatedEvent(message);
        }
    }
}

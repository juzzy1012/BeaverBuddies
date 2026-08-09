using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TimberNet
{
    public class ConnectionFailureException : Exception
    {
        public ConnectionFailureException() : base("Client connection timed out") { }
    }

    public class TimberClient : TimberNetBase
    {

        private readonly ISocketStream client;
        private string? readyToken;
        private bool hasFinishedLoading;
        private bool readyNotificationSent;

        public event Action? OnSessionRestart;

        public override bool ShouldTick => base.ShouldTick && receivedEvents.Count > 0;

        public TimberClient(ISocketStream client) : base()
        {
            this.client = client;
        }

        public override void DoUserInitiatedEvent(JObject message)
        {
            // Don't actually do the event (i.e. add it to the hash)
            // Wait for the server to confirm w/ adjusted Tick
            SendEvent(client, message);
        }

        protected override void ReceiveEvent(JObject message)
        {
            string type = GetType(message);
            if (type == SET_STATE_EVENT)
            {
                readyToken = message[READY_TOKEN_KEY]?.ToObject<string>();
                TryNotifyLoaded();
            }
            else if (type == SESSION_RESTART_EVENT)
            {
                Log("Host requested a coordinated session reload");
                OnSessionRestart?.Invoke();
                return;
            }
            base.ReceiveEvent(message);
        }

        public void NotifyLoaded()
        {
            hasFinishedLoading = true;
            // Process any state message that arrived while the game scene loaded.
            Update();
            TryNotifyLoaded();
        }

        private void TryNotifyLoaded()
        {
            if (!hasFinishedLoading || readyNotificationSent || string.IsNullOrEmpty(readyToken))
            {
                return;
            }

            JObject message = new JObject();
            message[TICKS_KEY] = TickCount;
            message[TYPE_KEY] = CLIENT_READY_EVENT;
            message[READY_TOKEN_KEY] = readyToken;
            SendEvent(client, message);
            readyNotificationSent = true;
            Log("Notified server that loading finished");
        }

        protected override void ProcessReceivedEvent(JObject message)
        {
            base.ProcessReceivedEvent(message);
            Log($"Received event: {message[TYPE_KEY]?.ToString() ?? "<null>"}");
            AddEventToHash(message);
        }

        public override void Start()
        {
            base.Start();
            // TODO: Handle async properly and cleanup
            // TODO: Make wait configurable?
            if (!client.ConnectAsync().Wait(3000))
            {
                throw new ConnectionFailureException();
            }
            // Connect a TCP socket at the address
            Task.Run(() => StartListening(client, true));
        }


        public override void Close()
        {
            base.Close();
            client.Close();
        }
    }
}

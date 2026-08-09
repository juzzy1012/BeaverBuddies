using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TimberNet
{
    public class ConnectionFailureException : Exception
    {
        public ConnectionFailureException() : base("Client connection timed out") { }
    }

    public class TimberClient : TimberNetBase
    {
        private static readonly TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(3);
        private static readonly TimeSpan InitialResponseTimeout = TimeSpan.FromSeconds(30);

        private readonly ISocketStream client;
        private string? readyToken;
        private bool hasFinishedLoading;
        private bool readyNotificationSent;
        private int initialResponseState;
        private readonly CancellationTokenSource responseTimeoutSource =
            new CancellationTokenSource();

        public event Action? OnSessionRestart;

        public override bool ShouldTick => base.ShouldTick && receivedEvents.Count > 0;

        public TimberClient(ISocketStream client) : base()
        {
            this.client = client;
            OnMapTransferStarted += _ => CompleteInitialResponse();
            OnError += _ => CompleteInitialResponse();
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
            Task.Run(ConnectAndListen);
        }

        private async Task ConnectAndListen()
        {
            try
            {
                Task connection = client.ConnectAsync();
                Task completed = await Task.WhenAny(
                    connection, Task.Delay(ConnectionTimeout)).ConfigureAwait(false);
                if (completed != connection)
                {
                    client.Close();
                    _ = connection.ContinueWith(
                        task => _ = task.Exception,
                        CancellationToken.None,
                        TaskContinuationOptions.OnlyOnFaulted,
                        TaskScheduler.Default);
                    throw new ConnectionFailureException();
                }

                await connection.ConfigureAwait(false);
                if (!IsStopped)
                {
                    _ = Task.Delay(InitialResponseTimeout, responseTimeoutSource.Token).ContinueWith(
                        _ => CheckInitialResponseTimeout(),
                        CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously |
                            TaskContinuationOptions.OnlyOnRanToCompletion,
                        TaskScheduler.Default);
                    StartListening(client, true);
                }
            }
            catch (Exception exception)
            {
                if (!IsStopped)
                {
                    ReportError(exception.GetBaseException().Message);
                }
            }
        }

        private void CompleteInitialResponse()
        {
            if (Interlocked.CompareExchange(ref initialResponseState, 1, 0) == 0)
            {
                responseTimeoutSource.Cancel();
            }
        }

        private void CheckInitialResponseTimeout()
        {
            if (!IsStopped &&
                Interlocked.CompareExchange(ref initialResponseState, -1, 0) == 0)
            {
                try
                {
                    ReportError("The host accepted the connection but did not begin sending a save.");
                }
                finally
                {
                    client.Close();
                }
            }
        }


        public override void Close()
        {
            base.Close();
            responseTimeoutSource.Cancel();
            client.Close();
        }
    }
}

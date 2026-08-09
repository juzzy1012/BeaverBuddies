using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace TimberNet
{
    // TODO: I should create a method here that attempts to operate on a steam,
    // and handles errors uniformly if it fails.
    // Pretty much any error means the session is over, but the game should show
    // the error rather than crashing.
    public abstract class TimberNetBase
    {
        public const int HEADER_SIZE = 4;
        public const string TICKS_KEY = "ticksSinceLoad";
        public const string TYPE_KEY = "type";
        public const string SET_STATE_EVENT = "SetState";
        public const string HEARTBEAT_EVENT = "Heartbeat";
        public const string CLIENT_READY_EVENT = "ClientReady";
        public const string SESSION_RESTART_EVENT = "SessionRestart";
        public const string SESSION_RESTART_REQUIRED = "BeaverBuddies.SessionRestartRequired";
        public const string READY_TOKEN_KEY = "readyToken";
        public const int MAX_BUFFER_SIZE = 8192 * 4; // 32K
        public const int MAX_EVENT_SIZE = 8 * 1024 * 1024;
        public const int MAX_MAP_SIZE = 512 * 1024 * 1024;
        public const int MAX_ERROR_SIZE = 64 * 1024;

        public delegate void MessageReceived(string message);
        public delegate void MapReceived(byte[] mapBytes);
        public delegate void MapTransferStarted(int mapLength);

        public event MessageReceived? OnLog;
        public event MessageReceived? OnError;
        public event MessageReceived? OnDisconnected;
        public event MapReceived? OnMapReceived;
        public event MapTransferStarted? OnMapTransferStarted;

        private readonly ConcurrentQueue<string> receivedEventQueue = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> logQueue = new ConcurrentQueue<string>();
        private byte[]? mapBytes = null;

        private int stopped;
        public bool IsStopped => Volatile.Read(ref stopped) != 0;

        public int Hash { get; private set; } = 17;

        public int TickCount { get; private set; }

        public int TicksBehind
        {
            get
            {
                if (receivedEvents.Count == 0)
                    return 0;
                return Math.Max(0, GetTick(receivedEvents.Last()) - TickCount);
            }
        }

        public bool Started { get; private set; }

        public virtual bool ShouldTick => Started;

        protected List<JObject> receivedEvents = new List<JObject>();

        public virtual void Close()
        {
            Interlocked.Exchange(ref stopped, 1);
        }

        public TimberNetBase()
        {
            Log("Started");
        }

        protected void Log(string message)
        {
            Log(message, TickCount, Hash);
        }

        protected void Log(string message, int ticks, int hash)
        {
            // Should be threadsafe
            OnLog?.Invoke($"T{ticks.ToString("D4")} [{hash.ToString("X8")}] : {message}");
            //logQueue.Enqueue($"T{ticks.ToString("D4")} [{hash.ToString("X8")}] : {message}");
        }

        protected void ReportError(string message)
        {
            OnError?.Invoke(message);
        }

        private void ReportDisconnected(string message)
        {
            OnDisconnected?.Invoke(message);
        }

        public virtual void Start()
        {
            Started = true;
        }

        public static int GetTick(JObject message)
        {
            if (message[TICKS_KEY] == null)
                throw new Exception($"Message does not contain {TICKS_KEY} key");
            return message[TICKS_KEY]!.ToObject<int>();
        }

        public static string GetType(JObject message)
        {
            var type = message["type"];
            if (type == null)
                throw new Exception($"Message does not contain type key");
            return type.ToObject<string>()!;
        }

        protected void InsertInScript(JObject message, List<JObject> script)
        {
            int tick = GetTick(message);
            int index = script.FindIndex(m => GetTick(m) > tick);

            if (index == -1)
                script.Add(message);
            else
                script.Insert(index, message);
        }

        public static List<T> PopEventsForTick<T>(int tick, List<T> events, Func<T, int> getTick)
        {
            List<T> list = new List<T>();
            while (events.Count > 0)
            {
                T message = events[0];
                int delay = getTick(message);
                if (delay > tick)
                    break;

                events.RemoveAt(0);
                list.Add(message);
            }
            return list;
        }

        private List<JObject> PopEventsToProcess(List<JObject> events)
        {
            if (events.Count == 0) return new List<JObject>();
            JObject firstEvent = events[0];
            int firstEventTick = GetTick(firstEvent);
            if (firstEventTick < TickCount)
                Log($"Warning: late event {GetType(firstEvent)}: {firstEventTick} < {TickCount}");

            return PopEventsForTick(TickCount, events, GetTick);
        }

        /**
         * Process an event that the user initiated.
         */
        public virtual void DoUserInitiatedEvent(JObject message)
        {
            AddEventToHash(message);
        }

        /**
        * Process a validated event from a peer that is ready to happen on
        * the Update() thread.
        */
        protected virtual void ProcessReceivedEvent(JObject message)
        {
        }

        protected void AddEventToHash(JObject message)
        {
            if (GetType(message) == SET_STATE_EVENT)
            {
                Hash = message["hash"]!.ToObject<int>();
            }
            else
            {
                AddToHash(message.ToString());
            }
            Log($"Event: {GetType(message)}");
        }

        protected void SendLength(ISocketStream stream, int length)
        {
            byte[] buffer = BitConverter.GetBytes(length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(buffer);
            stream.Write(buffer, 0, buffer.Length);
        }

        protected void SendDataWithLength(ISocketStream stream, byte[] data)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));

            // A length-prefixed message is one atomic frame. Multiple client
            // setup and gameplay tasks may write to the same stream.
            lock (stream)
            {
                SendLength(stream, data.Length);
                int chunkSize = stream.MaxChunkSize;
                if (chunkSize <= 0 || stream.MaxBytesPerSecond <= 0)
                    throw new InvalidOperationException("Socket stream has invalid transfer limits.");

                // How long to sleep between chunks (may be 0).
                int sleepMS = chunkSize * 1000 / stream.MaxBytesPerSecond;
                for (int i = 0; i < data.Length; i += chunkSize)
                {
                    if (i != 0 && sleepMS > 0)
                    {
                        Thread.Sleep(sleepMS);
                    }
                    int length = Math.Min(chunkSize, data.Length - i);
                    stream.Write(data, i, length);
                }
            }
        }

        protected void SendEvent(ISocketStream client, JObject message)
        {
            Log($"Sending: {GetType(message)} for tick {GetTick(message)}");
            byte[] buffer = MessageToBuffer(message);

            try
            {
                if (buffer.Length > MAX_EVENT_SIZE)
                {
                    throw new InvalidDataException(
                        $"Event frame is {buffer.Length} bytes; maximum is {MAX_EVENT_SIZE}.");
                }
                SendDataWithLength(client, buffer);
            } catch (Exception e)
            {
                Log($"Error sending event: {e.Message}");
                // Reliable transports either deliver the complete ordered frame or
                // the session is no longer safe to continue. Leaving the peer in
                // the client list after a failed write creates a silent desync.
                client.Close();
            }
        }

        protected bool TryReadLength(ISocketStream stream, out int length)
        {
            byte[] headerBuffer;
            try
            {
                headerBuffer = stream.ReadUntilComplete(HEADER_SIZE);
            }
            catch
            {
                length = 0;
                return false;
            }
            if (BitConverter.IsLittleEndian)
                Array.Reverse(headerBuffer);

            length = BitConverter.ToInt32(headerBuffer, 0);
            return true;
        }

        protected void StartListening(ISocketStream client, bool isClient)
        {
            int messageCount = 0;
            bool terminalMessageHandled = false;
            string disconnectReason = "Connection closed.";
            try
            {
                while (client.Connected && !IsStopped)
                {
                    if (!TryReadLength(client, out int messageLength))
                    {
                        disconnectReason = "Connection closed while reading a frame header.";
                        break;
                    }

                    // The first server-to-client frame is always the save file,
                    // except for a zero-length retry/error sentinel.
                    if (messageCount == 0 && isClient)
                    {
                        if (messageLength == 0)
                        {
                            ReadErrorMessage(client);
                            terminalMessageHandled = true;
                            return;
                        }
                        ValidateFrameLength(messageLength, MAX_MAP_SIZE, "map");
                        ReportMapTransferStarted(messageLength);
                        ReceiveFile(client, messageLength);
                        messageCount++;
                        continue;
                    }

                    ValidateFrameLength(messageLength, MAX_EVENT_SIZE, "event");
                    byte[] buffer = client.ReadUntilComplete(messageLength);
                    string message = BufferToStringMessage(buffer);
                    receivedEventQueue.Enqueue(message);
                    messageCount++;
                }
            }
            catch (Exception exception)
            {
                disconnectReason = exception.Message;
                Log($"Connection listener stopped: {exception}");
            }
            finally
            {
                if (isClient && !IsStopped && !terminalMessageHandled)
                {
                    ReportDisconnected(disconnectReason);
                }
            }
        }

        private void ReportMapTransferStarted(int mapLength)
        {
            try
            {
                OnMapTransferStarted?.Invoke(mapLength);
            }
            catch (Exception exception)
            {
                // Progress observers must never be able to terminate the protocol
                // reader. They are advisory and do not affect synchronization.
                Log($"Map transfer observer failed: {exception}");
            }
        }

        private static void ValidateFrameLength(int length, int maximum, string frameType)
        {
            if (length <= 0 || length > maximum)
            {
                throw new InvalidDataException(
                    $"Invalid {frameType} frame length {length}; expected 1-{maximum} bytes.");
            }
        }

        protected byte[] MessageToBuffer(JObject message)
        {
            string json = message.ToString(Newtonsoft.Json.Formatting.None);
            return MessageToBuffer(json);
        }

        protected byte[] MessageToBuffer(string message)
        {
            return CompressionUtils.Compress(message);
        }

        protected string BufferToStringMessage(byte[] buffer)
        {
            return CompressionUtils.Decompress(buffer);
        }

        private void ReadErrorMessage(ISocketStream stream)
        {
            if (TryReadLength(stream, out int length))
            {
                ValidateFrameLength(length, MAX_ERROR_SIZE, "error");
                byte[] bytes = stream.ReadUntilComplete(length);
                string message = BufferToStringMessage(bytes);
                ReportError(message);
                return;
            }
            throw new IOException("Connection closed while reading an error response.");
        }

        public static int CombineHash(int h1, int h2)
        {
            return h1 * 31 + h2;
        }

        private void AddToHash(string str)
        {
            AddToHash(Encoding.UTF8.GetBytes(str));
        }

        private void AddToHash(byte[] bytes)
        {
            Hash = CombineHash(Hash, GetHashCode(bytes));
        }

        public static int GetHashCode(byte[] bytes)
        {
            int code = 0;
            foreach (byte b in bytes)
            {
                code = CombineHash(code, b);
            }
            return code;
        }

        private void AddFileToHash(byte[] bytes)
        {
            AddToHash(bytes);
        }

        private void ReceiveFile(ISocketStream stream, int messageLength)
        {
            byte[] mapBytes = stream.ReadUntilComplete(messageLength);
            AddFileToHash(mapBytes);
            Log($"Received map with length {mapBytes.Length} and Hash: {GetHashCode(mapBytes).ToString("X8")}");
            Volatile.Write(ref this.mapBytes, mapBytes);
        }

        private void ProcessReceivedEventsQueue()
        {
            while (receivedEventQueue.TryDequeue(out string? message))
            {
                try
                {
                    ReceiveEvent(JObject.Parse(message));
                } catch (Exception e)
                {
                    Log($"Error receiving event: {e.Message}");
                }
            }
        }

        /**
         * Called when an event is received from a connected Net
         * and ready to be added to the queue for processing.
         */
        protected virtual void ReceiveEvent(JObject message)
        {
            InsertInScript(message, receivedEvents);
        }

        private void ProcessLogs()
        {
            while (logQueue.TryDequeue(out string? log))
            {
                OnLog?.Invoke(log);
            }
        }

        private void ProcessReceivedMap()
        {
            byte[]? receivedMap = Interlocked.Exchange(ref mapBytes, null);
            if (receivedMap == null) return;
            OnMapReceived?.Invoke(receivedMap);
        }

        /**
         * Updates, processing queued logs, maps and events.
         */
        public void Update()
        {
            ProcessLogs();
            if (!Started) return;
            ProcessReceivedMap();
            ProcessReceivedEventsQueue();

        }

        private List<JObject> FilterEvents(List<JObject> events)
        {
            return events.Where(ShouldReadEvent).ToList();
        }

        private bool ShouldReadEvent(JObject message)
        { 
            string type = GetType(message);
            return !(type == SET_STATE_EVENT || type == HEARTBEAT_EVENT);
        }

        /**
         * Reads received events that should be processed by the game
         * and deletes and returns.
         * Will call update before processing events.
         */
        public virtual List<JObject> ReadEvents(int ticksSinceLoad)
        {
            //if (ticksSinceLoad != TickCount) Log($"Setting ticks from {TickCount} to {ticksSinceLoad}");
            TickCount = ticksSinceLoad;
            Update();
            List<JObject> toProcess = PopEventsToProcess(receivedEvents);
            toProcess.ForEach(e => ProcessReceivedEvent(e));
            return FilterEvents(toProcess);
        }

        public bool HasEventsForTick(int tickSinceLoad)
        {
            Update();
            return receivedEvents.Any(e => GetTick(e) == tickSinceLoad);
        }
    }
}

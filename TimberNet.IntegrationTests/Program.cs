using System.Net;
using System.Net.Sockets;
using Newtonsoft.Json.Linq;
using TimberNet;

internal static class Program
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static int Main()
    {
        try
        {
            Run("concurrent initial-client handshake", TestConcurrentInitialClients);
            Run("coordinated late join", TestCoordinatedLateJoin);
            Run("atomic framing and protocol limits", TestFramingAndLimits);
            Run("disconnect lifecycle", TestDisconnectLifecycle);
            Run("transport fallback", TestTransportFallback);
            Console.WriteLine("PASS: all TimberNet integration tests completed.");
            return 0;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"FAIL: {exception}");
            return 1;
        }
    }

    private static void Run(string name, Action test)
    {
        test();
        Console.WriteLine($"PASS: {name}");
    }

    private static void TestConcurrentInitialClients()
    {
        TimberServer? server = null;
        TimberClient? firstClient = null;
        TimberClient? secondClient = null;
        try
        {
            int port = FindAvailablePort();
            byte[] map = TestMap();
            int mapRequests = 0;
            var releaseMaps = new TaskCompletionSource<byte[]>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            Task<byte[]> ProvideMap()
            {
                if (Interlocked.Increment(ref mapRequests) == 2)
                {
                    releaseMaps.TrySetResult(map);
                }
                return releaseMaps.Task;
            }

            server = new TimberServer(
                new TCPListenerWrapper(port),
                ProvideMap,
                () => CreateEvent(0, "IntegrationTestInitialize"),
                minimumReadyClients: 2);
            server.Start();

            firstClient = ConnectClient(port);
            secondClient = ConnectClient(port);
            WaitUntil(
                () => Volatile.Read(ref mapRequests) == 2,
                () => server.Update(),
                "both clients were not registered before map release");

            WaitForMap(firstClient);
            WaitForMap(secondClient);
            firstClient.NotifyLoaded();
            secondClient.NotifyLoaded();

            WaitUntil(
                () => firstClient.HasEventsForTick(0) && secondClient.HasEventsForTick(0),
                () =>
                {
                    firstClient.Update();
                    secondClient.Update();
                    server.Update();
                },
                "initialization event was not delivered to both clients");

            List<JObject> firstEvents = firstClient.ReadEvents(0);
            List<JObject> secondEvents = secondClient.ReadEvents(0);
            AssertEqual(1, firstEvents.Count, "first client initialization event count");
            AssertEqual(1, secondEvents.Count, "second client initialization event count");
            AssertEqual("IntegrationTestInitialize", TimberNetBase.GetType(firstEvents[0]),
                "first client initialization type");
            AssertEqual("IntegrationTestInitialize", TimberNetBase.GetType(secondEvents[0]),
                "second client initialization type");

            WaitUntil(
                () => server.AreAllClientsReady,
                () =>
                {
                    server.Update();
                    firstClient.Update();
                    secondClient.Update();
                },
                "concurrent clients never reached the ready barrier");
            AssertEqual(server.Hash, firstClient.Hash, "first concurrent client hash");
            AssertEqual(server.Hash, secondClient.Hash, "second concurrent client hash");
        }
        finally
        {
            secondClient?.Close();
            firstClient?.Close();
            server?.Close();
        }
    }

    private static void TestCoordinatedLateJoin()
    {
        TimberServer? server = null;
        TimberClient? firstClient = null;
        TimberClient? lateClient = null;
        TimberClient? duplicateLateClient = null;
        TimberClient? retriedLateClient = null;
        try
        {
            int port = FindAvailablePort();
            int restartRequests = 0;

            server = new TimberServer(
                new TCPListenerWrapper(port),
                () => Task.FromResult(TestMap()),
                () => CreateEvent(0, "IntegrationTestInitialize"),
                minimumReadyClients: 1);
            server.OnLateJoinRequested += () => Interlocked.Increment(ref restartRequests);
            server.Start();
            AssertEqual(false, server.AreAllClientsReady,
                "minimum-client ready barrier before reconnect");

            firstClient = ConnectClient(port);
            WaitForMap(firstClient);
            firstClient.NotifyLoaded();
            WaitUntil(
                () => firstClient.HasEventsForTick(0),
                firstClient.Update,
                "initial client did not receive its initialization event");
            firstClient.ReadEvents(0);
            WaitUntil(
                () => server.AreAllClientsReady,
                server.Update,
                "initial client never reached the ready barrier");
            AssertEqual(server.Hash, firstClient.Hash, "initial client hash");

            bool restartNotificationReceived = false;
            firstClient.OnSessionRestart += () => restartNotificationReceived = true;
            server.MarkGameStarted();

            string? lateError = null;
            lateClient = ConnectClient(port, error => lateError = error);
            WaitUntil(
                () => Volatile.Read(ref lateError) != null,
                lateClient.Update,
                "late client was not told to wait for a checkpoint reload");
            AssertEqual(TimberNetBase.SESSION_RESTART_REQUIRED, lateError,
                "late-join retry signal");
            WaitUntil(
                () => Volatile.Read(ref restartRequests) == 1,
                server.Update,
                "late join did not request a coordinated reload");
            AssertEqual(1, server.ClientCount, "registered client count after late join");

            int serverHashBeforeControlMessage = server.Hash;
            int clientHashBeforeControlMessage = firstClient.Hash;
            server.NotifySessionRestart();
            WaitUntil(
                () => restartNotificationReceived,
                firstClient.Update,
                "existing client did not receive the reload notification");
            AssertEqual(serverHashBeforeControlMessage, server.Hash,
                "server hash after reload control message");
            AssertEqual(clientHashBeforeControlMessage, firstClient.Hash,
                "client hash after reload control message");

            string? duplicateError = null;
            duplicateLateClient = ConnectClient(port, error => duplicateError = error);
            WaitUntil(
                () => Volatile.Read(ref duplicateError) != null,
                duplicateLateClient.Update,
                "duplicate retry did not receive the reload signal");
            AssertEqual(1, Volatile.Read(ref restartRequests),
                "duplicate coordinated reload request count");

            server.CancelSessionRestart();
            string? retriedError = null;
            retriedLateClient = ConnectClient(port, error => retriedError = error);
            WaitUntil(
                () => Volatile.Read(ref retriedError) != null,
                retriedLateClient.Update,
                "retry after a cancelled reload did not receive the reload signal");
            WaitUntil(
                () => Volatile.Read(ref restartRequests) == 2,
                server.Update,
                "cancelled coordinated reload could not be retried");
        }
        finally
        {
            retriedLateClient?.Close();
            duplicateLateClient?.Close();
            lateClient?.Close();
            firstClient?.Close();
            server?.Close();
        }
    }

    private static void TestFramingAndLimits()
    {
        var stream = new RecordingSocket();
        var net = new TestNetBase();
        byte[] first = Enumerable.Repeat((byte)0x11, 4096).ToArray();
        byte[] second = Enumerable.Repeat((byte)0x22, 4096).ToArray();
        using var start = new ManualResetEventSlim(false);

        Task firstSend = Task.Run(() =>
        {
            start.Wait();
            net.SendFrame(stream, first);
        });
        Task secondSend = Task.Run(() =>
        {
            start.Wait();
            net.SendFrame(stream, second);
        });
        start.Set();
        Task.WaitAll(firstSend, secondSend);

        List<byte[]> frames = ParseFrames(stream.Bytes);
        AssertEqual(2, frames.Count, "atomic frame count");
        AssertEqual(true,
            frames.Any(frame => frame.SequenceEqual(first)), "first atomic frame");
        AssertEqual(true,
            frames.Any(frame => frame.SequenceEqual(second)), "second atomic frame");

        string text = new string('x', 2048);
        byte[] compressed = CompressionUtils.Compress(text);
        AssertEqual(text, CompressionUtils.Decompress(compressed, 4096),
            "bounded decompression round trip");
        AssertThrows<InvalidDataException>(
            () => CompressionUtils.Decompress(compressed, 1024),
            "decompression expansion limit");

        var failedWrite = new ThrowingWriteSocket();
        net.SendMessage(failedWrite, CreateEvent(0, "WriteFailure"));
        AssertEqual(false, failedWrite.Connected,
            "failed reliable write must close the connection");

        byte[] oversizedMapHeader = BigEndianLength(TimberNetBase.MAX_MAP_SIZE + 1);
        var invalidSocket = new ScriptedSocket(oversizedMapHeader);
        var invalidClient = new TimberClient(invalidSocket);
        string? disconnectReason = null;
        invalidClient.OnDisconnected += reason => disconnectReason = reason;
        invalidClient.Start();
        WaitUntil(
            () => Volatile.Read(ref disconnectReason) != null,
            () => { },
            "oversized map frame did not close the connection");
        if (!disconnectReason!.Contains("Invalid map frame length"))
        {
            throw new Exception($"unexpected oversized-frame error: {disconnectReason}");
        }
        invalidClient.Close();

        byte[] map = TestMap();
        var mapInput = BigEndianLength(map.Length).Concat(map).ToArray();
        var mapSocket = new ScriptedSocket(mapInput);
        var mapClient = new TimberClient(mapSocket);
        int announcedMapLength = 0;
        mapClient.OnMapTransferStarted += length => announcedMapLength = length;
        mapClient.Start();
        WaitUntil(
            () => Volatile.Read(ref announcedMapLength) != 0,
            () => { },
            "valid map header did not report transfer progress");
        AssertEqual(map.Length, announcedMapLength, "announced map length");
        mapClient.Close();
    }

    private static void TestDisconnectLifecycle()
    {
        var unexpectedSocket = new ScriptedSocket(Array.Empty<byte>());
        var unexpectedClient = new TimberClient(unexpectedSocket);
        int unexpectedDisconnects = 0;
        unexpectedClient.OnDisconnected += _ => Interlocked.Increment(ref unexpectedDisconnects);
        unexpectedClient.Start();
        WaitUntil(
            () => Volatile.Read(ref unexpectedDisconnects) == 1,
            () => { },
            "unexpected EOF did not raise a disconnect");
        unexpectedClient.Close();

        var blockingSocket = new BlockingSocket();
        var intentionalClient = new TimberClient(blockingSocket);
        int intentionalDisconnects = 0;
        intentionalClient.OnDisconnected += _ => Interlocked.Increment(ref intentionalDisconnects);
        intentionalClient.Start();
        if (!blockingSocket.ReadStarted.Wait(Timeout))
        {
            throw new TimeoutException("intentional-close test never started reading");
        }
        intentionalClient.Close();
        Thread.Sleep(100);
        AssertEqual(0, Volatile.Read(ref intentionalDisconnects),
            "intentional close disconnect count");
    }

    private static void TestTransportFallback()
    {
        var socket = new ScriptedSocket(Array.Empty<byte>());
        var working = new OneShotListener(socket);
        var listener = new MultiSocketListener(new FailingListener(), working);
        try
        {
            listener.Start();
            AssertEqual(1, listener.StartFailures.Count(),
                "failed transport count");
            AssertEqual<ISocketStream>(socket, listener.AcceptClient(),
                "fallback transport accepted socket");
        }
        finally
        {
            listener.Stop();
        }
        AssertEqual(true, working.WasStopped, "fallback transport stopped");
    }

    private static byte[] TestMap() =>
        new byte[] { 0x50, 0x4B, 0x03, 0x04, 1, 2, 3, 4 };

    private static JObject CreateEvent(int tick, string type)
    {
        return new JObject
        {
            [TimberNetBase.TICKS_KEY] = tick,
            [TimberNetBase.TYPE_KEY] = type,
            ["value"] = tick,
        };
    }

    private static TimberClient ConnectClient(int port, Action<string>? onError = null)
    {
        var client = new TimberClient(new TCPClientWrapper("127.0.0.1", port));
        if (onError != null)
        {
            client.OnError += message => onError(message);
        }
        client.Start();
        return client;
    }

    private static void WaitForMap(TimberClient client)
    {
        bool received = false;
        client.OnMapReceived += _ => received = true;
        WaitUntil(() => received, client.Update, "client did not receive the map");
    }

    private static void WaitUntil(Func<bool> condition, Action pump, string failure)
    {
        DateTime deadline = DateTime.UtcNow + Timeout;
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException(failure);
            }
            pump();
            Thread.Sleep(5);
        }
    }

    private static int FindAvailablePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static byte[] BigEndianLength(int length)
    {
        byte[] bytes = BitConverter.GetBytes(length);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(bytes);
        }
        return bytes;
    }

    private static List<byte[]> ParseFrames(byte[] bytes)
    {
        var frames = new List<byte[]>();
        int offset = 0;
        while (offset < bytes.Length)
        {
            if (bytes.Length - offset < TimberNetBase.HEADER_SIZE)
                throw new Exception("truncated frame header");
            byte[] header = bytes.Skip(offset).Take(TimberNetBase.HEADER_SIZE).ToArray();
            if (BitConverter.IsLittleEndian)
                Array.Reverse(header);
            int length = BitConverter.ToInt32(header, 0);
            offset += TimberNetBase.HEADER_SIZE;
            if (length < 0 || length > bytes.Length - offset)
                throw new Exception($"invalid recorded frame length {length}");
            frames.Add(bytes.Skip(offset).Take(length).ToArray());
            offset += length;
        }
        return frames;
    }

    private static void AssertThrows<TException>(Action action, string description)
        where TException : Exception
    {
        try
        {
            action();
        }
        catch (TException)
        {
            return;
        }
        throw new Exception($"{description}: expected {typeof(TException).Name}");
    }

    private static void AssertEqual<T>(T expected, T actual, string description)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
        {
            throw new Exception($"{description}: expected {expected}, got {actual}");
        }
    }

    private sealed class TestNetBase : TimberNetBase
    {
        public void SendFrame(ISocketStream stream, byte[] payload)
        {
            SendDataWithLength(stream, payload);
        }

        public void SendMessage(ISocketStream stream, JObject message)
        {
            SendEvent(stream, message);
        }
    }

    private sealed class RecordingSocket : ISocketStream
    {
        private readonly List<byte> bytes = new List<byte>();
        public byte[] Bytes
        {
            get
            {
                lock (bytes)
                {
                    return bytes.ToArray();
                }
            }
        }
        public bool Connected => true;
        public string? Name => "recording";
        public int MaxChunkSize => 7;
        public int MaxBytesPerSecond => int.MaxValue;
        public Task ConnectAsync() => Task.CompletedTask;
        public void Close() { }
        public int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
        public void Write(byte[] buffer, int offset, int count)
        {
            lock (bytes)
            {
                for (int index = 0; index < count; index++)
                {
                    bytes.Add(buffer[offset + index]);
                }
            }
            Thread.Yield();
        }
    }

    private sealed class ThrowingWriteSocket : ISocketStream
    {
        private int connected = 1;
        public bool Connected => Volatile.Read(ref connected) != 0;
        public string? Name => "throwing-write";
        public int MaxChunkSize => 1024;
        public int MaxBytesPerSecond => int.MaxValue;
        public Task ConnectAsync() => Task.CompletedTask;
        public void Close() => Interlocked.Exchange(ref connected, 0);
        public int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
        public void Write(byte[] buffer, int offset, int count) =>
            throw new IOException("simulated write failure");
    }

    private class ScriptedSocket : ISocketStream
    {
        private readonly byte[] input;
        private int position;
        private int connected = 1;
        public ScriptedSocket(byte[] input) => this.input = input;
        public bool Connected => Volatile.Read(ref connected) != 0;
        public string? Name => "scripted";
        public int MaxChunkSize => 1024;
        public int MaxBytesPerSecond => int.MaxValue;
        public Task ConnectAsync() => Task.CompletedTask;
        public virtual void Close() => Interlocked.Exchange(ref connected, 0);
        public int Read(byte[] buffer, int offset, int count)
        {
            if (position >= input.Length)
            {
                Interlocked.Exchange(ref connected, 0);
                return 0;
            }
            int length = Math.Min(count, input.Length - position);
            Array.Copy(input, position, buffer, offset, length);
            position += length;
            return length;
        }
        public void Write(byte[] buffer, int offset, int count) { }
    }

    private sealed class BlockingSocket : ISocketStream
    {
        private readonly ManualResetEventSlim closed = new ManualResetEventSlim(false);
        private int connected = 1;
        public ManualResetEventSlim ReadStarted { get; } = new ManualResetEventSlim(false);
        public bool Connected => Volatile.Read(ref connected) != 0;
        public string? Name => "blocking";
        public int MaxChunkSize => 1024;
        public int MaxBytesPerSecond => int.MaxValue;
        public Task ConnectAsync() => Task.CompletedTask;
        public void Close()
        {
            Interlocked.Exchange(ref connected, 0);
            closed.Set();
        }
        public int Read(byte[] buffer, int offset, int count)
        {
            ReadStarted.Set();
            closed.Wait();
            return 0;
        }
        public void Write(byte[] buffer, int offset, int count) { }
    }

    private sealed class FailingListener : ISocketListener
    {
        public void Start() => throw new IOException("simulated listener failure");
        public ISocketStream AcceptClient() => throw new NotSupportedException();
        public void Stop() { }
    }

    private sealed class OneShotListener : ISocketListener
    {
        private readonly ISocketStream socket;
        private readonly ManualResetEventSlim stopped = new ManualResetEventSlim(false);
        private int accepts;
        public bool WasStopped { get; private set; }

        public OneShotListener(ISocketStream socket) => this.socket = socket;
        public void Start() { }
        public ISocketStream AcceptClient()
        {
            if (Interlocked.Increment(ref accepts) == 1)
            {
                return socket;
            }
            stopped.Wait();
            throw new IOException("listener stopped");
        }
        public void Stop()
        {
            WasStopped = true;
            stopped.Set();
        }
    }
}

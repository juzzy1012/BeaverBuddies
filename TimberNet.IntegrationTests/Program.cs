using System.Net;
using System.Net.Sockets;
using Newtonsoft.Json.Linq;
using TimberNet;

internal static class Program
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static int Main()
    {
        TimberServer? server = null;
        TimberClient? firstClient = null;
        TimberClient? lateClient = null;
        TimberClient? duplicateLateClient = null;
        TimberClient? retriedLateClient = null;
        try
        {
            int port = FindAvailablePort();
            byte[] map = { 0x50, 0x4B, 0x03, 0x04, 1, 2, 3, 4 };
            int restartRequests = 0;

            server = new TimberServer(
                new TCPListenerWrapper(port),
                () => Task.FromResult(map),
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
            AssertEqual(
                TimberNetBase.SESSION_RESTART_REQUIRED,
                lateError,
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

            // Many automatic retries can hit the old server while the host is
            // loading. They must not schedule multiple saves/reloads.
            string? duplicateError = null;
            duplicateLateClient = ConnectClient(port, error => duplicateError = error);
            WaitUntil(
                () => Volatile.Read(ref duplicateError) != null,
                duplicateLateClient.Update,
                "duplicate retry did not receive the reload signal");
            AssertEqual(1, Volatile.Read(ref restartRequests),
                "duplicate coordinated reload request count");

            // If checkpoint creation fails, the host can release the guard and
            // the next retry is allowed to request another attempt.
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

            Console.WriteLine(
                "PASS: running sessions reject late joins, coalesce reload requests, " +
                "and notify existing clients without changing the deterministic hash.");
            return 0;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"FAIL: {exception}");
            return 1;
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

    private static void AssertEqual<T>(T expected, T actual, string description)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
        {
            throw new Exception($"{description}: expected {expected}, got {actual}");
        }
    }
}

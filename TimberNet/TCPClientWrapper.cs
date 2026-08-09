using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TimberNet
{
    public class TCPClientWrapper : ISocketStream
    {
        public readonly string? address;
        public readonly int port;
        private readonly TcpClient client;

        public int MaxChunkSize => 8192 * 4; // 32K
        // TCP already provides flow control and backpressure. Artificially
        // throttling save transfers here made LAN joining needlessly slow.
        public int MaxBytesPerSecond => int.MaxValue;

        public string? Name => null;

        public TCPClientWrapper(string address, int port) 
        {
            client = new TcpClient();
            this.address = address;
            this.port = port;
        }

        public TCPClientWrapper(TcpClient client)
        {
            this.client = client;
            ConfigureSocket();
            address = null;
            port = 0;
        }

        public bool Connected => client.Connected;


        public async Task ConnectAsync()
        {
            if (address == null)
            {
                throw new Exception("Client was initialized without an address.");
            }
            await client.ConnectAsync(address, port).ConfigureAwait(false);
            ConfigureSocket();
        }

        private void ConfigureSocket()
        {
            client.NoDelay = true;
            client.Client.SetSocketOption(
                SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        }

        public void Close()
        {
            try
            {
                client.GetStream().Close();
                client.Close();
            }
            catch { }
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            if (!client.Connected)
            {
                throw new InvalidOperationException("Client is not connected");
            }
            return client.GetStream().Read(buffer, offset, count);
        }

        public void Write(byte[] buffer, int offset, int count)
        {
            if (count > MaxChunkSize)
            {
                throw new ArgumentException($"Count {count} exceeds max chunk size {MaxChunkSize}");
            }
            if (!client.Connected)
            {
                throw new InvalidOperationException("Client is not connected");
            }
            client.GetStream().Write(buffer, offset, count);
        }
    }
}

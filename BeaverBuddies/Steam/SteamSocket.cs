using Steamworks;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Timberborn.BuildingsUI;
using Timberborn.Workshops;
using TimberNet;
using UnityEngine.PlayerLoop;

namespace BeaverBuddies.Steam
{
    public class SteamSocket : ISocketStream, ISteamPacketReceiver
    {
        // Steam can only buffer 1MB at a time, so we need to
        // leave time for that to clear out. Hopefully this is enough.
        // 1 MB per 8 seconds
        const int BYTES_PER_SECOND = 1024 * 1024 / 8;
        // Note this doesn't affect the theoretical issue of multiple
        // events being sent in a single packet - this could split them up
        // but that's fine. So we just choose a moderate size.
        const int MAX_CHUNK_SIZE = 1024 * 8; // 8KB

        public int MaxBytesPerSecond => BYTES_PER_SECOND;
        public int MaxChunkSize => MAX_CHUNK_SIZE;


        public string Name { get; private set; }

        public readonly CSteamID friendID;
        //public readonly CSteamID lobbyID;

        private readonly ConcurrentQueueWithWait<byte[]> readBuffer = new ConcurrentQueueWithWait<byte[]>();
        private readonly CancellationTokenSource closeSource = new CancellationTokenSource();
        private byte[] currentReadBuffer;
        private int readOffset = 0;
        private int connected;
        private int closed;
        private readonly bool outboundConnection;

        private SteamPacketListener packetListener;

        public SteamSocket(CSteamID friendID, bool autoconnect = false)
        {
            this.friendID = friendID;
            Name = SteamFriends.GetFriendPersonaName(friendID);
            connected = autoconnect ? 1 : 0;
            outboundConnection = !autoconnect;
            if (outboundConnection)
            {
                SteamOverlayConnectionService.PeerDisconnected += OnPeerDisconnected;
            }
        }

        public bool Connected => Volatile.Read(ref connected) != 0;

        public void RegisterSteamPacketListener(SteamPacketListener listener)
        {
            packetListener = listener;
            if (!listener.TryRegisterSocket(this))
            {
                packetListener = null;
                throw new InvalidOperationException(
                    $"A Steam connection for {friendID} is already registered.");
            }
        }

        public Task ConnectAsync()
        {
            if (Volatile.Read(ref closed) != 0)
            {
                throw new IOException("Steam connection is closed.");
            }
            // This is the client joining, and this only gets called when
            // we've already joined the lobby. It automatically closes
            // the prior client (I think).
            Interlocked.Exchange(ref connected, 1);
            Plugin.Log("SteamSocket requested to connect!");
            // A dedicated control-channel packet makes reconnects work even
            // after the host creates a replacement lobby. It causes Steam to
            // raise P2PSessionRequest_t on the host without entering TimberNet's
            // ordered byte stream on channel 0.
            byte[] probe = { 0x42, 0x42 };
            if (!SteamNetworking.SendP2PPacket(
                friendID, probe, (uint)probe.Length,
                EP2PSend.k_EP2PSendReliable, 1))
            {
                Interlocked.Exchange(ref connected, 0);
                throw new IOException("Steam rejected the connection probe.");
            }
            return Task.CompletedTask;
        }

        public void Close()
        {
            if (Interlocked.Exchange(ref closed, 1) != 0)
            {
                return;
            }
            Interlocked.Exchange(ref connected, 0);
            closeSource.Cancel();
            if (outboundConnection)
            {
                SteamOverlayConnectionService.PeerDisconnected -= OnPeerDisconnected;
            }
            packetListener?.UnregisterSocket(this);
            SteamNetworking.CloseP2PSessionWithUser(friendID);
        }

        private void OnPeerDisconnected(CSteamID remoteSteamID)
        {
            if (remoteSteamID == friendID)
            {
                Close();
            }
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            // Block until we've read something
            if (!Connected)
                throw new IOException("Steam connection is closed.");

            try
            {
                while (currentReadBuffer == null &&
                    !readBuffer.WaitAndTryDequeue(out currentReadBuffer, closeSource.Token)) { }
            }
            catch (OperationCanceledException)
            {
                throw new IOException("Steam connection was closed while reading.");
            }

            int bytesToCopy = Math.Min(count, currentReadBuffer.Length - readOffset);
            Array.Copy(currentReadBuffer, readOffset, buffer, offset, bytesToCopy);
            readOffset += bytesToCopy;
            if (readOffset == currentReadBuffer.Length)
            {
                currentReadBuffer = null;
                readOffset = 0;
            }
            return bytesToCopy;
        }

        public void Write(byte[] buffer, int offset, int count)
        {
            if (count > MaxChunkSize)
            {
                throw new IOException($"Attempted to write {buffer.Length} bytes, which exceeds the max chunk size of {MaxChunkSize} bytes.");
            }
            if (!Connected)
                throw new IOException("Steam connection is closed.");
            if (offset > 0)
            {
                // Make a copy to avoid modifying the caller's buffer
                byte[] newBuffer = new byte[count];
                Array.Copy(buffer, offset, newBuffer, 0, count);
                buffer = newBuffer;
            }
            Plugin.Log($"SteamSocket sending {count} bytes");
            if (!SteamNetworking.SendP2PPacket(
                friendID, buffer, (uint)count, EP2PSend.k_EP2PSendReliable))
            {
                throw new IOException("Steam rejected the outgoing packet.");
            }
        }

        public void ReceiveData(byte[] data)
        {
            if (Connected)
            {
                readBuffer.Enqueue(data);
            }
        }
    }
}

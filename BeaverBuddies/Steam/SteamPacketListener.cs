using Steamworks;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using TimberNet;

namespace BeaverBuddies.Steam
{
    public class SteamPacketListener
    {
        private Dictionary<CSteamID, SteamSocket> sockets = new Dictionary<CSteamID, SteamSocket>();
        private readonly object socketsLock = new object();

        public bool TryRegisterSocket(SteamSocket socket)
        {
            lock (socketsLock)
            {
                if (sockets.TryGetValue(socket.friendID, out SteamSocket existing) &&
                    existing.Connected)
                {
                    return ReferenceEquals(existing, socket);
                }
                sockets[socket.friendID] = socket;
                return true;
            }
        }

        public bool HasConnectedSocket(CSteamID remoteSteamID)
        {
            lock (socketsLock)
            {
                return sockets.TryGetValue(remoteSteamID, out SteamSocket socket) &&
                    socket.Connected;
            }
        }

        public void UnregisterSocket(SteamSocket socket)
        {
            lock (socketsLock)
            {
                if (sockets.TryGetValue(socket.friendID, out SteamSocket registered) &&
                    ReferenceEquals(registered, socket))
                {
                    sockets.Remove(socket.friendID);
                }
            }
        }

        public void CloseSocket(CSteamID remoteSteamID)
        {
            SteamSocket socket = null;
            lock (socketsLock)
            {
                sockets.TryGetValue(remoteSteamID, out socket);
            }
            socket?.Close();
        }

        public void CloseAllSockets()
        {
            List<SteamSocket> toClose;
            lock (socketsLock)
            {
                toClose = new List<SteamSocket>(sockets.Values);
            }
            foreach (SteamSocket socket in toClose)
            {
                socket.Close();
            }
        }

        public void Update()
        {
            uint messageSize;
            while (SteamNetworking.IsP2PPacketAvailable(out messageSize))
            {
                byte[] buffer = new byte[messageSize];
                uint bytesRead;

                CSteamID remoteSteamID;

                // Read the incoming packet
                if (SteamNetworking.ReadP2PPacket(buffer, messageSize, out bytesRead, out remoteSteamID))
                {
                    // Process the received data
                    //Plugin.Log($"Received {messageSize} bytes from: {remoteSteamID}");
                    //if (buffer.Length == 4)
                    //{
                    //    // This isn't being read correctly - likely an Endian issue
                    //    Plugin.Log("Length: " + BitConverter.ToInt32(buffer, 0));
                    //} else if (buffer.Length < 1000)
                    //{
                    //    Plugin.Log("Data: " + CompressionUtils.Decompress(buffer));
                    //}

                    SteamSocket socket;
                    lock (socketsLock)
                    {
                        sockets.TryGetValue(remoteSteamID, out socket);
                    }
                    if (socket != null)
                    {
                        socket.ReceiveData(buffer);
                    }
                    else
                    {
                        Plugin.LogWarning("Received message from unknown user: " + remoteSteamID);
                    }
                }
                else
                {
                    Plugin.LogWarning("Failed to read packet!");
                }
            }
        }
    }
}

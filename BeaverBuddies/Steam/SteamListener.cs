using Steamworks;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using TimberNet;

namespace BeaverBuddies.Steam
{
    public class SteamListener : ISocketListener, ISteamPacketReceiver
    {
        public CSteamID LobbyID { get; private set; }

        private List<IDisposable> callbacks = new List<IDisposable>();
        private ConcurrentQueueWithWait<SteamSocket> joiningUsers = new ConcurrentQueueWithWait<SteamSocket>();
        private readonly CancellationTokenSource stopSource = new CancellationTokenSource();
        private SteamPacketListener steamPacketListener;

        public SteamListener()
        {
            if (!SteamOverlayConnectionService.IsSteamEnabled)
            {
                throw new Exception("SteamListener created when Steam is not enabled!");
            }
        }

        public void RegisterSteamPacketListener(SteamPacketListener steamPacketListener)
        {
            this.steamPacketListener = steamPacketListener;
        }

        public void Start()
        {
            if (steamPacketListener == null)
            {
                throw new InvalidOperationException("SteamPacketListener must be registered before starting the SteamListener.");
            }
            Plugin.Log("SteamListener started...");
            callbacks.Add(Callback<LobbyChatUpdate_t>.Create(OnLobbyChatUpdate));
            callbacks.Add(Callback<LobbyCreated_t>.Create(OnLobbyCreated));
            callbacks.Add(Callback<P2PSessionRequest_t>.Create(OnP2PSessionRequest));
            SteamMatchmaking.CreateLobby(ELobbyType.k_ELobbyTypeFriendsOnly, 8);
        }

        private void OnLobbyCreated(LobbyCreated_t callback)
        {
            // Handle the callback
            if (callback.m_eResult == EResult.k_EResultOK)
            {
                // Lobby created successfully
                LobbyID = new CSteamID(callback.m_ulSteamIDLobby);
                // Friend only is the default; invisible means invite-only.
                var type = Settings.LobbyJoinable ? ELobbyType.k_ELobbyTypeFriendsOnly : ELobbyType.k_ELobbyTypeInvisible;
                SteamMatchmaking.SetLobbyType(LobbyID, type);
                Plugin.Log($"Lobby created with ID: {LobbyID} is joinable={Settings.LobbyJoinable}");
            }
            else
            {
                // Handle error
                Plugin.LogError("Failed to create lobby: " + callback.m_eResult);
            }
        }

        private void OnLobbyChatUpdate(LobbyChatUpdate_t callback)
        {
            Plugin.Log("Lobby chat update: " + callback.m_ulSteamIDLobby);
            if ((callback.m_rgfChatMemberStateChange & (uint)EChatMemberStateChange.k_EChatMemberStateChangeEntered) != 0)
            {
                CSteamID userJoined = new CSteamID(callback.m_ulSteamIDUserChanged);
                AcceptUser(userJoined);
                return;
            }

            uint departed =
                (uint)EChatMemberStateChange.k_EChatMemberStateChangeLeft |
                (uint)EChatMemberStateChange.k_EChatMemberStateChangeDisconnected |
                (uint)EChatMemberStateChange.k_EChatMemberStateChangeKicked |
                (uint)EChatMemberStateChange.k_EChatMemberStateChangeBanned;
            if ((callback.m_rgfChatMemberStateChange & departed) != 0)
            {
                steamPacketListener.CloseSocket(
                    new CSteamID(callback.m_ulSteamIDUserChanged));
            }
        }

        private void OnP2PSessionRequest(P2PSessionRequest_t callback)
        {
            SteamNetworking.AcceptP2PSessionWithUser(callback.m_steamIDRemote);
            AcceptUser(callback.m_steamIDRemote);
        }

        private void AcceptUser(CSteamID user)
        {
            if (user == SteamUser.GetSteamID() ||
                steamPacketListener.HasConnectedSocket(user))
            {
                return;
            }

            var socket = new SteamSocket(user, true);
            socket.RegisterSteamPacketListener(steamPacketListener);
            joiningUsers.Enqueue(socket);
        }

        public ISocketStream AcceptClient()
        {
            Plugin.Log("Waiting to accept a client...");
            SteamSocket socket;
            while (!joiningUsers.WaitAndTryDequeue(out socket, stopSource.Token)) { }
            Plugin.Log("New client accepted!");
            return socket;
        }

        public void Stop()
        {
            Plugin.Log("Stopping SteamListener...");
            stopSource.Cancel();
            steamPacketListener?.CloseAllSockets();
            SteamMatchmaking.LeaveLobby(LobbyID);
            foreach (IDisposable callback in callbacks)
            {
                callback.Dispose();
            }
        }

        public void ShowInviteFriendsPanel()
        {
            SteamFriends.ActivateGameOverlayInviteDialog(LobbyID);
        }
    }
}

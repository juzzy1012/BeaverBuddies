using HarmonyLib;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Timberborn.GameSaveRepositorySystem;
using Timberborn.GameSaveRepositorySystemUI;
using Timberborn.MainMenuScene;
using Timberborn.Localization;
using UnityEngine.UIElements;
using System.Threading.Tasks;
using System.Runtime.InteropServices.ComTypes;
using Newtonsoft.Json.Bson;
using Timberborn.GameSceneLoading;
using BeaverBuddies.IO;
using BeaverBuddies.Util;
using Timberborn.CoreUI;
using System.Collections;
using BeaverBuddies.Steam;
using TimberNet;
using UnityEngine;
using Timberborn.SceneLoading;
using Timberborn.Common;

namespace BeaverBuddies.Connect
{

    [HarmonyPatch(typeof(LoadGameBox), nameof(LoadGameBox.GetPanel))]
    public class LoadGameBoxGetPanelPatcher
    {
        public static void Postfix(LoadGameBox __instance, ref VisualElement __result)
        {
            ILoc _loc = __instance._loc;
            ButtonInserter.DuplicateOrGetButton(__result, "LoadButton", "HostButton", (button) =>
            {
               button.text = _loc.T("BeaverBuddies.Saving.HostCoopGame");
               button.clicked += () => HostSelectedGame(__instance);
            });
        }

        [ManualMethodOverwrite]
        /*
         * 04/19/2025
        if (_saveList.TryGetSelectedSave(out var selectedSave))
        {
            if (_gameSaveRepository.SaveExists(selectedSave.SaveReference))
            {
                _validatingGameLoader.LoadGameIfSaveValid(selectedSave.SaveReference);
                return true;
            }

            Debug.LogWarning("Save: " + selectedSave.DisplayName + " doesn't exist, failed to load.");
        }
        return false;
         */
        // Duplicates the LoadGameBox.LoadGame method, but loads the game with
        // the HostingSaveReference instead of ValidatingGameLoader
        private static void HostSelectedGame(LoadGameBox __instance)
        {
            if (__instance._saveList.TryGetSelectedSave(out var selectedSave))
            {
                if (__instance._gameSaveRepository.SaveExists(selectedSave.SaveReference))
                {
                    ServerHostingUtils.LoadIfSaveValidAndHost(__instance._validatingGameLoader, __instance._dialogBoxShower, selectedSave.SaveReference);
                }
                // Debug.LogWarning("Save: " + selectedSave.DisplayName + " doesn't exist, failed to load.");
            }
        }
    }

    internal class ServerHostingUtils
    {
        public static void LoadIfSaveValidAndHost(ValidatingGameLoader loader, DialogBoxShower shower,
            SaveReference saveReferece, int minimumReadyClients = 0)
        {
            CheckNextValidator(loader, shower, saveReferece, 0, minimumReadyClients);
        }

        [ManualMethodOverwrite]
        /*
        04/19/2025 ValidatingGameLoader.CheckNextValidator
        if (index >= _gameLoadValidators.Length)
        {
	        _gameSceneLoader.StartSaveGame(saveReference);
	        return;
        }
        _gameLoadValidators[index].ValidateSave(saveReference, delegate
        {
	        CheckNextValidator(saveReference, index + 1);
        });
         */
        private static void CheckNextValidator(ValidatingGameLoader loader, DialogBoxShower shower,
            SaveReference saveReference, int index, int minimumReadyClients)
        {
            if (index >= loader._gameLoadValidators.Length)
            {
                LoadAndHost(loader, shower, saveReference, minimumReadyClients);
                return;
            }
            loader._gameLoadValidators[index].ValidateSave(saveReference, delegate
            {
                CheckNextValidator(loader, shower, saveReference, index + 1, minimumReadyClients);
            });
        }

        private static IEnumerator UpdateDialogBox(DialogBox box, ServerEventIO io, ILoc loc,
            Action startWhenReady, int minimumReadyClients)
        {
            var label = box._root.Q<Label>("Message");
            string baseMessage = loc.T("BeaverBuddies.Host.ConnectedClients");
            while (true)
            {
                // ReplayService does not exist while the host is in this lobby,
                // so the lobby must process readiness acknowledgements itself.
                io.Update();
                List<string> clients = io.NetBase.GetConnectedClients();
                string content = baseMessage;
                int nonLocalID = 1;
                foreach (string client in clients)
                {
                    string name = client ?? $"{loc.T("BeaverBuddies.Host.DirectConnectClient")} ({nonLocalID++})";
                    content += $"\n* {name}";
                }
                label.text = content;

                if (minimumReadyClients > 0 && io.AreAllClientsReady)
                {
                    Plugin.Log($"All {minimumReadyClients} clients loaded the checkpoint; " +
                        "resuming the host automatically");
                    startWhenReady();
                    yield break;
                }
                yield return 0;
            }
        }

        public static byte[] GetMapBtyes(GameSaveRepository repository, SaveReference saveReference)
        {
            var inputStream = repository.OpenSaveWithoutLogging(saveReference);
            byte[] data;
            using (var memoryStream = new MemoryStream())
            {
                inputStream.CopyTo(memoryStream);
                data = memoryStream.ToArray();
            }
            inputStream.Close();
            return data;
        }

        public static MonoBehaviour GetMonoBehaviour(ISceneLoader loader)
        {
            return ((SceneLoader)loader)._coroutineStarter._monoBehaviour;
        }

        public static void LoadAndHost(ValidatingGameLoader loader, DialogBoxShower shower,
            SaveReference saveReference, int minimumReadyClients = 0)
        {
            var sceneLoader = loader._gameSceneLoader;
            var repository = sceneLoader._gameSaveRepository;
            byte[] data = GetMapBtyes(repository, saveReference);
            Plugin.Log($"Reading map with length {data.Length}");

            ServerEventIO io = new ServerEventIO();
            EventIO.Set(io);
            if (!io.Start(data, minimumReadyClients))
            {
                EventIO.Reset();
                shower.Create()
                    .SetMessage("Could not start the multiplayer server. " +
                        "Check that the configured port is available, then try again.")
                    .SetDefaultCancelButton()
                    .Show();
                return;
            }

            var behavior = GetMonoBehaviour(sceneLoader._sceneLoader);
            Coroutine coroutine = null;
            bool isStarting = false;

            var socketListener = io.SocketListener;
            SteamListener steamListener = socketListener as SteamListener;
            if (socketListener is MultiSocketListener)
            {
                steamListener = ((MultiSocketListener)socketListener).GetListener<SteamListener>();
            }
            Plugin.Log($"Steam listener: {steamListener}");

            var loc = shower._loc;
            Action<bool> startGame = allowMissingClients =>
            {
                if (isStarting)
                {
                    return;
                }
                isStarting = true;

                if (allowMissingClients && minimumReadyClients > 0)
                {
                    // The normal button remains a deliberate escape hatch if a
                    // previous participant has left during the coordinated reload.
                    io.AllowStartingWithConnectedClients();
                }
                if (coroutine != null)
                {
                    behavior.StopCoroutine(coroutine);
                }

                // Make sure to set the RNG seed before loading the map.
                // The client does the same.
                DeterminismService.InitGameStartState(data);
                sceneLoader.StartSaveGame(saveReference);
            };

            var boxCreator = shower.Create()
                .SetMessage("")
                .SetConfirmButton(() => startGame(true), loc.T("BeaverBuddies.Host.StartGame"))
                .SetCancelButton(() =>
                {
                    if (coroutine != null)
                    {
                        behavior.StopCoroutine(coroutine);
                    }
                    io.Close();
                });
            if (steamListener != null)
            {
                boxCreator.SetInfoButton(() =>
                {
                    steamListener.ShowInviteFriendsPanel();
                }, loc.T("BeaverBuddies.Host.InviteFriends"));
            }
            boxCreator.SetDefaultCancelButton(loc.T(CommonLocKeys.CancelKey));

            DialogBox box = boxCreator.Show();
            coroutine = behavior.StartCoroutine(UpdateDialogBox(
                box, io, shower._loc, () => startGame(false), minimumReadyClients));

        }
    }
}

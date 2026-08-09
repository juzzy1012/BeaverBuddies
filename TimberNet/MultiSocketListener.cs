using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TimberNet
{
    public class MultiSocketListener : ISocketListener
    {
        private readonly List<ISocketListener> listeners = new List<ISocketListener>();
        private readonly List<ISocketListener> activeListeners = new List<ISocketListener>();
        private readonly List<Exception> startFailures = new List<Exception>();
        private readonly object lifecycleLock = new object();

        private readonly ConcurrentQueueWithWait<ISocketStream> accepted = new ConcurrentQueueWithWait<ISocketStream>();
        private readonly CancellationTokenSource stopSource = new CancellationTokenSource();
        private bool isAccepting = false;
        private int isStopped;

        public IEnumerable<ISocketListener> Listeners => listeners;
        public IEnumerable<Exception> StartFailures => startFailures;

        public MultiSocketListener(params ISocketListener[] listeners) 
        {
            this.listeners.AddRange(listeners);
        }

        public ISocketStream AcceptClient()
        {
            if (!isAccepting)
            {
                StartAccepting();
                isAccepting = true;
            }
            accepted.WaitAndTryDequeue(out ISocketStream socket, stopSource.Token);
            return socket;
        }

        private void StartAccepting()
        {
            List<ISocketListener> snapshot;
            lock (lifecycleLock)
            {
                snapshot = new List<ISocketListener>(activeListeners);
            }
            foreach (var listener in snapshot)
            {
                Task.Run(() =>
                {
                    while (Volatile.Read(ref isStopped) == 0)
                    {
                        try
                        {
                            accepted.Enqueue(listener.AcceptClient());
                        }
                        catch when (Volatile.Read(ref isStopped) != 0)
                        {
                            break;
                        }
                        catch
                        {
                            // A transient listener failure should not permanently
                            // disable the other transport or the accept loop.
                            Thread.Sleep(100);
                        }
                    }
                });
            }
        }

        public void Start()
        {
            lock (lifecycleLock)
            {
                if (Volatile.Read(ref isStopped) != 0)
                    throw new InvalidOperationException("Listener has already been stopped.");
                if (activeListeners.Count > 0)
                    return;

                startFailures.Clear();
                foreach (ISocketListener listener in listeners)
                {
                    try
                    {
                        listener.Start();
                        activeListeners.Add(listener);
                    }
                    catch (Exception exception)
                    {
                        startFailures.Add(exception);
                    }
                }

                if (activeListeners.Count == 0)
                {
                    throw new AggregateException(
                        "None of the configured transports could start.", startFailures);
                }
            }
        }

        public void Stop()
        {
            Interlocked.Exchange(ref isStopped, 1);
            stopSource.Cancel();
            List<ISocketListener> snapshot;
            lock (lifecycleLock)
            {
                snapshot = new List<ISocketListener>(activeListeners);
                activeListeners.Clear();
            }
            foreach (ISocketListener listener in snapshot)
            {
                try
                {
                    listener.Stop();
                }
                catch
                {
                    // Continue stopping the other transport. A broken Steam
                    // listener must not leave the TCP accept thread alive.
                }
            }
        }

        public T GetListener<T>()
        {
            lock (lifecycleLock)
            {
                return (T)activeListeners.Find(listener => listener is T);
            }
        }
    }
}

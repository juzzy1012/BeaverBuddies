using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace TimberNet
{
    public class ConcurrentQueueWithWait<T>
    {
        private ConcurrentQueue<T> queue;
        private readonly ManualResetEventSlim hasAvailable = new ManualResetEventSlim(false);

        public ConcurrentQueueWithWait()
        {
            queue = new ConcurrentQueue<T>();
        }

        public void Enqueue(T item)
        {
            queue.Enqueue(item);
            hasAvailable.Set();
        }

        public bool WaitAndTryDequeue(out T item,
            CancellationToken cancellationToken = default)
        {
            hasAvailable.Wait(cancellationToken);
            if (queue.TryDequeue(out item))
            {
                hasAvailable.Reset();
                if (!queue.IsEmpty)
                {
                    hasAvailable.Set();
                }
                return true;
            }
            return false;
        }

    }
}

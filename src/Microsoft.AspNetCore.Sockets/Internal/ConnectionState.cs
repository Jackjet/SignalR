// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.AspNetCore.Sockets.Internal
{
    public class ConnectionState
    {
        public Connection Connection { get; set; }
        public IChannelConnection<Message> Application { get; }

        public CancellationTokenSource Cancellation { get; set; }

        public SemaphoreSlim Lock { get; } = new SemaphoreSlim(1, 1);

        public string RequestId { get; set; }

        public Task TransportTask { get; set; }
        public Task ApplicationTask { get; set; }

        public DateTime LastSeenUtc { get; set; }
        public State Status { get; set; } = State.Inactive;

        public ConnectionState(Connection connection, IChannelConnection<Message> application)
        {
            Connection = connection;
            Application = application;
            LastSeenUtc = DateTime.UtcNow;
        }

        public async Task DisposeAsync()
        {
            try
            {
                await Lock.WaitAsync();

                if (Status == State.Disposed)
                {
                    return;
                }

                Status = State.Disposed;

                RequestId = null;

                // If the application task is faulted, propagate the error to the transport
                if (ApplicationTask.IsFaulted)
                {
                    Connection.Transport.Output.TryComplete(ApplicationTask.Exception.InnerException);
                }

                // If the transport task is faulted, propagate the error to the application
                if (TransportTask.IsFaulted)
                {
                    Application.Output.TryComplete(TransportTask.Exception.InnerException);
                }

                Connection.Dispose();
                Application.Dispose();

                // REVIEW: Add a timeout so we don't wait forever
                await Task.WhenAll(ApplicationTask, TransportTask);
            }
            finally
            {
                Lock.Release();
            }
        }

        public enum State
        {
            Inactive,
            Active,
            Disposed
        }
    }
}

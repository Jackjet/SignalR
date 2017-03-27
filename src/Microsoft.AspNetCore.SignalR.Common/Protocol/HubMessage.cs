﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Microsoft.AspNetCore.SignalR.Protocol
{
    public abstract class HubMessage
    {
        public long InvocationId { get; }

        protected HubMessage(long invocationId)
        {
            InvocationId = invocationId;
        }
    }
}
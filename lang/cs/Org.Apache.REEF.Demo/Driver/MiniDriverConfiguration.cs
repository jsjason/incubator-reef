// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Diagnostics;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Evaluator.DriverConnectionConfigurationProviders;
using Org.Apache.REEF.Common.Evaluator.Parameters;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Demo.Driver
{
    /// <summary>
    /// Fill this out to configure a Driver.
    /// </summary>
    [ClientSide]
    public sealed class MiniDriverConfiguration : ConfigurationModuleBuilder
    {
        public static readonly RequiredImpl<IObserver<IMiniDriverStarted>> OnDriverStarted = new RequiredImpl<IObserver<IMiniDriverStarted>>();

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new MiniDriverConfiguration()
                    .BindSetEntry(GenericType<MiniDriverNamedParameters.MiniDriverStartedHandlers>.Class, OnDriverStarted)
                    .Build();
            }
        }
    }
}

﻿// Licensed to the Apache Software Foundation (ASF) under one
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

using System.Collections.Generic;
using System.IO;
using Org.Apache.REEF.Driver.Context;

namespace Org.Apache.REEF.Demo.Stage
{
    public class PartitionInfo
    {
        private readonly string _id;
        private readonly IList<IActiveContext> _loadedContexts;

        public PartitionInfo(string id, params IActiveContext[] loadedContexts)
        {
            Stream
            _id = id;
            _loadedContexts = new List<IActiveContext>(loadedContexts);
        }

        /// <summary>
        /// String identifier of this partition.
        /// </summary>
        public string Id
        {
            get { return _id; }
        }

        /// <summary>
        /// LoadedContexts of the blocks that belong to this partition.
        /// </summary>
        public IList<IActiveContext> LoadedContexts
        {
            get { return _loadedContexts; }
        }
    }
}

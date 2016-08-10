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

using System;
using System.Collections.Generic;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Examples
{
    /// <summary>
    /// Demonstration of how partitions and blocks can be fetched from the dataset layer.
    /// </summary>
    public sealed class SampleTask : ITask
    {
        private readonly IDataSetManager _dataSetManager;
        private readonly ISet<string> _partitionIds;

        [Inject]
        private SampleTask(IDataSetManager dataSetManager,
                           [Parameter(typeof(PartitionIds))] ISet<string> partitionIds)
        {
            _dataSetManager = dataSetManager;
            _partitionIds = partitionIds;
        }

        public byte[] Call(byte[] memento)
        {
            // This does not necessarily have to be done at the task; doing this in a context (service) may be better.
            foreach (string partitionId in _partitionIds)
            {
                IInputPartition<byte[]> partition = _dataSetManager.FetchPartition<byte[]>(partitionId, partitionId);

                // do something with the partition data.. deserialization needed
                // e.g. IInputPartition<byte[]> -> IInputPartition<Vector>
                Console.WriteLine(partition.GetPartitionHandle());
            }

            return null;
        }

        public void Dispose()
        {
        }
    }
}

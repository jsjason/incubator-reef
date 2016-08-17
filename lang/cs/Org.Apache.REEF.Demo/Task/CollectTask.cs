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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Demo.Driver;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Task
{
    public sealed class CollectTask<T> : ITask
    {
        private readonly DataSetManager _dataSetManager;
        private readonly string _oldDataSetId;

        [Inject]
        private CollectTask(DataSetManager dataSetManager,
                            [Parameter(typeof(OldDataSetIdNamedParameter))] string oldDataSetId,
                            )
        {
            _dataSetManager = dataSetManager;
            _oldDataSetId = oldDataSetId;
        }

        public byte[] Call(byte[] memento)
        {
            Console.WriteLine(this + " " + _oldDataSetId);
            foreach (IInputPartition<T> partition in _dataSetManager.GetLocalPartitions<T>(_oldDataSetId))
            {
                
            }

            return null;
        }

        public void Dispose()
        {
        }
    }
}

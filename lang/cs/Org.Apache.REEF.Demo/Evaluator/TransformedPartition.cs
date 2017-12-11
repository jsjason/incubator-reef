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
using Org.Apache.REEF.IO.PartitionedData;

namespace Org.Apache.REEF.Demo.Evaluator
{
    internal sealed class TransformedPartition<T1, T2> : IInputPartition<T2>
    {
        private readonly string _partitionId;
        private readonly ITransform<T1, T2> _transform;
        private readonly IInputPartition<T1> _inputPartition;

        internal TransformedPartition(string partitionId,
                                      ITransform<T1, T2> transform,
                                      IInputPartition<T1> inputPartition)
        {
            _partitionId = partitionId;
            _transform = transform;
            _inputPartition = inputPartition;
        }

        public string Id
        {
            get { return _partitionId; }
        }

        public void Cache()
        {
            throw new NotImplementedException();
        }

        public T2 GetPartitionHandle()
        {
            return _transform.Apply(_inputPartition.GetPartitionHandle());
        }
    }
}

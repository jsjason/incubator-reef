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
using System.Collections.Concurrent;
using System.Collections.Generic;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Driver
{
    internal sealed class PartitionCollector : IObserver<IContextMessage>
    {
        private readonly ResultCodec _resultCodec;

        private readonly ConcurrentDictionary<string, SynchronizedCollection<Tuple<string, string>>>
            _partitionDictionary;

        [Inject]
        private PartitionCollector(ResultCodec resultCodec)
        {
            _resultCodec = resultCodec;
            _partitionDictionary = new ConcurrentDictionary<string, SynchronizedCollection<Tuple<string, string>>>();
        }

        public void OnNext(IContextMessage msg)
        {
            string contextId = msg.MessageSourceId;
            var partitionsOnContext = _partitionDictionary.GetOrAdd(contextId, _ => new SynchronizedCollection<Tuple<string, string>>());

            foreach (var tuple in _resultCodec.Decode(msg.Message))
            {
                partitionsOnContext.Add(tuple);
            }
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception e)
        {
            throw e;
        }

        internal ConcurrentDictionary<string, SynchronizedCollection<Tuple<string, string>>> PartitionDictionary
        {
            get { return _partitionDictionary; }
        }
    }
}

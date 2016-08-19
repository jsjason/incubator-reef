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
using System.Collections.Generic;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Demo.Task
{
    public sealed class PartitionReporter : IContextMessageSource
    {
        private readonly string _contextId;
        private readonly ResultCodec _resultCodec; 
        private readonly IList<Tuple<string, string>> _newPartitions;
        
        [Inject]
        private PartitionReporter([Parameter(typeof(ContextConfigurationOptions.ContextIdentifier))] string contextId,
                                  ResultCodec resultCodec)
        {
            _contextId = contextId;
            _resultCodec = resultCodec;
            _newPartitions = new List<Tuple<string, string>>();
        }

        public void NewPartition(string dataSetId, string partitionId)
        {
            lock (_newPartitions)
            {
                _newPartitions.Add(new Tuple<string, string>(dataSetId, partitionId));
            }
        }

        public Optional<ContextMessage> Message
        {
            get
            {
                lock (_newPartitions)
                {
                    if (_newPartitions.Count == 0)
                    {
                        return Optional<ContextMessage>.Empty();
                    }
                    else
                    {
                        var message = Optional<ContextMessage>.Of(ContextMessage.From(_contextId, _resultCodec.Encode(_newPartitions)));
                        _newPartitions.Clear();
                        return message;
                    }
                }
            }
        }
    }
}

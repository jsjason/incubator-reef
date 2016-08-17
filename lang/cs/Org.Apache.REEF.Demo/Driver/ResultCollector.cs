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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Driver
{
    internal sealed class ResultCollector : IObserver<IContextMessage>
    {
        private readonly ResultCodec _resultCodec;
        private readonly Guid _guid = Guid.NewGuid();

        [Inject]
        private ResultCollector(ResultCodec resultCodec)
        {
            _resultCodec = resultCodec;
            Console.WriteLine(_guid);
        }

        public void OnNext(IContextMessage msg)
        {
            string contextId = msg.MessageSourceId;
            Console.WriteLine(contextId);
            foreach (var tuple in _resultCodec.Decode(msg.Message))
            {
                Console.WriteLine(tuple.Item1 + " *** " + tuple.Item2);
            }
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception e)
        {
            throw e;
        }
    }
}

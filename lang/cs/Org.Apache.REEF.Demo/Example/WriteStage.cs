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
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Demo.Driver;
using Org.Apache.REEF.Demo.Evaluator;
using Org.Apache.REEF.Demo.Stage;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Demo.Example
{
    internal sealed class WriteStage : IObserver<IStageDriverStarted>, IObserver<IStageDriverCompletedTask>
    {
        private readonly IInjectionFuture<StageRunner> _stageRunner;
        private readonly string _dataSetId;
        private readonly CountdownEvent _countdownEvent = new CountdownEvent(0);

        [Inject]
        private WriteStage(IInjectionFuture<StageRunner> stageRunner,
                           [Parameter(typeof(OldDataSetIdNamedParameter))] string dataSetId)
        {
            _stageRunner = stageRunner;
            _dataSetId = dataSetId;
        }

        public void OnNext(IStageDriverStarted stageDriverStarted)
        {
            IConfiguration dataSetIdConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter(typeof(OldDataSetIdNamedParameter), _dataSetId)
                .Build();

            ISet<IActiveContext> activeContexts = new HashSet<IActiveContext>();
            foreach (var partitionInfo in stageDriverStarted.DataSetInfo.PartitionInfos)
            {
                partitionInfo.LoadedContexts.ForEach(context => activeContexts.Add(context));
            }

            _countdownEvent.Reset(activeContexts.Count);

            foreach (IActiveContext activeContext in activeContexts)
            {
                IConfiguration taskConf = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, "WriteTask-" + activeContext.Id)
                    .Set(TaskConfiguration.Task, GenericType<WriteTask>.Class)
                    .Build();

                activeContext.SubmitTask(Configurations.Merge(taskConf, dataSetIdConf));
            }

            Thread.Sleep(20000);
            _stageRunner.Get().EndStage();
        }

        public void OnNext(IStageDriverCompletedTask stageDriverCompletedTask)
        {
            _countdownEvent.Signal();
        }

        public void OnError(Exception e)
        {
        }

        public void OnCompleted()
        {
        }
    }
}

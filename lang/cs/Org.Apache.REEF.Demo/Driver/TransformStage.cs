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
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Demo.Driver
{
    internal sealed class TransformStage<T1, T2> : IObserver<IMiniDriverStarted>
    {
        private readonly IInjectionFuture<StageRunner> _stageRunner;
        private readonly IConfiguration _transformConf;
        private readonly string _oldDataSetId;
        private readonly string _newDataSetId;

        [Inject]
        private TransformStage(IInjectionFuture<StageRunner> stageRunner,
                               [Parameter(typeof(SerializedTransformConfiguration))] string serializedTransformConf,
                               [Parameter(typeof(OldDataSetIdNamedParameter))] string oldDataSetId,
                               [Parameter(typeof(NewDataSetIdNamedParameter))] string newDataSetId,
                               AvroConfigurationSerializer avroConfigurationSerializer)
        {
            _stageRunner = stageRunner;
            _transformConf = avroConfigurationSerializer.FromString(serializedTransformConf);
            _oldDataSetId = oldDataSetId;
            _newDataSetId = newDataSetId;
        }

        public void OnNext(IMiniDriverStarted miniDriverStarted)
        {
            IConfiguration dataSetIdConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter(typeof(OldDataSetIdNamedParameter), _oldDataSetId)
                .BindNamedParameter(typeof(NewDataSetIdNamedParameter), _newDataSetId)
                .Build();

            ISet<IActiveContext> activeContexts = new HashSet<IActiveContext>();
            foreach (var partitionInfo in miniDriverStarted.DataSetInfo.PartitionInfos)
            {
                partitionInfo.LoadedContexts.ForEach(context => activeContexts.Add(context));
            }

            foreach (IActiveContext activeContext in activeContexts)
            {
                IConfiguration taskConf = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, "TransformTask-" + activeContext.Id)
                    .Set(TaskConfiguration.Task, GenericType<TransformTask<T1, T2>>.Class)
                    .Build();
                activeContext.SubmitTask(Configurations.Merge(taskConf, _transformConf, dataSetIdConf));
            }

            Thread.Sleep(5000);
            _stageRunner.Get().EndStage();
        }

        public void OnError(Exception e)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
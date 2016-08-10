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
using Org.Apache.REEF.Demo.Stage;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Demo.Driver
{
    public class DataSet<T> : IDataSet<T>
    {
        private readonly string _id;
        private readonly DataSetInfo _dataSetInfo;

        public DataSet(string id,
                       DataSetInfo dataSetInfo)
        {
            _id = id;
            _dataSetInfo = dataSetInfo;
        }

        public string Id
        {
            get { return _id; }
        }

        public IDataSet<T2> TransformPartitions<T2>(IConfiguration transformConf)
        {
            IInjector injector = TangFactory.GetTang().NewInjector(transformConf);
            if (!injector.IsInjectable(typeof(ITransform<T, T2>)))
            {
                throw new Exception("Given configuration does not contain the correct ITransform configuration.");
            }

            IConfiguration stageConf = MiniDriverConfiguration.ConfigurationModule
                .Set(MiniDriverConfiguration.OnDriverStarted, GenericType<TransformStage>.Class)
                .Build();

            return RunStage<T2>(Configurations.Merge(transformConf, stageConf));
        }

        public IDataSet<T2> RunStage<T2>(IConfiguration stageConf)
        {
            IInjector injector = TangFactory.GetTang().NewInjector(stageConf);
            injector.BindVolatileInstance(GenericType<DataSetInfo>.Class, _dataSetInfo);
            
            StageRunner stageRunner = injector.GetInstance<StageRunner>();
            stageRunner.StartStage();
            stageRunner.AwaitStage();
            return null; // retrieve IDataSet<T2> somehow
        }

        public IEnumerable<T> Collect()
        {
            throw new NotImplementedException();
        }
    }
}

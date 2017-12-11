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
using Org.Apache.REEF.Demo.Driver;
using Org.Apache.REEF.Demo.Evaluator;
using Org.Apache.REEF.Demo.Stage;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Demo.Example
{
    public class DemoDriver : IObserver<IDriverStarted>
    {
        private readonly IDataSetMaster _dataSetMaster;
        private readonly Uri _dataSetUri;

        [Inject]
        private DemoDriver(IDataSetMaster dataSetMaster,
                           [Parameter(typeof(DataSetUri))] string dataSetUri)
        {
            _dataSetMaster = dataSetMaster;
            _dataSetUri = ToUri(dataSetUri);
        }

        public void OnNext(IDriverStarted driverStarted)
        {
            IDataSet<LabelVector[]> dataSet = _dataSetMaster.Load(_dataSetUri)
                .TransformPartitions<LabelNumTxt[], TextToLabelNumTxt>()
                .TransformPartitions<LabelVectorTxt[], NumToVector>()
                .TransformPartitions<LabelVectorVector[], TextToVector>()
                .TransformPartitions<LabelVector[], VectorConcat>();

            dataSet.RunStage<object>(StageDriverConfiguration.ConfigurationModule
                .Set(StageDriverConfiguration.OnDriverStarted, GenericType<WriteStage>.Class)
                .Set(StageDriverConfiguration.OnTaskCompleted, GenericType<WriteStage>.Class)
                .Build());

            dataSet.Collect();
        }

        public void OnError(Exception e)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        private static Uri ToUri(string uriString)
        {
            return new Uri(uriString);
        }
    }
}

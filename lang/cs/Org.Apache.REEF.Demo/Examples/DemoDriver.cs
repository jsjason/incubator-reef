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
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Demo.Examples
{
    public class DemoDriver : IObserver<IDriverStarted>
    {
        private static readonly Logger LOG = Logger.GetLogger(typeof(DemoDriver));

        private readonly IDataSetMaster _dataSetMaster;
        private readonly Uri _dataSetUri;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        private DemoDriver(IDataSetMaster dataSetMaster,
                           [Parameter(typeof(DataSetUri))] string dataSetUriString,
                           IEvaluatorRequestor evaluatorRequestor)
        {
            _dataSetMaster = dataSetMaster;
            _dataSetUri = ToUri(dataSetUriString);
            _evaluatorRequestor = evaluatorRequestor;
        }

        public void OnNext(IDriverStarted driverStarted)
        {
            LOG.Log(Level.Info, "Start dataset loading.");

            // this will spawn evaluators loaded with blocks
            // the IDataSetMaster implementations will need to have OnNext(IAllocatedEvaluator) and OnNext(IActiveContext) handlers
            IDataSet<byte[]> dataSet = _dataSetMaster.Load<byte[]>(_dataSetUri);

            LOG.Log(Level.Info, "Done loading dataset.");

            // MiniDriver configuration
            // information on which block is on which evaluator will be given
            // IConfiguration conf = TangFactory.GetTang().NewConfigurationBuilder().Build();

            // IDataSet<AnotherSerializableClass> anotherDataSet = _dataSet.RunStage<AnotherSerializableClass>(conf);

            // store the new dataset/model
            // _dataSetMaster.Store(anotherDataSet);
            IConfiguration transformConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<ITransform<byte[], string>>.Class, GenericType<ByteToStringTransform>.Class)
                .Build();
            IDataSet<string> dataSet2 = dataSet.TransformPartitions<string>(transformConf);

            // IConfiguration anotherTransformConf = TangFactory.GetTang().NewConfigurationBuilder()
            //    .BindImplementation(GenericType<ITransform<string, string>>.Class, GenericType<StringDuplicateTransform>.Class)
            //    .Build();
            // IDataSet<string> dataSet3 = dataSet2.TransformPartitions<string>(anotherTransformConf);
            // dataSet3.Collect();
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception e)
        {
        }

        public static Uri ToUri(string uriString)
        {
            return new Uri(uriString);
        }
    }
}

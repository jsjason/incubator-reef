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
using System.Linq;
using Org.Apache.REEF.Demo.Stage;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Demo.Driver
{
    public class DataSet<T> : IDataSet<T>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DataSet<T>)); 

        private readonly string _id;
        private readonly DataSetInfo _dataSetInfo;
        private readonly PartitionCollector _partitionCollector;
        private readonly AvroConfigurationSerializer _avroConfigurationSerializer = new AvroConfigurationSerializer();

        internal DataSet(string id,
                         DataSetInfo dataSetInfo,
                         PartitionCollector partitionCollector)
        {
            _id = id;
            _dataSetInfo = dataSetInfo;
            _partitionCollector = partitionCollector;
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
                Exceptions.Throw(new Exception("Given configuration does not contain the correct ITransform configuration."), Logger);
            }

            IConfiguration serializedTransformConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter(typeof(SerializedTransformConfiguration), _avroConfigurationSerializer.ToString(transformConf))
                .Build();

            IConfiguration stageConf = MiniDriverConfiguration.ConfigurationModule
                .Set(MiniDriverConfiguration.OnDriverStarted, GenericType<TransformStage<T, T2>>.Class)
                .Build();

            return RunStage<T2>(Configurations.Merge(serializedTransformConf, stageConf));
        }

        public IDataSet<T2> RunStage<T2>(IConfiguration stageConf)
        {
            string newDataSetId = _id + "-Transformed";

            IInjector injector = TangFactory.GetTang().NewInjector(stageConf);
            injector.BindVolatileInstance(GenericType<DataSetInfo>.Class, _dataSetInfo);
            injector.BindVolatileParameter(GenericType<OldDataSetIdNamedParameter>.Class, _id);
            injector.BindVolatileParameter(GenericType<NewDataSetIdNamedParameter>.Class, newDataSetId);
            
            StageRunner stageRunner = injector.GetInstance<StageRunner>();
            stageRunner.StartStage();
            stageRunner.AwaitStage();

            IDictionary<string, ISet<IActiveContext>> partitions = new Dictionary<string, ISet<IActiveContext>>();
            var contextDictionary = FormContextDictionary();
            foreach (var keyValue in _partitionCollector.PartitionDictionary)
            {
                string contextId = keyValue.Key;
                foreach (var dataSetAndPartitionIds in keyValue.Value)
                {
                    if (!dataSetAndPartitionIds.Item1.Equals(newDataSetId))
                    {
                        continue;
                    }
                    
                    string partitionId = dataSetAndPartitionIds.Item2;
                    if (!partitions.ContainsKey(partitionId))
                    {
                        partitions[partitionId] = new HashSet<IActiveContext>();
                    }
                    partitions[partitionId].Add(contextDictionary[contextId]);
                }
            }

            return new DataSet<T2>(newDataSetId,
                new DataSetInfo(newDataSetId,
                    partitions.Select(pair => new PartitionInfo(pair.Key, pair.Value.ToArray())).ToArray()),
                    _partitionCollector);
        }

        public IEnumerable<T> Collect()
        {
            IInjector injector = TangFactory.GetTang().NewInjector(MiniDriverConfiguration.ConfigurationModule
                .Set(MiniDriverConfiguration.OnDriverStarted, GenericType<CollectStage<T>>.Class)
                .Build());
            injector.BindVolatileInstance(GenericType<DataSetInfo>.Class, _dataSetInfo);
            injector.BindVolatileParameter(GenericType<OldDataSetIdNamedParameter>.Class, _id);

            StageRunner stageRunner = injector.GetInstance<StageRunner>();
            stageRunner.StartStage();
            stageRunner.AwaitStage();

            Array.ForEach(_dataSetInfo.PartitionInfos, info => info.LoadedContexts.ForEach(context => context.Dispose()));
            return null;
        }

        private IDictionary<string, IActiveContext> FormContextDictionary()
        {
            IDictionary<string, IActiveContext> contextDictionary = new Dictionary<string, IActiveContext>();
            foreach (PartitionInfo partitionInfo in _dataSetInfo.PartitionInfos)
            {
                foreach (IActiveContext context in partitionInfo.LoadedContexts)
                {
                    contextDictionary[context.Id] = context;
                }
            }
            return contextDictionary;
        }

        public DataSetInfo DataSetInfo
        {
            get { return _dataSetInfo; }
        }
    }
}

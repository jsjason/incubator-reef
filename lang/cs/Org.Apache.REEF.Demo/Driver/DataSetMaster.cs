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
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Demo.Examples;
using Org.Apache.REEF.Demo.Stage;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Demo.Driver
{
    /// <summary>
    /// Rough implementation of IDataSetMaster with very inefficient synchronization support for loading multiple datasets concurrently.
    /// Allocates one partition per one evaluator.
    /// </summary>
    public sealed class DataSetMaster : IDataSetMaster, IObserver<IAllocatedEvaluator>, IObserver<IActiveContext>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DataSetMaster));

        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly IPartitionDescriptorFetcher _partitionDescriptorFetcher;
        private readonly AvroConfigurationSerializer _avroConfigurationSerializer;
        private readonly IDictionary<string, Queue<IConfiguration>> _partitionConfigurationsForDatasets;
        private readonly IDictionary<string, CountdownEvent> _latchesForDatasets;
        private readonly ConcurrentDictionary<string, Tuple<string, string>> _contextIdToDataSetAndPartitionId;
        private readonly ConcurrentDictionary<string, SynchronizedCollection<PartitionInfo>> _partitionInfosForDatasets;

        [Inject]
        private DataSetMaster(IEvaluatorRequestor evaluatorRequestor,
                              IPartitionDescriptorFetcher partitionDescriptorFetcher,
                              AvroConfigurationSerializer avroConfigurationSerializer)
        {
            _evaluatorRequestor = evaluatorRequestor;
            _partitionDescriptorFetcher = partitionDescriptorFetcher;
            _avroConfigurationSerializer = avroConfigurationSerializer;
            _partitionConfigurationsForDatasets = new Dictionary<string, Queue<IConfiguration>>();
            _latchesForDatasets = new Dictionary<string, CountdownEvent>();
            _contextIdToDataSetAndPartitionId = new ConcurrentDictionary<string, Tuple<string, string>>();
            _partitionInfosForDatasets = new ConcurrentDictionary<string, SynchronizedCollection<PartitionInfo>>();
        }

        public IDataSet<T> Load<T>(Uri uri)
        {
            string dataSetId = uri.ToString();
            Logger.Log(Level.Info, "Load data from {0}", dataSetId);

            Queue<IConfiguration> partitionConfigurations = new Queue<IConfiguration>();
            foreach (var partitionDescriptor in _partitionDescriptorFetcher.GetPartitionDescriptors(uri))
            {
                var partitionConf = partitionDescriptor.GetPartitionConfiguration();
                var finalPartitionConf = TangFactory.GetTang().NewConfigurationBuilder(partitionConf)
                    .BindNamedParameter<DataPartitionId, string>(GenericType<DataPartitionId>.Class,
                        partitionDescriptor.Id)
                    .Build();
                partitionConfigurations.Enqueue(finalPartitionConf);
            }

            Logger.Log(Level.Info, "Submit evaluators for {0}", dataSetId);
            lock (partitionConfigurations)
            {
                _partitionConfigurationsForDatasets[dataSetId] = partitionConfigurations;
                _latchesForDatasets[dataSetId] = new CountdownEvent(partitionConfigurations.Count);
                _partitionInfosForDatasets[dataSetId] = new SynchronizedCollection<PartitionInfo>();

                _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                    .SetNumber(partitionConfigurations.Count)
                    .SetMegabytes(1024)
                    .Build());
            }

            Logger.Log(Level.Info, "Waiting evaluators for {0}", dataSetId);
            _latchesForDatasets[dataSetId].Wait();
            Logger.Log(Level.Info, "Done waiting evaluators for {0}", dataSetId);

            SynchronizedCollection<PartitionInfo> partitionInfos;
            if (!_partitionInfosForDatasets.TryRemove(dataSetId, out partitionInfos))
            {
                throw new Exception(string.Format("This should not happen for {0}", dataSetId));
            }
            return new DataSet<T>(dataSetId, new DataSetInfo(dataSetId, partitionInfos.ToArray()));
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            Logger.Log(Level.Info, "Got {0}", allocatedEvaluator);

            IConfiguration partitionConf = null;
            string dataSetId = string.Empty;
            foreach (KeyValuePair<string, Queue<IConfiguration>> pair in _partitionConfigurationsForDatasets)
            {
                dataSetId = pair.Key;
                Queue<IConfiguration> partitionConfigurations = pair.Value;
                lock (partitionConfigurations)
                {
                    if (partitionConfigurations.Count == 0)
                    {
                        //// this dataset has already been given enough evaluators
                        Logger.Log(Level.Verbose,
                            "Dataset {0} has been given enough evaluators, but is still in dictionary. Will remove.",
                            dataSetId);
                        //// _partitionConfigurationsForDatasets.Remove(dataSetId);
                        continue;
                    }

                    partitionConf = partitionConfigurations.Dequeue();
                    if (partitionConfigurations.Count == 0)
                    {
                        Logger.Log(Level.Verbose,
                            "Dataset {0} has been given enough evaluators. Will remove from dictionary.",
                            dataSetId);

                        Monitor.Pulse(partitionConfigurations);
                    }
                    break;
                }
            }

            if (partitionConf == null)
            {
                Logger.Log(Level.Warning, "Found no dataset partition descriptors for {0}. Will release evaluator immediately.", allocatedEvaluator.Id);
                allocatedEvaluator.Dispose();
                return;
            }

            IInjector injector = TangFactory.GetTang().NewInjector(partitionConf);
            string partitionId = injector.GetNamedInstance<DataPartitionId, string>();

            IConfiguration serviceConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<SerializedInitialDataLoadPartitions, string>(
                    GenericType<SerializedInitialDataLoadPartitions>.Class,
                    _avroConfigurationSerializer.ToString(partitionConf))
                .Build();

            string contextId = string.Format("Context-{0}", allocatedEvaluator.Id);
            IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, contextId)
                .Set(ContextConfiguration.OnContextStart, GenericType<DataLoadContext>.Class)
                .Build();
            _contextIdToDataSetAndPartitionId[contextId] = new Tuple<string, string>(dataSetId, partitionId);

            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Info, "Got {0}", activeContext);
            Tuple<string, string> dataSetAndPartitionId = _contextIdToDataSetAndPartitionId[activeContext.Id];
            string dataSetId = dataSetAndPartitionId.Item1;
            string partitionId = dataSetAndPartitionId.Item2;
            Logger.Log(Level.Info, "Signal CountDownLatch for {0} of {1}", partitionId, dataSetId);
            _latchesForDatasets[dataSetId].Signal();

            _partitionInfosForDatasets[dataSetId].Add(new PartitionInfo(partitionId, activeContext));
        }

        public void Store<T>(IDataSet<T> dataSet)
        {
            throw new NotImplementedException();
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

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
using System.Text;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Demo.Driver;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Demo.Example
{
    public class DemoClient
    {
        private readonly IREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;

        [Inject]
        private DemoClient(IREEFClient reefClient, JobRequestBuilder jobRequestBuilder)
        {
            _reefClient = reefClient;
            _jobRequestBuilder = jobRequestBuilder;
        }

        public void Run()
        {
            var driverConf = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<DemoDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<DataSetMaster>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<DataSetMaster>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<TaskCompletedHandler>.Class)
                .Set(DriverConfiguration.OnContextMessage, GenericType<PartitionCollector>.Class)
                .Build();

            var paramConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<DataSetUri, string>(GenericType<DataSetUri>.Class,
                    @"C:\Users\Jason\Documents\criteo-small")
                .Build();

            var jobRequest = _jobRequestBuilder
                .AddDriverConfiguration(Configurations.Merge(driverConf, paramConf))
                .AddGlobalAssemblyForType(typeof(DemoDriver))
                .AddGlobalAssemblyForType(typeof(DataSetMaster))
                .AddGlobalAssemblyForType(typeof(TaskCompletedHandler))
                .AddGlobalAssemblyForType(typeof(PartitionCollector))
                .SetJobIdentifier("DemoREEEF")
                .Build();

            _reefClient.Submit(jobRequest);
        }

        public static void MainOne(string[] args)
        {
            byte[] bytes = Encoding.UTF8.GetBytes("");
            Console.WriteLine(bytes.Length);
            Console.WriteLine(BitConverter.ToString(bytes));
        }

        public static void Main(string[] args)
        {
            TangFactory.GetTang().NewInjector(LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "6")
                        .Build())
                        .GetInstance<DemoClient>()
                        .Run();
        }
    }
}

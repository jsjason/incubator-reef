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
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Demo.Driver;
using Org.Apache.REEF.Demo.Examples;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Demo
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

        private void Run()
        {
            IConfiguration driverConf = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<DemoDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<DataSetMaster>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<DataSetMaster>.Class)
                .Set(DriverConfiguration.OnContextMessage, GenericType<PartitionCollector>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<DataSetTaskCompleteHandler>.Class)
                .Build();

            IConfiguration addConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<DataSetUri, string>(GenericType<DataSetUri>.Class, @"C:\Users\t-joosj\Documents\ab")
                .Build();

            JobRequest jobRequest = _jobRequestBuilder
                .AddDriverConfiguration(Configurations.Merge(driverConf, addConf))
                .AddGlobalAssemblyForType(typeof(DemoDriver))
                .AddGlobalAssemblyForType(typeof(DataSetMaster))
                .AddGlobalAssemblyForType(typeof(PartitionCollector))
                .AddGlobalAssemblyForType(typeof(DataSetTaskCompleteHandler))
                .SetJobIdentifier("DemoREEF")
                .Build();

            _reefClient.Submit(jobRequest);
        }

        public static void MainOne(string[] args)
        {
            TangFactory.GetTang().NewInjector(
                LocalRuntimeClientConfiguration.ConfigurationModule
                    .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "6")
                        .Build())
                .GetInstance<DemoClient>().Run();
        }

        public static void MainTwo(string[] args)
        {
            int? s = 3;
            object i2 = (object)s;
            Console.WriteLine(s);
            Console.WriteLine(i2);

            IWrapper<int?> t = new Wrapper<int?>(5);
            IWrapper<object> t2 = t as IWrapper<object>;
            Console.WriteLine(t);
            Console.WriteLine(t2);
        }

        public static void Main(string[] args)
        {
            IFormatter formatter = new BinaryFormatter();
            using (Stream stream = new MemoryStream())
            {
                String 
                formatter.Serialize(stream, "Abcabsabsas");
                stream.Seek(0, SeekOrigin.Begin);
                object o = formatter.Deserialize(stream);
                Console.WriteLine(o);
            }
        }

        public static void Test(string[] args)
        {
            using (Stream stream = File.Open(@"C:\Users\t-joosj\Documents\ab\abc.txt", FileMode.CreateNew))
            {
                int i = 2;
                byte[] bytes = BitConverter.GetBytes(i);
                stream.Write(bytes, 0, bytes.Length);
            }

            using (Stream stream = File.Open(@"C:\Users\t-joosj\Documents\ab\abcd.txt", FileMode.CreateNew))
            {
                int i = 3;
                byte[] bytes = BitConverter.GetBytes(i);
                stream.Write(bytes, 0, bytes.Length);
            }

            using (Stream stream = File.Open(@"C:\Users\t-joosj\Documents\ab\abcde.txt", FileMode.CreateNew))
            {
                int i = 5;
                byte[] bytes = BitConverter.GetBytes(i);
                stream.Write(bytes, 0, bytes.Length);
            }
        }
    }
}

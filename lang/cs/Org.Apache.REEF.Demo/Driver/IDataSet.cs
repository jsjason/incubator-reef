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

using System.Collections.Generic;
using Org.Apache.REEF.Demo.Evaluator;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Demo.Driver
{
    /// <summary>
    /// A driver-side representation of a partitioned dataset that is distributed across evaluators.
    /// </summary>
    /// <typeparam name="T">Partition type</typeparam>
    public interface IDataSet<out T>
    {
        /// <summary>
        /// String identifier of this dataset.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Apply a transform to the partitions of this dataset.
        /// </summary>
        /// <remarks>
        /// <code>transformConf</code> gets shipped to each partition.
        /// This is a partition-wise operation.
        /// </remarks>
        /// <typeparam name="T2">Partition type of new dataset</typeparam>
        /// <param name="transformConf">Tang configuration containing the transform</param>
        /// <returns>New dataset consisting of the transformed partitions</returns>
        IDataSet<T2> TransformPartitions<T2>(IConfiguration transformConf);

        IDataSet<T2> TransformPartitions<T2, TTransform>() where TTransform : ITransform<T, T2>;

            /// <summary>
        /// General interface for applying operations.
        /// </summary>
        /// <typeparam name="T2">Partition type of new dataset</typeparam>
        /// <param name="stageConf">Tang configuration containing the StageDriver information</param>
        /// <returns>New dataset consisting of new partitions</returns>
        IDataSet<T2> RunStage<T2>(IConfiguration stageConf);

        /// <summary>
        /// Fetch the actual data to the local process.
        /// May result in OutOfMemory exception if <code>T</code> is too large.
        /// </summary>
        /// <returns><see cref="IEnumerable{T}"/> of the actual partition data</returns>
        IEnumerable<T> Collect();
    }
}

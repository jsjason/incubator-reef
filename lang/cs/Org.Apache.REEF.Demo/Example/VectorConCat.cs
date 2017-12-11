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
using System.Linq;
using Org.Apache.REEF.Demo.Evaluator;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Example
{
    public sealed class VectorConcat : ITransform<LabelVectorVector[], LabelVector[]>
    {
        [Inject]
        private VectorConcat()
        {
        }

        public LabelVector[] Apply(LabelVectorVector[] input)
        {
            return input.Select(labelVectorVector =>
            {
                var vector1 = labelVectorVector.Vector1;
                var vector2 = labelVectorVector.Vector2;
                var newVector = new SparseVector(vector1.Length + vector2.Length);

                foreach (var pair in vector1.AllNonZeros)
                {
                    newVector[pair.Key] = pair.Value;
                }

                foreach (var pair in vector2.AllNonZeros)
                {
                    newVector[pair.Key + vector1.Length] = pair.Value;
                }

                return new LabelVector(labelVectorVector.Label, newVector);
            }).ToArray();
        }
    }
}

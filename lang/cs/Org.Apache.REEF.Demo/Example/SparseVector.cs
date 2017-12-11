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

namespace Org.Apache.REEF.Demo.Example
{
    public sealed class SparseVector
    {
        private readonly IDictionary<int, float> _data;
        private readonly int _length;

        public SparseVector(int length)
        {
            _data = new Dictionary<int, float>();
            _length = length;
        }

        public float this[int i]
        {
            get
            {
                CheckArgument(i);
                return _data.ContainsKey(i) ? _data[i] : 0;
            }

            set
            {
                CheckArgument(i);
                if (value == 0)
                {
                    _data.Remove(i);
                }
                else
                {
                    _data[i] = value;
                }
            }
        }

        public int Length
        {
            get { return _length;}
        }

        public IEnumerable<KeyValuePair<int, float>> AllNonZeros
        {
            get { return _data; }
        }

        private void CheckArgument(int i)
        {
            if (i < 0 || i >= _length)
            {
                throw new ArgumentOutOfRangeException();
            }
        }
    }
}

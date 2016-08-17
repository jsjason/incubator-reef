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
using System.IO;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Demo.Task
{
    internal sealed class ResultCodec : ICodec<IEnumerable<Tuple<string, string>>>
    {
        [Inject]
        private ResultCodec()
        {
        }

        public byte[] Encode(IEnumerable<Tuple<string, string>> tuples)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                foreach (var tuple in tuples)
                {
                    byte[] bytes1 = ByteUtilities.StringToByteArrays(tuple.Item1);
                    byte[] bytes1Length = BitConverter.GetBytes(bytes1.Length);
                    stream.Write(bytes1Length, 0, bytes1Length.Length);
                    stream.Write(bytes1, 0, bytes1.Length);

                    byte[] bytes2 = ByteUtilities.StringToByteArrays(tuple.Item2);
                    byte[] bytes2Length = BitConverter.GetBytes(bytes2.Length);
                    stream.Write(bytes2Length, 0, bytes2Length.Length);
                    stream.Write(bytes2, 0, bytes2.Length);
                }

                return stream.ToArray();
            }
        }

        public IEnumerable<Tuple<string, string>> Decode(byte[] data)
        {
            using (MemoryStream stream = new MemoryStream(data))
            {
                IList<Tuple<string, string>> retList = new List<Tuple<string, string>>();
                while (stream.Position < stream.Length)
                {
                    byte[] bytes1Length = new byte[4];
                    stream.Read(bytes1Length, 0, bytes1Length.Length);
                    
                    int item1Length = BitConverter.ToInt32(bytes1Length, 0);
                    byte[] bytes1 = new byte[item1Length];
                    stream.Read(bytes1, 0, bytes1.Length);
                    string item1 = ByteUtilities.ByteArraysToString(bytes1);

                    byte[] bytes2Length = new byte[4];
                    stream.Read(bytes2Length, 0, bytes2Length.Length);
                    
                    int item2Length = BitConverter.ToInt32(bytes2Length, 0);
                    byte[] bytes2 = new byte[item2Length];
                    stream.Read(bytes2, 0, bytes2.Length);
                    string item2 = ByteUtilities.ByteArraysToString(bytes2);

                    retList.Add(new Tuple<string, string>(item1, item2));
                }

                return retList;
            }
        }
    }
}

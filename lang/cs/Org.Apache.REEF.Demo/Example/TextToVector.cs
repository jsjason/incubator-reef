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
using Org.Apache.REEF.Demo.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Demo.Example
{
    public sealed class TextToVector : ITransform<LabelVectorTxt[], LabelVectorVector[]>
    {
        private static int HashBitLength = 16;
        private static int BitsInOneByte = 8;

        [Inject]
        private TextToVector()
        {
        }

        public LabelVectorVector[] Apply(LabelVectorTxt[] input)
        {
            LabelVectorVector[] retData = new LabelVectorVector[input.Length];
            for (int i = 0; i < input.Length; i++)
            {
                var inputTextData = input[i].Txt.Data;
                SparseVector vector = new SparseVector(inputTextData.Length * (int)Math.Pow(2, HashBitLength));
                for (int j = 0; j < inputTextData.Length; j++)
                {
                    string innerData = inputTextData[j];
                    byte[] bytes = ByteUtilities.StringToByteArrays(innerData);
                    if (bytes.Length > 0)
                    {
                        byte[] hashBytes = new byte[HashBitLength/BitsInOneByte];
                        Array.Copy(bytes, hashBytes, HashBitLength/BitsInOneByte);

                        int hash = BitConverter.ToUInt16(hashBytes, 0);
                        vector[j*(2 ^ HashBitLength) + hash] = 1;
                    }
                }

                retData[i] = new LabelVectorVector(input[i].Label, input[i].Vector, vector);
            }

            return retData;
        }
    }
}

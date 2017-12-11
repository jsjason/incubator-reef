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
using Org.Apache.REEF.Demo.Evaluator;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Example
{
    public sealed class TextToLabelNumTxt : ITransform<string[], LabelNumTxt[]>
    {
        [Inject]
        private TextToLabelNumTxt()
        {
        }

        public LabelNumTxt[] Apply(string[] input)
        {
            List<LabelNumTxt> retList = new List<LabelNumTxt>();
            foreach (string line in input)
            {
                string[] split = line.Split('\t');
                if (split.Length != 40)
                {
                    throw new Exception("Not Criteo.");
                }

                int label;
                if (!int.TryParse(split[0], out label))
                {
                    throw new Exception("Exception during parsing label.");
                }

                int?[] numData = new int?[13];
                for (int i = 1; i <= numData.Length; i++)
                {
                    int value;
                    if (!int.TryParse(split[i], out value))
                    {
                        numData[i - 1] = value;
                    }
                    else
                    {
                        numData[i - 1] = null;
                    }
                }

                string[] txtData = new string[26];
                for (int i = numData.Length + 1; i <= numData.Length + txtData.Length; i++)
                {
                    txtData[i - numData.Length - 1] = split[i];
                }

                var labelNumTxt = new LabelNumTxt(label, numData, txtData);
                retList.Add(labelNumTxt);
            }

            return retList.ToArray();
        }
    }
}

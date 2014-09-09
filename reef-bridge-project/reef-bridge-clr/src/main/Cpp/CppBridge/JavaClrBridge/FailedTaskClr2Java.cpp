/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Clr2JavaImpl.h"

namespace Microsoft {
  namespace Reef {
    namespace Driver {
      namespace Bridge {
        ref class ManagedLog {
          internal:
            static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
        };

        FailedTaskClr2Java::FailedTaskClr2Java(JNIEnv *env, jobject jobjectFailedTask) {
          ManagedLog::LOGGER->LogStart("FailedTaskClr2Java::AllocatedEvaluatorClr2Java");
          pin_ptr<JavaVM*> pJavaVm = &_jvm;
          int gotVm = env -> GetJavaVM(pJavaVm);
          _jobjectFailedTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectFailedTask));
          ManagedLog::LOGGER->LogStop("FailedTaskClr2Java::AllocatedEvaluatorClr2Java");
        }

        IActiveContextClr2Java^ FailedTaskClr2Java::GetActiveContext() {
          ManagedLog::LOGGER->LogStart("FailedTaskClr2Java::GetActiveContext");

          JNIEnv *env = RetrieveEnv(_jvm);

          jclass jclassFailedTask = env->GetObjectClass(_jobjectFailedTask);
          jfieldID jidActiveContext = env->GetFieldID(jclassFailedTask, "jactiveContext", "Lcom/microsoft/reef/javabridge/ActiveContextBridge;");
          jobject jobjectActiveContext = env->GetObjectField(_jobjectFailedTask, jidActiveContext);

          ManagedLog::LOGGER->LogStop("FailedTaskClr2Java::GetActiveContext");
          return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
        }

        String^ FailedTaskClr2Java::GetString() {
          ManagedLog::LOGGER->LogStart("FailedTaskClr2Java::GetString");
          JNIEnv *env = RetrieveEnv(_jvm);

          jclass jclassFailedTask = env->GetObjectClass (_jobjectFailedTask);
          jmethodID jmidGetFailedTaskString = env->GetMethodID(jclassFailedTask, "getFailedTaskString", "()Ljava/lang/String;");

          if (jmidGetFailedTaskString == NULL) {
            ManagedLog::LOGGER->LogStart("jmidGetFailedTaskString is NULL");
            return nullptr;
          }
          jstring jFailedTaskString = (jstring)env -> CallObjectMethod(
                                        _jobjectFailedTask,
                                        jmidGetFailedTaskString);
          ManagedLog::LOGGER->LogStop("FailedTaskClr2Java::GetString");
          return ManagedStringFromJavaString(env, jFailedTaskString);
        }

        void FailedTaskClr2Java::OnError(String^ message) {
          ManagedLog::LOGGER->Log("FailedTaskClr2Java::OnError");
          JNIEnv *env = RetrieveEnv(_jvm);
          HandleClr2JavaError(env, message, _jobjectFailedTask);
        }
      }
    }
  }
}
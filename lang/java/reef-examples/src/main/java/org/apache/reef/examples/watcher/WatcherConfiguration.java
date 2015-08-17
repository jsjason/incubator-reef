/*
 * Copyright (C) 2015 Seoul National University
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
package org.apache.reef.examples.watcher;

import org.apache.reef.driver.parameters.*;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.wake.time.Clock;

/**
 *
 */
public final class WatcherConfiguration extends ConfigurationModuleBuilder {

  public static final ConfigurationModule CONF = new WatcherConfiguration()
      .bindSetEntry(Clock.RuntimeStartHandler.class, Watcher.DriverRuntimeStartHandler.class)
      .bindSetEntry(Clock.StartHandler.class, Watcher.DriverStartHandler.class)
      .bindSetEntry(Clock.StopHandler.class, Watcher.DriverStopHandler.class)
      .bindSetEntry(Clock.RuntimeStopHandler.class, Watcher.DriverRuntimeStopHandler.class)
      .bindSetEntry(ServiceContextActiveHandlers.class, Watcher.ContextActiveHandler.class)
      .bindSetEntry(ServiceContextClosedHandlers.class, Watcher.ContextClosedHandler.class)
      .bindSetEntry(ServiceContextFailedHandlers.class, Watcher.ContextFailedHandler.class)
      .bindSetEntry(ServiceEvaluatorAllocatedHandlers.class, Watcher.EvaluatorAllocatedHandler.class)
      .bindSetEntry(ServiceEvaluatorFailedHandlers.class, Watcher.EvaluatorFailedHandler.class)
      .bindSetEntry(ServiceEvaluatorCompletedHandlers.class, Watcher.EvaluatorCompletedHandler.class)
      .bindSetEntry(ServiceTaskFailedHandlers.class, Watcher.TaskFailedHandler.class)
      .bindSetEntry(ServiceTaskCompletedHandlers.class, Watcher.TaskCompletedHandler.class)
      .bindSetEntry(ServiceTaskMessageHandlers.class, Watcher.TaskMessageHandler.class)
      .bindSetEntry(ServiceTaskRunningHandlers.class, Watcher.TaskRunningHandler.class)
      .bindSetEntry(ServiceTaskSuspendedHandlers.class, Watcher.TaskSuspendedHandler.class)
      .build();
}

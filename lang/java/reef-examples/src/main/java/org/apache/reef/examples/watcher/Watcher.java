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

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.task.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;
import org.codehaus.jackson.map.ObjectMapper;

import javax.inject.Inject;
import java.io.IOException;

/**
 *
 */
@Unit
public final class Watcher {

  private final String driverIdentifier;
  private final HttpClient httpClient;
  private String id;

  @Inject
  private Watcher(@Parameter(DriverIdentifier.class) final String driverIdentifier) {
    this.driverIdentifier = driverIdentifier;
    this.httpClient = HttpClientBuilder.create().build();
    reportStartEvent();
  }

  private void reportStartEvent() {
    ObjectMapper objectMapper = new ObjectMapper();
    final String res = report("http://localhost:1337/job/create?name=" + driverIdentifier + "&state=RUNNING");
    try {
      Job job = objectMapper.readValue(res, Job.class);
      this.id = job.getId();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void reportEvent(final String name, final String state) {
    report("http://localhost:1337/event/create?type=" + name + "&name=" + state + "&job=" + id);
  }

  private String report(final String url) {
    final HttpGet request = new HttpGet(url);
    try {
      final HttpResponse response = httpClient.execute(request);
      final int code = response.getStatusLine().getStatusCode();
      return EntityUtils.toString(response.getEntity());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public final class DriverRuntimeStartHandler implements EventHandler<RuntimeStart> {

    @Override
    public void onNext(final RuntimeStart value) {
      reportEvent("RuntimeStart_" + value.getTimeStamp(), "START");
    }
  }

  public final class DriverStartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime value) {
      reportEvent("Driver_Start_" + value.getTimeStamp(), "START");
    }
  }

  public final class DriverStopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime value) {
      reportEvent("Driver_Stop_" + value.getTimeStamp(), "STOP");
    }
  }

  public final class DriverRuntimeStopHandler implements EventHandler<RuntimeStop> {

    @Override
    public void onNext(final RuntimeStop value) {
      reportEvent("RuntimeStop_" + value.getTimeStamp(), "STOP");
    }
  }

  public final class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext value) {
      reportEvent("ContextStart_" + value.getId(), "Start");
    }
  }

  public final class ContextClosedHandler implements EventHandler<ClosedContext> {

    @Override
    public void onNext(final ClosedContext value) {
      reportEvent("ContextStop_" + value.getId(), "Stop");
    }
  }

  public final class ContextFailedHandler implements EventHandler<FailedContext> {

    @Override
    public void onNext(final FailedContext value) {
      reportEvent("FailedContext_" + value.getId(), "Failed");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator value) {
      reportEvent("EvaluatorAllocated_" + value.getId(), "Allocated");
    }
  }

  public final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator value) {
      reportEvent("FailedEvaluator_" + value.getId(), "Failed");
    }
  }

  public final class EvaluatorCompletedHandler implements EventHandler<CompletedEvaluator> {

    @Override
    public void onNext(final CompletedEvaluator value) {
      reportEvent("CompletedEvaluator_" + value.getId(), "Complete");
    }
  }

  public final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask value) {
      reportEvent("CompletedTask_" + value.getId(), "Complete");
    }
  }

  public final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask value) {
      reportEvent("FailedTask_" + value.getId(), "Failed");
    }
  }

  public final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask value) {
      reportEvent("RunningTask_" + value.getId() + "_ContextID_" + value.getActiveContext().getId(), "Running");
    }
  }

  public final class TaskMessageHandler implements EventHandler<TaskMessage> {

    @Override
    public void onNext(final TaskMessage value) {
      reportEvent("TaskMessage_[Context_ID_" +
          value.getContextId() + "][source" + value.getMessageSourceID() + "]", "message");
    }
  }

  public final class TaskSuspendedHandler implements EventHandler<SuspendedTask> {

    @Override
    public void onNext(final SuspendedTask value) {
      reportEvent("SuspendedTask_" + value.getId(), "Suspended");
    }
  }
}

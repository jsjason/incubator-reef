/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

  private void reportEndEvent() {
    report("http://localhost:1337/job/update/" + id + "?state=FINISHED");
  }

  private void reportEvent(final String type, final String name, final String evaluatorId) {
    final StringBuilder urlBuilder = new StringBuilder()
        .append("http://localhost:1337/event/create")
        .append("?type=")
        .append(type)
        .append("&name=")
        .append(name)
        .append("&job=")
        .append(id);
    if (evaluatorId != null) {
      urlBuilder.append("&evaluatorId=")
          .append(evaluatorId);
    }
    report(urlBuilder.toString());
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
      reportEvent("RuntimeStart", driverIdentifier, null);
    }
  }

  public final class DriverStartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime value) {
      reportEvent("StartTime", driverIdentifier, null);
    }
  }

  public final class DriverStopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime value) {
      reportEvent("StopTime", driverIdentifier, null);
    }
  }

  public final class DriverRuntimeStopHandler implements EventHandler<RuntimeStop> {

    @Override
    public void onNext(final RuntimeStop value) {
      reportEvent("RuntimeStop", driverIdentifier, null);
      reportEndEvent();
    }
  }

  public final class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext value) {
      reportEvent("ContextStart", value.getId(), value.getEvaluatorId());
    }
  }

  public final class ContextClosedHandler implements EventHandler<ClosedContext> {

    @Override
    public void onNext(final ClosedContext value) {
      reportEvent("ClosedContext", value.getId(), value.getEvaluatorId());
    }
  }

  public final class ContextFailedHandler implements EventHandler<FailedContext> {

    @Override
    public void onNext(final FailedContext value) {
      reportEvent("FailedContext" , value.getId(), value.getEvaluatorId());
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator value) {
      reportEvent("AllocatedEvaluator", value.getId(), value.getId());
    }
  }

  public final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator value) {
      reportEvent("FailedEvaluator", value.getId(), value.getId());
    }
  }

  public final class EvaluatorCompletedHandler implements EventHandler<CompletedEvaluator> {

    @Override
    public void onNext(final CompletedEvaluator value) {
      reportEvent("CompletedEvaluator", value.getId(), value.getId());
    }
  }

  public final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask value) {
      reportEvent("CompletedTask", value.getId(), value.getActiveContext().getEvaluatorId());
    }
  }

  public final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask value) {
      final String evaluatorId;
      if (value.getActiveContext().isPresent()) {
        evaluatorId = value.getActiveContext().get().getEvaluatorId();
      } else {
        evaluatorId = null;
      }
      reportEvent("CompletedTask", value.getId(), evaluatorId);
    }
  }

  public final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask value) {
      reportEvent("RunningTask", value.getId(), value.getActiveContext().getEvaluatorId());
    }
  }

  public final class TaskMessageHandler implements EventHandler<TaskMessage> {

    @Override
    public void onNext(final TaskMessage value) {
      reportEvent("TaskMessage", value.getId(), value.getContextId());
    }
  }

  public final class TaskSuspendedHandler implements EventHandler<SuspendedTask> {

    @Override
    public void onNext(final SuspendedTask value) {
      reportEvent("SuspendedTask", value.getId(), value.getActiveContext().getEvaluatorId());
    }
  }
}

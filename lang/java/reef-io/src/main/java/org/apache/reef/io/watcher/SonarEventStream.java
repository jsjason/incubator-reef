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
package org.apache.reef.io.watcher;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.codehaus.jackson.map.ObjectMapper;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class SonarEventStream implements EventStream {

  private final HttpClient httpClient;
  private final String id;

  @Inject
  private SonarEventStream(@Parameter(DriverIdentifier.class) final String driverId) {
    this.httpClient = HttpClientBuilder.create().build();

    final ObjectMapper objectMapper = new ObjectMapper();
    final String res = report("http://localhost:1337/job/create?name=" + driverId + "&state=RUNNING");
    try {
      SonarJob job = objectMapper.readValue(res, SonarJob.class);
      this.id = job.getId();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onEvent(final EventType type, final String jsonEncodedEvent) {
    try {
      final Map<String, Object> map = new ObjectMapper().readValue(jsonEncodedEvent, HashMap.class);

      switch (type) {
      case ActiveContext:
      case ClosedContext:
      case FailedContext:
        final Map<String, Object> baseMap = (Map<String, Object>)map.get("base");
        final String evalId = (String)baseMap.get("evaluatorId");
        final String contextId = (String)baseMap.get("id");
        reportEvent(type.toString(), contextId, evalId);
        break;

      case AllocatedEvaluator:
      case CompletedEvaluator:
      case FailedEvaluator:
        final String evalId2 = (String)map.get("id");
        reportEvent(type.toString(), "JobEval", evalId2);
        break;

      case CompletedTask:
      case FailedTask:
      case RunningTask:
      case SuspendedTask:
        final Map<String, Object> contextMap = (Map<String, Object>)map.get("activeContext");
        final Map<String, Object> baseMap2 = (Map<String, Object>)contextMap.get("base");
        final String evalId3 = (String)baseMap2.get("evaluatorId");
        final String taskId = (String)map.get("id");
        reportEvent(type.toString(), taskId, evalId3);
        break;

      default:
          reportEvent(type.toString(), "Job", null);

      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
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
      urlBuilder
          .append("&evaluatorId=")
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
}

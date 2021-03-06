/*
 * Copyright 2019 Google LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 * Any software provided by Google hereunder is distributed “AS IS”,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, and is not intended for production use.
 */

package com.google.gcs.sdrs.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.gcs.sdrs.common.ExecutionEventType;
import com.google.gcs.sdrs.controller.pojo.ErrorResponse;
import com.google.gcs.sdrs.controller.pojo.EventResponse;
import com.google.gcs.sdrs.controller.pojo.ExecutionEventRequest;
import com.google.gcs.sdrs.controller.validation.ValidationResult;
import com.google.gcs.sdrs.service.impl.EventsServiceImpl;
import javax.ws.rs.core.Response;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.junit.Before;
import org.junit.Test;

public class EventsControllerTest {

  private EventsController controller;

  @Before()
  public void setup() {
    controller = new EventsController();
    controller.service = mock(EventsServiceImpl.class);
  }

  @Test
  public void generateExceptionResponseWithValidInputReturnsResponseWithFields() {
    HttpException testException = new ValidationException(ValidationResult.fromString("test"));
    Response response = controller.generateExceptionResponse(testException);
    assertEquals(response.getStatus(), HttpStatus.BAD_REQUEST_400.getStatusCode());
    assertEquals(((ErrorResponse) response.getEntity()).getMessage(), "Invalid input: test");
  }

  @Test
  public void executeEventWhenSuccessfulIncludesResponseFields() {
    ExecutionEventRequest request = new ExecutionEventRequest();
    request.setExecutionEventType(ExecutionEventType.POLICY);

    Response response = controller.executeEvent(request);

    assertEquals(response.getStatus(), HttpStatus.OK_200.getStatusCode());
    assertTrue(((EventResponse) response.getEntity()).getMessage().length() > 0);
    assertNotNull(((EventResponse) response.getEntity()).getUuid());
  }

  @Test
  public void executeEventMissingTypeFails() {
    ExecutionEventRequest request = new ExecutionEventRequest();

    Response response = controller.executeEvent(request);

    assertEquals(response.getStatus(), HttpStatus.BAD_REQUEST_400.getStatusCode());
    assertTrue(((ErrorResponse) response.getEntity()).getMessage().contains("type"));
  }

  @Test
  public void executePolicyWithValidFieldsSucceeds() {
    ExecutionEventRequest request = new ExecutionEventRequest();
    request.setExecutionEventType(ExecutionEventType.POLICY);

    Response response = controller.executeEvent(request);

    assertEquals(response.getStatus(), HttpStatus.OK_200.getStatusCode());
  }

  @Test
  public void executeUserEventWithValidFieldsSucceeds() {
    ExecutionEventRequest request = new ExecutionEventRequest();
    request.setExecutionEventType(ExecutionEventType.USER_COMMANDED);
    request.setProjectId("projectId");
    request.setTarget("gs://b/s/t");

    Response response = controller.executeEvent(request);

    assertEquals(response.getStatus(), HttpStatus.OK_200.getStatusCode());
  }

  @Test
  public void executeUserEventWithInvalidProjectIdFails() {
    ExecutionEventRequest request = new ExecutionEventRequest();
    request.setExecutionEventType(ExecutionEventType.USER_COMMANDED);
    request.setProjectId(null);
    request.setTarget("gs://b/s/t");

    Response response = controller.executeEvent(request);

    assertEquals(response.getStatus(), HttpStatus.BAD_REQUEST_400.getStatusCode());
    assertTrue(((ErrorResponse) response.getEntity()).getMessage().contains("projectId"));
  }

  @Test
  public void executeValidationSucceeds() {
    Response response = controller.executeValidation();

    assertEquals(response.getStatus(), HttpStatus.OK_200.getStatusCode());
  }
}

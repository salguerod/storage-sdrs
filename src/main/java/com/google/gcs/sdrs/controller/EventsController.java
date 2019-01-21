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

import com.google.cloudy.retention.controller.validation.FieldValidations;
import com.google.cloudy.retention.controller.validation.ValidationResult;
import com.google.gcs.sdrs.controller.pojo.request.ExecutionEventRequest;
import com.google.gcs.sdrs.controller.pojo.response.EventResponse;
import java.util.LinkedList;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Controller for exposing event based behavior */
@Path("/events")
public class EventsController extends BaseController {

  private static final Logger logger = LoggerFactory.getLogger(EventsController.class);

  /** Accepts a request to invoke a policy or process a manual delete */
  @POST
  @Path("/execution")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response executeEvent(ExecutionEventRequest request) {
    String requestUuid = generateRequestUuid();

    try {
      validateExecutionEvent(request);

      // TODO: Perform business logic

      EventResponse response = new EventResponse();

      response.setRequestUuid(requestUuid);
      response.setMessage("Event registered and awaiting execution.");

      return Response.status(200).entity(response).build();
    } catch (HttpException exception) {
      return generateExceptionResponse(exception, requestUuid);
    }
  }

  /**
   * Runs validation checks against the "Execution" event request type
   *
   * @throws ValidationException when the request is invalid
   */
  private void validateExecutionEvent(ExecutionEventRequest request) throws ValidationException {
    List<ValidationResult> partialValidations = new LinkedList<>();

    if (request.getType() == null) {
      partialValidations.add(ValidationResult.fromString("type must be provided"));
    } else {
      switch (request.getType()) {
        case POLICY:
          break;
        case USER_COMMANDED:
          if (request.getProjectId() == null) {
            partialValidations.add(
                ValidationResult.fromString("projectId must be provided if type is USER"));
          }
          partialValidations.add(
              FieldValidations.validateFieldFollowsBucketNamingStructure(
                  "target", request.getTarget()));
          break;
        default:
          break;
      }
    }

    ValidationResult result = ValidationResult.compose(partialValidations);
    if (!result.isValid) {
      throw new ValidationException(result);
    }
  }
}

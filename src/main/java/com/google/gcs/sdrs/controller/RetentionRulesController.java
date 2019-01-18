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
 *
 */

package com.google.gcs.sdrs.controller;

import com.google.cloudy.retention.controller.validation.FieldValidations;
import com.google.cloudy.retention.controller.validation.ValidationResult;
import com.google.gcs.sdrs.controller.pojo.request.RetentionRuleCreateRequest;
import com.google.gcs.sdrs.controller.pojo.request.RetentionRuleUpdateRequest;
import com.google.gcs.sdrs.controller.pojo.response.RetentionRuleCreateResponse;
import com.google.gcs.sdrs.controller.pojo.response.RetentionRuleUpdateResponse;
import com.google.gcs.sdrs.enums.RetentionRuleTypes;
import java.util.LinkedList;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Controller for managing retention rule objects over HTTP */
@Path("/retentionrules")
public class RetentionRulesController extends BaseController {

  private static final Logger logger = LoggerFactory.getLogger(RetentionRulesController.class);
  private static final Integer RETENTION_MAX_VALUE = 200;

  /** CRUD create endpoint */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(RetentionRuleCreateRequest request) {
    String requestUuid = generateRequestUuid();

    try {
      validateCreate(request);

      // TODO: Perform business logic

      RetentionRuleCreateResponse response = new RetentionRuleCreateResponse();
      response.setRequestUuid(requestUuid);

      // TODO: Replace with real value
      response.setRuleId(1);
      return Response.status(200).entity(response).build();
    } catch (HttpException exception) {
      return generateExceptionResponse(exception, requestUuid);
    }
  }

  /** CRUD update endpoint */
  @PUT
  @Path("/{ruleId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response update(@PathParam("ruleId") Integer ruleId, RetentionRuleUpdateRequest request) {
    String requestUuid = generateRequestUuid();

    try {
      validateUpdate(request);

      // TODO: Perform business logic

      RetentionRuleUpdateResponse response = new RetentionRuleUpdateResponse();
      response.setRequestUuid(requestUuid);

      // TODO: map actual values to response
      response.setDatasetName("dataset");
      response.setDataStorageName("gs://bucket/dataset");
      response.setProjectId("projectId");
      response.setRetentionPeriod(123);
      response.setRuleId(ruleId);
      response.setType(RetentionRuleTypes.DATASET);

      return Response.status(200).entity(response).build();
    } catch (HttpException exception) {
      return generateExceptionResponse(exception, requestUuid);
    }
  }

  /**
   * Validates the object for creating a retention rule
   *
   * @param request the create request object
   * @throws ValidationException when the request is invalid
   */
  private void validateCreate(RetentionRuleCreateRequest request) throws ValidationException {
    List<ValidationResult> partialValidations = new LinkedList<>();

    partialValidations.add(validateRetentionPeriod(request.getRetentionPeriod()));

    if (request.getType() == null) {
      partialValidations.add(ValidationResult.fromString("type must be provided"));
    } else {
      switch (request.getType()) {
        case GLOBAL:
          break;
        case DATASET:
          partialValidations.add(
              FieldValidations.validateDataStorageName(request.getDataStorageName()));

          if (request.getProjectId() == null) {
            partialValidations.add(
                ValidationResult.fromString("projectId must be provided if type is DATASET"));
          }
          break;
        default:
          break;
      }
    }

    ValidationResult result = ValidationResult.compose(partialValidations);

    if (result.validationMessages.size() > 0) {
      throw new ValidationException(result);
    }
  }

  private void validateUpdate(RetentionRuleUpdateRequest request) throws ValidationException {
    ValidationResult result = validateRetentionPeriod(request.getRetentionPeriod());

    if (result.validationMessages.size() > 0) {
      throw new ValidationException(result);
    }
  }

  private ValidationResult validateRetentionPeriod(Integer retentionPeriod) {
    List<String> messages = new LinkedList<>();
    if (retentionPeriod == null) {
      messages.add("retentionPeriod must be provided");
    } else {
      if (retentionPeriod < 0) {
        messages.add("retentionPeriod must be at least 0");
      }
      if (retentionPeriod > RETENTION_MAX_VALUE) {
        messages.add(
            String.format("retentionPeriod exceeds maximum value of %d", RETENTION_MAX_VALUE));
      }
    }
    return new ValidationResult(messages);
  }
}

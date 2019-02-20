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

import javax.ws.rs.core.Response;

/** Exception thrown when internal errors occur. Hides internal details. */
public class InternalServerException extends HttpException {

  public InternalServerException(Exception exception) {
    this.initCause(exception);
  }

  /** Gets an error message hiding internal details */
  @Override
  public String getMessage() {
    return "Internal server error";
  }

  /** Gets the error HTTP status code */
  @Override
  public int getStatusCode() {
    return Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
  }
}

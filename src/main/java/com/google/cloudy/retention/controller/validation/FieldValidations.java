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

package com.google.cloudy.retention.controller.validation;

import java.util.LinkedList;
import java.util.List;

/** Exposes static methods for validation shared between controllers. */
public class FieldValidations {
  private static final String STORAGE_PREFIX = "gs://";

  /**
   * Runs a validation check on a data storage name
   *
   * @return A validation result containing a list of validation error messages.
   */
  public static ValidationResult validateDataStorageName(String dataStorageName) {
    List<String> validationMessages = new LinkedList<>();
    if (dataStorageName == null) {
      validationMessages.add("dataStorageName must be provided");
    } else {
      // DataStorageName should match gs://<bucket_name>/<dataset_name>
      if (!dataStorageName.startsWith(STORAGE_PREFIX)) {
        validationMessages.add(
            String.format("dataStorageName must start with '%s'", STORAGE_PREFIX));
      } else {
        String bucketAndDataset = dataStorageName.substring(STORAGE_PREFIX.length());
        String[] pathSegments = bucketAndDataset.split("/");

        if (pathSegments[0].length() == 0) {
          validationMessages.add("dataStorageName must include a bucket name");
        }
        if (pathSegments.length < 2 || pathSegments[1].length() == 0) {
          validationMessages.add("dataStorageName must include a dataset name");
        }
      }
    }
    return new ValidationResult(validationMessages);
  }

  private FieldValidations() {}
}

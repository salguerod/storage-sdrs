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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/** Exposes a list of validation messages */
public class ValidationResult {

  /**
   * An immutable list of user actionable changes required to pass a validation check. If it is
   * empty, it should be treated as valid.
   */
  public final List<String> validationMessages;

  /** Creates a ValidationResult */
  public ValidationResult(List<String> validationMessages) {
    this.validationMessages = Collections.unmodifiableList(validationMessages);
  }

  /** Create a ValidationResult from a composite of partial results. */
  public static ValidationResult compose(List<ValidationResult> validationResults) {
    List<String> partialMessages = new LinkedList<>();
    for (ValidationResult validationResult : validationResults) {
      partialMessages.addAll(validationResult.validationMessages);
    }
    return new ValidationResult(partialMessages);
  }

  /** Convenience method for creating a ValidationResult with a single validation message */
  public static ValidationResult fromString(String validationMessage) {
    List<String> messages = new LinkedList<>();
    messages.add(validationMessage);
    return new ValidationResult(messages);
  }
}

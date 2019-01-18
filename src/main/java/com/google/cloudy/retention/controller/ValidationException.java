package com.google.cloudy.retention.controller;

import com.google.cloudy.retention.controller.validation.ValidationResult;
import java.util.List;

/**
 * Exception thrown in case of validation errors. Supports messages including multiple identified
 * errors.
 */
public class ValidationException extends HttpException {

  private List<String> validationMessages;

  /** Constructs a ValidationException based off of values within a ValidationResult object */
  public ValidationException(ValidationResult validationResult) {
    validationMessages = validationResult.validationMessages;
  }

  /** Gets the error message including all tracked errors */
  @Override
  public String getMessage() {
    String messages = String.join(", ", validationMessages);
    return String.format("Invalid input: %s", messages);
  }

  /** Gets the validation error HTTP status code */
  @Override
  public int getStatusCode() {
    return 400;
  }
}

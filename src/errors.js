export class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class PreconditionFailedError extends Error {
  constructor(message, expectedEtag = null) {
    super(message);
    this.name = this.constructor.name;
    this.expectedEtag = expectedEtag;
  }
}

export class AuthTokenTimestampValidationError extends Error {
  constructor(message, oldestValidTokenTimestamp) {
    super(message);
    this.name = this.constructor.name;
    this.oldestValidTokenTimestamp = oldestValidTokenTimestamp;
  }
}

export class DoesNotExist extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class ConflictError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class BadPathError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class NotEnoughProofError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class InvalidInputError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class PayloadTooLargeError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
  }
}

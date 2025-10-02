/**
 * Storage error types
 */

export class StorageError extends Error {
  public readonly code: string;
  public readonly isRetryable: boolean;
  public readonly details?: any;

  constructor(
    message: string,
    code: string = 'STORAGE_ERROR',
    isRetryable: boolean = false,
    details?: any
  ) {
    super(message);
    this.name = 'StorageError';
    this.code = code;
    this.isRetryable = isRetryable;
    this.details = details;
  }
}

export class WALError extends StorageError {
  constructor(
    message: string,
    isRetryable: boolean = false,
    details?: any
  ) {
    super(message, 'WAL_ERROR', isRetryable, details);
    this.name = 'WALError';
  }
}

export class SegmentError extends StorageError {
  constructor(
    message: string,
    isRetryable: boolean = false,
    details?: any
  ) {
    super(message, 'SEGMENT_ERROR', isRetryable, details);
    this.name = 'SegmentError';
  }
}

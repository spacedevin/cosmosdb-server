/**
 * WAL entry types
 */

export interface WALEntry {
  sequenceNumber: number;
  timestamp: number;
  operation: 'CREATE' | 'UPDATE' | 'DELETE';
  partitionKey: string;
  documentId: string;
  data?: any;
  checksum?: string;
}

export interface WALMetadata {
  totalEntries: number;
  lastSequenceNumber: number;
  createdAt: number;
  lastModified: number;
}

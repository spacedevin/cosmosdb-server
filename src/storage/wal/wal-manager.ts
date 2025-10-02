/**
 * Write-Ahead Log Manager for crash safety
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import * as crypto from 'crypto';
import { WALEntry, WALMetadata } from './wal-entry';
import { WALError } from '../../utils/error-types';

export interface WALResult {
  success: boolean;
  error?: string;
  sequenceNumber?: number;
  fallbackToMemory?: boolean;
}

export interface WALReplayResult {
  entriesReplayed: number;
  corruptedEntries: number;
  recoveryActions: string[];
}

export class WALManager {
  private walFile: string;
  private metadataFile: string;
  private sequenceNumber = 0;
  private applyCallback?: (entry: WALEntry) => Promise<void>;
  private initialized = false;

  constructor(private walDir: string) {
    this.walFile = path.join(walDir, 'wal.log');
    this.metadataFile = path.join(walDir, 'wal.meta');
  }

  /**
   * Initialize the WAL manager
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      // Ensure WAL directory exists
      await fs.mkdir(this.walDir, { recursive: true });

      // Load metadata
      await this.loadMetadata();

      this.initialized = true;
    } catch (error) {
      throw new WALError(
        `Failed to initialize WAL: ${error}`,
        false,
        { error }
      );
    }
  }

  /**
   * Set callback for applying WAL entries during replay
   */
  setApplyCallback(callback: (entry: WALEntry) => Promise<void>): void {
    this.applyCallback = callback;
  }

  /**
   * Append an entry to the WAL
   */
  async append(entry: Omit<WALEntry, 'sequenceNumber' | 'timestamp' | 'checksum'>): Promise<WALResult> {
    try {
      const walEntry: WALEntry = {
        ...entry,
        sequenceNumber: ++this.sequenceNumber,
        timestamp: Date.now(),
        checksum: this.calculateChecksum(entry)
      };

      // Serialize the entry
      const serialized = this.safeStringify(walEntry) + '\n';

      try {
        // Append to WAL file
        await fs.appendFile(this.walFile, serialized);

        // Update metadata
        await this.updateMetadata();

        return {
          success: true,
          sequenceNumber: walEntry.sequenceNumber
        };
      } catch (writeError) {
        console.warn('WAL write failed, falling back to memory-only mode:', writeError);
        return {
          success: false,
          error: `Write failed: ${writeError}`,
          fallbackToMemory: true
        };
      }
    } catch (error) {
      return {
        success: false,
        error: `WAL append error: ${error}`
      };
    }
  }

  /**
   * Replay WAL entries for recovery
   */
  async replay(): Promise<WALReplayResult> {
    const result: WALReplayResult = {
      entriesReplayed: 0,
      corruptedEntries: 0,
      recoveryActions: []
    };

    try {
      // Check if WAL file exists
      try {
        await fs.access(this.walFile);
      } catch {
        // No WAL file to replay
        return result;
      }

      // Read WAL file
      const walContent = await fs.readFile(this.walFile, 'utf8');
      const lines = walContent.split('\n').filter(line => line.trim());

      for (const line of lines) {
        try {
          const entry: WALEntry = JSON.parse(line);

          // Validate checksum
          if (entry.checksum && !this.validateChecksum(entry)) {
            result.corruptedEntries++;
            result.recoveryActions.push(`Skipped corrupted entry: ${entry.sequenceNumber}`);
            continue;
          }

          // Apply entry if callback is set
          if (this.applyCallback) {
            await this.applyCallback(entry);
          }

          result.entriesReplayed++;

          // Update sequence number
          if (entry.sequenceNumber > this.sequenceNumber) {
            this.sequenceNumber = entry.sequenceNumber;
          }
        } catch (parseError) {
          result.corruptedEntries++;
          result.recoveryActions.push(`Failed to parse WAL entry: ${parseError}`);
        }
      }

      // Clear WAL after successful replay
      if (result.entriesReplayed > 0) {
        await this.clear();
        result.recoveryActions.push(`Cleared WAL after replaying ${result.entriesReplayed} entries`);
      }

    } catch (error) {
      throw new WALError(`WAL replay failed: ${error}`, false, { error });
    }

    return result;
  }

  /**
   * Clear the WAL
   */
  async clear(): Promise<void> {
    try {
      // Remove WAL file if it exists
      try {
        await fs.unlink(this.walFile);
      } catch {
        // File doesn't exist, that's fine
      }

      // Reset sequence number
      this.sequenceNumber = 0;

      // Update metadata
      await this.updateMetadata();
    } catch (error) {
      throw new WALError(`Failed to clear WAL: ${error}`, false, { error });
    }
  }

  /**
   * Close the WAL manager
   */
  async close(): Promise<void> {
    // WAL is stateless, just mark as not initialized
    this.initialized = false;
    this.applyCallback = undefined;
  }

  /**
   * Load metadata from file
   */
  private async loadMetadata(): Promise<void> {
    try {
      const metadataContent = await fs.readFile(this.metadataFile, 'utf8');
      const metadata: WALMetadata = JSON.parse(metadataContent);
      this.sequenceNumber = metadata.lastSequenceNumber;
    } catch {
      // No metadata file exists, start fresh
      this.sequenceNumber = 0;
      await this.updateMetadata();
    }
  }

  /**
   * Update metadata file
   */
  private async updateMetadata(): Promise<void> {
    const metadata: WALMetadata = {
      totalEntries: this.sequenceNumber,
      lastSequenceNumber: this.sequenceNumber,
      createdAt: Date.now(),
      lastModified: Date.now()
    };

    try {
      await fs.writeFile(this.metadataFile, JSON.stringify(metadata, null, 2));
    } catch (error) {
      console.warn('Failed to update WAL metadata:', error);
      // Don't throw - this is not critical
    }
  }

  /**
   * Calculate checksum for WAL entry
   */
  private calculateChecksum(entry: Omit<WALEntry, 'sequenceNumber' | 'timestamp' | 'checksum'>): string {
    const data = this.safeStringify({
      operation: entry.operation,
      partitionKey: entry.partitionKey,
      documentId: entry.documentId,
      data: entry.data
    });
    return crypto.createHash('sha256').update(data).digest('hex');
  }

  /**
   * Safe JSON stringify that handles circular references
   */
  private safeStringify(obj: any): string {
    const seen = new WeakSet();
    return JSON.stringify(obj, (key, value) => {
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) {
          return '[Circular]';
        }
        seen.add(value);
      }
      return value;
    });
  }

  /**
   * Validate checksum for WAL entry
   */
  private validateChecksum(entry: WALEntry): boolean {
    if (!entry.checksum) return true; // No checksum to validate

    const expectedChecksum = this.calculateChecksum(entry);
    return expectedChecksum === entry.checksum;
  }
}

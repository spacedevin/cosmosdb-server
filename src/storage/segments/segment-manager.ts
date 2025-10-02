/**
 * Segment Manager for efficient storage and retrieval
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import * as crypto from 'crypto';
import * as zlib from 'zlib';
import { promisify } from 'util';
import { SegmentError } from '../../utils/error-types';

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

export interface SegmentOptions {
  compressionEnabled?: boolean;
  maxSegmentSize?: number;
  cacheSize?: number;
}

export interface SegmentReadResult<T> {
  success: boolean;
  document?: T;
  error?: string;
}

export interface SegmentIndex {
  [key: string]: {
    segmentId: string;
    offset: number;
    length: number;
    deleted?: boolean;
  };
}

/**
 * Manages document storage in segments with optional compression
 */
export class SegmentManager {
  private segmentDir: string;
  private indexFile: string;
  private index: SegmentIndex = {};
  private currentSegmentId = 0;
  private currentSegmentSize = 0;
  private writeQueue: Array<{ key: string; data: any }> = [];
  private writePromise?: Promise<void>;
  private initialized = false;

  constructor(
    baseDir: string,
    private options: SegmentOptions = {}
  ) {
    this.segmentDir = baseDir; // Use baseDir directly since it already points to segments directory
    this.indexFile = path.join(baseDir, 'index.json');
    this.options = {
      compressionEnabled: false,
      maxSegmentSize: 64 * 1024 * 1024, // 64MB
      cacheSize: 10000,
      ...options
    };
  }

  /**
   * Initialize the segment manager
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      // Ensure segment directory exists
      await fs.mkdir(this.segmentDir, { recursive: true });

      // Load index
      await this.loadIndex();

      // Find current segment ID
      await this.findCurrentSegmentId();

      this.initialized = true;
    } catch (error) {
      throw new SegmentError(
        `Failed to initialize segment manager: ${error}`,
        false,
        { error }
      );
    }
  }

  /**
   * Read a document from segments
   */
  async read<T>(key: string): Promise<SegmentReadResult<T>> {
    try {
      const indexEntry = this.index[key];
      if (!indexEntry || indexEntry.deleted) {
        return { success: true, document: undefined };
      }

      // Try both the correct path and the legacy nested path
      let segmentFile = path.join(this.segmentDir, `${indexEntry.segmentId}.seg`);
      
      // Check if file exists, if not try the legacy nested path
      try {
        await fs.access(segmentFile);
      } catch {
        // Try legacy nested path
        segmentFile = path.join(this.segmentDir, 'segments', `${indexEntry.segmentId}.seg`);
        try {
          await fs.access(segmentFile);
        } catch {
          return { success: false, error: `Segment file not found: ${segmentFile}` };
        }
      }

      // Read segment data
      const handle = await fs.open(segmentFile, 'r');
      const buffer = Buffer.alloc(indexEntry.length);
      await handle.read(buffer, 0, indexEntry.length, indexEntry.offset);
      await handle.close();

      // Decompress if needed
      let data: Buffer;
      if (this.options.compressionEnabled) {
        data = await gunzip(buffer);
      } else {
        data = buffer;
      }

      // Parse document - handle both old corrupted format and new format
      const rawData = data.toString('utf8').trim();
      
      try {
        const parsed = JSON.parse(rawData);
        
        // If the parsed data has _data property, extract it (legacy format)
        if (parsed && typeof parsed === 'object' && parsed._data) {
          return { success: true, document: parsed._data as T };
        }
        
        // Otherwise use the parsed data directly (new format)
        return { success: true, document: parsed as T };
      } catch (parseError) {
        return {
          success: false,
          error: `Failed to parse document ${key}: ${parseError}`
        };
      }
    } catch (error) {
      return {
        success: false,
        error: `Failed to read document ${key}: ${error}`
      };
    }
  }

  /**
   * Queue a write operation
   */
  async queueWrite(key: string, data: any): Promise<void> {
    this.writeQueue.push({ key, data });

    // Start write processing if not already running
    if (!this.writePromise) {
      this.writePromise = this.processWriteQueue();
    }

    return this.writePromise;
  }

  /**
   * Mark a document as deleted
   */
  async markDeleted(key: string): Promise<void> {
    if (this.index[key]) {
      this.index[key].deleted = true;
      await this.saveIndex();
    }
  }

  /**
   * Get all keys (excluding deleted)
   */
  async getAllKeys(): Promise<string[]> {
    return Object.keys(this.index).filter(key => !this.index[key].deleted);
  }

  /**
   * Clear all segments and index
   */
  async clear(): Promise<void> {
    try {
      // Clear index
      this.index = {};
      this.currentSegmentId = 0;
      this.currentSegmentSize = 0;

      // Remove all segment files
      try {
        const files = await fs.readdir(this.segmentDir);
        for (const file of files) {
          if (file.endsWith('.seg')) {
            await fs.unlink(path.join(this.segmentDir, file));
          }
        }
      } catch {
        // Directory might not exist
      }

      // Save empty index
      await this.saveIndex();
    } catch (error) {
      throw new SegmentError(`Failed to clear segments: ${error}`, false, { error });
    }
  }

  /**
   * Close the segment manager
   */
  async close(): Promise<void> {
    // Wait for any pending writes
    if (this.writePromise) {
      await this.writePromise;
    }

    // Save index
    await this.saveIndex();

    this.initialized = false;
  }

  /**
   * Process the write queue
   */
  private async processWriteQueue(): Promise<void> {
    try {
      while (this.writeQueue.length > 0) {
        const batch = this.writeQueue.splice(0, 100); // Process in batches
        await this.writeBatch(batch);
      }
    } finally {
      this.writePromise = undefined;
    }
  }

  /**
   * Write a batch of documents
   */
  private async writeBatch(batch: Array<{ key: string; data: any }>): Promise<void> {
    for (const { key, data } of batch) {
      await this.writeDocument(key, data);
    }

    // Save index after each batch
    await this.saveIndex();
  }

  /**
   * Write a single document
   */
  private async writeDocument(key: string, data: any): Promise<void> {
    try {
      // Extract the actual data if it's wrapped in _data
      let actualData = data;
      if (data && typeof data === 'object' && data._data) {
        actualData = data._data;
      }
      
      // Serialize data with proper newline separation
      const serialized = JSON.stringify(actualData);
      let buffer = Buffer.from(serialized + '\n', 'utf8');

      // Compress if enabled
      if (this.options.compressionEnabled) {
        buffer = Buffer.from(await gzip(buffer));
      }

      // Check if we need a new segment
      if (this.currentSegmentSize + buffer.length > this.options.maxSegmentSize!) {
        this.currentSegmentId++;
        this.currentSegmentSize = 0;
      }

      const segmentFile = path.join(this.segmentDir, `${this.currentSegmentId}.seg`);
      const offset = this.currentSegmentSize;

      // Append to segment file
      await fs.appendFile(segmentFile, buffer);

      // Update index
      this.index[key] = {
        segmentId: this.currentSegmentId.toString(),
        offset,
        length: buffer.length,
        deleted: false
      };

      this.currentSegmentSize += buffer.length;
    } catch (error) {
      throw new SegmentError(
        `Failed to write document ${key}: ${error}`,
        true,
        { error }
      );
    }
  }

  /**
   * Load index from file
   */
  private async loadIndex(): Promise<void> {
    try {
      const indexContent = await fs.readFile(this.indexFile, 'utf8');
      this.index = JSON.parse(indexContent);
    } catch {
      // No index file exists, start fresh
      this.index = {};
      await this.saveIndex();
    }
  }

  /**
   * Save index to file
   */
  private async saveIndex(): Promise<void> {
    try {
      await fs.writeFile(this.indexFile, JSON.stringify(this.index, null, 2));
    } catch (error) {
      throw new SegmentError(`Failed to save index: ${error}`, false, { error });
    }
  }

  /**
   * Find the current segment ID by examining existing files
   */
  private async findCurrentSegmentId(): Promise<void> {
    try {
      const files = await fs.readdir(this.segmentDir);
      let maxSegmentId = 0;

      for (const file of files) {
        if (file.endsWith('.seg')) {
          const segmentId = parseInt(file.replace('.seg', ''), 10);
          if (segmentId > maxSegmentId) {
            maxSegmentId = segmentId;
          }
        }
      }

      this.currentSegmentId = maxSegmentId;

      // Calculate current segment size
      if (this.currentSegmentId > 0) {
        const segmentFile = path.join(this.segmentDir, `${this.currentSegmentId}.seg`);
        try {
          const stats = await fs.stat(segmentFile);
          this.currentSegmentSize = stats.size;
        } catch {
          this.currentSegmentSize = 0;
        }
      }
    } catch {
      // Directory doesn't exist or is empty
      this.currentSegmentId = 0;
      this.currentSegmentSize = 0;
    }
  }
}

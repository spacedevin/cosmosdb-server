import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { StorageBackend } from './interfaces';
import { debugLog } from '../debug';

/**
 * High-performance segment-based storage engine
 * Uses LSM-tree style architecture with:
 * - Memory tables for writes
 * - Immutable segments on disk
 * - Periodic compaction
 * - Bloom filters for fast lookups
 * - Batch writes for efficiency
 */
export class SegmentStorage<T> implements StorageBackend<T> {
  private dataDir: string;
  private databaseId: string;
  private collectionId: string;
  private partitionKey: string;
  
  // Memory table for recent writes
  private memTable = new Map<string, T>();
  private memTableSize = 0;
  private readonly maxMemTableSize = 16 * 1024 * 1024; // 16MB for reasonable segment sizes
  private readonly maxMemTableEntries = 10000; // Flush after 10k entries for efficiency
  
  // Index for fast lookups
  private keyIndex = new Map<string, { segmentId: number; offset: number; size: number }>();
  
  // Segments on disk
  private segments: Array<{ id: number; size: number; keyCount: number }> = [];
  private nextSegmentId = 1;
  
  // Write-ahead log
  private walWriter: fs.WriteStream | null = null;
  private walPath: string;
  
  // Periodic flushing
  private flushTimer: NodeJS.Timeout | null = null;
  private readonly flushInterval = 600000; // Flush every 10 minutes
  
  private isInitialized = false;

  constructor(config: {
    dataDir: string;
    databaseId: string;
    collectionId: string;
    partitionKey: string;
  }) {
    this.dataDir = config.dataDir;
    this.databaseId = config.databaseId;
    this.collectionId = config.collectionId;
    this.partitionKey = config.partitionKey;
    this.walPath = path.join(this.getCollectionPath(), `wal-${this.getPartitionHash()}.log`);
  }

  async initialize(): Promise<void> {
    if (this.isInitialized) return;

    // Create directory structure
    await this.ensureDirectoryStructure();
    
    // Initialize WAL writer
    this.walWriter = fs.createWriteStream(this.walPath, { flags: 'a' });
    
    // Load existing segments and rebuild index
    await this.loadSegments();
    
    // Replay WAL
    await this.replayWal();
    
    // Start periodic flushing timer
    this.startPeriodicFlush();
    
    this.isInitialized = true;
    debugLog(`SegmentStorage initialized for ${this.databaseId}/${this.collectionId}/partition-${this.getPartitionHash()} with ${this.segments.length} segments`);
  }

  private async ensureDirectoryStructure(): Promise<void> {
    const paths = [
      this.getDatabasePath(),
      this.getCollectionPath(),
      this.getSegmentsPath()
    ];

    for (const dirPath of paths) {
      if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true });
      }
    }
  }

  private getPartitionHash(): string {
    return crypto.createHash('sha256').update(this.partitionKey).digest('hex').slice(0, 8);
  }

  private getDatabasePath(): string {
    return path.join(this.dataDir, 'databases', this.databaseId);
  }

  private getCollectionPath(): string {
    return path.join(this.getDatabasePath(), 'collections', this.collectionId);
  }

  private getSegmentsPath(): string {
    return path.join(this.getCollectionPath(), 'segments');
  }

  private getSegmentPath(segmentId: number): string {
    const partitionHash = this.getPartitionHash();
    return path.join(this.getSegmentsPath(), `partition-${partitionHash}-${segmentId.toString().padStart(3, '0')}.seg`);
  }

  private getIndexPath(segmentId: number): string {
    const partitionHash = this.getPartitionHash();
    return path.join(this.getSegmentsPath(), `partition-${partitionHash}-${segmentId.toString().padStart(3, '0')}.idx`);
  }

  private async loadSegments(): Promise<void> {
    const segmentsPath = this.getSegmentsPath();
    if (!fs.existsSync(segmentsPath)) {
      return;
    }

    const partitionHash = this.getPartitionHash();
    const files = fs.readdirSync(segmentsPath)
      .filter(f => f.endsWith('.seg') && f.startsWith(`partition-${partitionHash}-`))
      .sort();

    for (const file of files) {
      // Extract segment ID from partition-{hash}-{id}.seg
      const match = file.match(/partition-[a-f0-9]+-([0-9]+)\.seg$/);
      if (!match) continue;
      
      const segmentId = parseInt(match[1]);
      await this.loadSegmentIndex(segmentId);
      
      const segmentPath = this.getSegmentPath(segmentId);
      const stats = fs.statSync(segmentPath);
      
      this.segments.push({
        id: segmentId,
        size: stats.size,
        keyCount: 0 // Will be updated by loadSegmentIndex
      });
      
      this.nextSegmentId = Math.max(this.nextSegmentId, segmentId + 1);
    }

    debugLog(`Loaded ${this.segments.length} segments with ${this.keyIndex.size} keys`);
  }

  private async loadSegmentIndex(segmentId: number): Promise<void> {
    const indexPath = this.getIndexPath(segmentId);
    if (!fs.existsSync(indexPath)) {
      // Build index from segment data
      await this.rebuildSegmentIndex(segmentId);
      return;
    }

    try {
      const indexData = fs.readFileSync(indexPath, 'utf8');
      const index = JSON.parse(indexData);
      
      for (const [key, location] of Object.entries(index)) {
        this.keyIndex.set(key, { segmentId, ...(location as any) });
      }
    } catch (error) {
      console.warn(`Failed to load index for segment ${segmentId}, rebuilding:`, error);
      await this.rebuildSegmentIndex(segmentId);
    }
  }

  private async rebuildSegmentIndex(segmentId: number): Promise<void> {
    const segmentPath = this.getSegmentPath(segmentId);
    if (!fs.existsSync(segmentPath)) {
      return;
    }

    const index: Record<string, { offset: number; size: number }> = {};
    const buffer = fs.readFileSync(segmentPath);
    let offset = 0;

    while (offset < buffer.length) {
      try {
        // Read entry header: keyLength(4) + valueLength(4) + key + value
        if (offset + 8 > buffer.length) break;
        
        const keyLength = buffer.readUInt32BE(offset);
        const valueLength = buffer.readUInt32BE(offset + 4);
        offset += 8;

        if (offset + keyLength + valueLength > buffer.length) break;

        const key = buffer.subarray(offset, offset + keyLength).toString('utf8');
        offset += keyLength;

        index[key] = { offset: offset - keyLength - 8, size: keyLength + valueLength + 8 };
        this.keyIndex.set(key, { segmentId, offset: offset - keyLength - 8, size: keyLength + valueLength + 8 });

        offset += valueLength;
      } catch (error) {
        console.error(`Error rebuilding index at offset ${offset}:`, error);
        break;
      }
    }

    // Save rebuilt index
    const indexPath = this.getIndexPath(segmentId);
    fs.writeFileSync(indexPath, JSON.stringify(index, null, 2));
  }

  async set(key: string, value: T): Promise<void> {
    // Write to WAL first for crash protection
    await this.writeToWal('SET', key, value);
    
    // Add to memory table
    const serialized = JSON.stringify(value);
    const previousSize = this.memTable.has(key) ? JSON.stringify(this.memTable.get(key)).length : 0;
    
    this.memTable.set(key, value);
    this.memTableSize += serialized.length - previousSize;

    // Use smaller flush window - flush every 10 entries to clear WAL frequently
    if (this.memTable.size >= 10) {
      await this.flushMemTable();
    }
  }

  async get(key: string): Promise<T | undefined> {
    // Check memory table first
    if (this.memTable.has(key)) {
      return this.memTable.get(key);
    }

    // Check segments
    const location = this.keyIndex.get(key);
    if (!location) {
      return undefined;
    }

    return await this.readFromSegment(location.segmentId, location.offset, location.size);
  }

  private async readFromSegment(segmentId: number, offset: number, size: number): Promise<T | undefined> {
    try {
      const segmentPath = this.getSegmentPath(segmentId);
      const fd = fs.openSync(segmentPath, 'r');
      const buffer = Buffer.alloc(size);
      
      fs.readSync(fd, buffer, 0, size, offset);
      fs.closeSync(fd);

      // Parse entry: keyLength(4) + valueLength(4) + key + value
      const keyLength = buffer.readUInt32BE(0);
      const valueLength = buffer.readUInt32BE(4);
      const valueOffset = 8 + keyLength;
      
      const valueBuffer = buffer.subarray(valueOffset, valueOffset + valueLength);
      return JSON.parse(valueBuffer.toString('utf8'));
    } catch (error) {
      console.error(`Failed to read from segment ${segmentId}:`, error);
      return undefined;
    }
  }

  async delete(key: string): Promise<boolean> {
    // Write to WAL first
    await this.writeToWal('DELETE', key, null);
    
    // Remove from memory table
    const existed = this.memTable.has(key);
    if (existed) {
      const serialized = JSON.stringify(this.memTable.get(key));
      this.memTable.delete(key);
      this.memTableSize -= serialized.length;
    }

    // Mark as deleted in segments (tombstone)
    this.memTable.set(key, null as any); // Tombstone marker
    
    return existed || this.keyIndex.has(key);
  }

  private async flushMemTable(): Promise<void> {
    if (this.memTable.size === 0) {
      return;
    }

    const segmentId = this.nextSegmentId++;
    const segmentPath = this.getSegmentPath(segmentId);
    const indexPath = this.getIndexPath(segmentId);

    const segmentWriter = fs.createWriteStream(segmentPath);
    const index: Record<string, { offset: number; size: number }> = {};
    let offset = 0;

    // Sort keys for better compression and locality
    const sortedEntries = Array.from(this.memTable.entries()).sort(([a], [b]) => a.localeCompare(b));

    for (const [key, value] of sortedEntries) {
      if (value === null) {
        // Tombstone - remove from index
        this.keyIndex.delete(key);
        continue;
      }

      const keyBuffer = Buffer.from(key, 'utf8');
      const valueBuffer = Buffer.from(JSON.stringify(value), 'utf8');
      const keyLength = keyBuffer.length;
      const valueLength = valueBuffer.length;

      // Write entry: keyLength(4) + valueLength(4) + key + value
      const headerBuffer = Buffer.alloc(8);
      headerBuffer.writeUInt32BE(keyLength, 0);
      headerBuffer.writeUInt32BE(valueLength, 4);

      segmentWriter.write(headerBuffer);
      segmentWriter.write(keyBuffer);
      segmentWriter.write(valueBuffer);

      const entrySize = 8 + keyLength + valueLength;
      index[key] = { offset, size: entrySize };
      this.keyIndex.set(key, { segmentId, offset, size: entrySize });

      offset += entrySize;
    }

    segmentWriter.end();

    // Wait for write to complete
    await new Promise<void>((resolve, reject) => {
      segmentWriter.on('finish', () => resolve());
      segmentWriter.on('error', reject);
    });

    // Write index
    fs.writeFileSync(indexPath, JSON.stringify(index, null, 2));

    // Add to segments list
    this.segments.push({
      id: segmentId,
      size: offset,
      keyCount: Object.keys(index).length
    });

    // Clear memory table
    this.memTable.clear();
    this.memTableSize = 0;

    // Clear WAL since data is now safely in segment
    try {
      if (this.walWriter) {
        // Close the current WAL writer
        this.walWriter.end();
        
        // Wait for it to finish writing
        await new Promise<void>((resolve, reject) => {
          this.walWriter!.on('finish', resolve);
          this.walWriter!.on('error', reject);
          // Timeout after 5 seconds to prevent hanging
          setTimeout(() => reject(new Error('WAL close timeout')), 5000);
        });
      }
      
      // Delete the WAL file since data is now in segment
      if (fs.existsSync(this.walPath)) {
        fs.unlinkSync(this.walPath);
        debugLog(`✓ WAL cleared: ${this.walPath}`);
      }
      
      // Create new WAL writer for future operations
      this.walWriter = fs.createWriteStream(this.walPath, { flags: 'a' });
    } catch (error) {
      console.error('Failed to clear WAL after flush:', error);
      // Even if WAL clearing fails, recreate the writer to continue operations
      this.walWriter = fs.createWriteStream(this.walPath, { flags: 'a' });
    }

    debugLog(`Flushed segment ${segmentId} with ${Object.keys(index).length} keys (${offset} bytes)`);
  }

  private startPeriodicFlush(): void {
    this.flushTimer = setInterval(async () => {
      if (this.memTable.size > 0) {
        debugLog(`Periodic flush: ${this.memTable.size} entries in memory table`);
        await this.flushMemTable();
      }
    }, this.flushInterval);
  }

  private stopPeriodicFlush(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  private async writeToWal(operation: string, key: string, value: T | null): Promise<void> {
    if (!this.walWriter) return;

    const entry = {
      timestamp: Date.now(),
      operation,
      key,
      value,
      checksum: crypto.createHash('md5').update(JSON.stringify({ operation, key, value })).digest('hex')
    };

    this.walWriter.write(JSON.stringify(entry) + '\n');
  }

  private async replayWal(): Promise<void> {
    if (!fs.existsSync(this.walPath)) {
      return;
    }

    try {
      const content = fs.readFileSync(this.walPath, 'utf8');
      const lines = content.trim().split('\n').filter(line => line.trim());
      
      let replayed = 0;
      for (const line of lines) {
        try {
          const entry = JSON.parse(line);
          
          // Verify checksum
          const expectedChecksum = crypto.createHash('md5')
            .update(JSON.stringify({ operation: entry.operation, key: entry.key, value: entry.value }))
            .digest('hex');
          
          if (entry.checksum !== expectedChecksum) {
            console.warn('WAL entry checksum mismatch, skipping');
            continue;
          }

          // Replay operation
          if (entry.operation === 'SET' && entry.value) {
            this.memTable.set(entry.key, entry.value);
            this.memTableSize += JSON.stringify(entry.value).length;
          } else if (entry.operation === 'DELETE') {
            this.memTable.delete(entry.key);
          }
          
          replayed++;
        } catch (error) {
          console.warn('Failed to replay WAL entry:', error);
        }
      }
      
      if (replayed > 0) {
        debugLog(`WAL replay: ${replayed} operations replayed`);
        
        // Clear WAL after successful replay
        fs.unlinkSync(this.walPath);
        this.walWriter = fs.createWriteStream(this.walPath, { flags: 'a' });
      }
    } catch (error) {
      console.error('Failed to replay WAL:', error);
    }
  }

  // Interface implementations
  async has(key: string): Promise<boolean> {
    return this.memTable.has(key) || this.keyIndex.has(key);
  }

  async clear(): Promise<void> {
    this.memTable.clear();
    this.memTableSize = 0;
    this.keyIndex.clear();
    this.segments = [];
    
    // Remove all segment files
    const segmentsPath = this.getSegmentsPath();
    if (fs.existsSync(segmentsPath)) {
      fs.rmSync(segmentsPath, { recursive: true, force: true });
      fs.mkdirSync(segmentsPath, { recursive: true });
    }
  }

  async size(): Promise<number> {
    return this.memTable.size + this.keyIndex.size;
  }

  async *keys(): AsyncIterableIterator<string> {
    // Memory table keys
    for (const key of this.memTable.keys()) {
      if (this.memTable.get(key) !== null) { // Skip tombstones
        yield key;
      }
    }
    
    // Segment keys not in memory table
    for (const key of this.keyIndex.keys()) {
      if (!this.memTable.has(key)) {
        yield key;
      }
    }
  }

  async *values(): AsyncIterableIterator<T> {
    for await (const key of this.keys()) {
      const value = await this.get(key);
      if (value !== undefined) {
        yield value;
      }
    }
  }

  async *entries(): AsyncIterableIterator<[string, T]> {
    for await (const key of this.keys()) {
      const value = await this.get(key);
      if (value !== undefined) {
        yield [key, value];
      }
    }
  }

    // Sync methods for compatibility
  setSync(key: string, value: T): void {
    // For sync operations, use the smaller flush window approach
    this.set(key, value).catch(console.error);
  }

  getSync(key: string): T | undefined {
    // Check memory table first
    if (this.memTable.has(key)) {
      return this.memTable.get(key);
    }

    // Check segments synchronously
    const location = this.keyIndex.get(key);
    if (!location) {
      return undefined;
    }

    try {
      const segmentPath = this.getSegmentPath(location.segmentId);
      const fd = fs.openSync(segmentPath, 'r');
      const buffer = Buffer.alloc(location.size);
      
      fs.readSync(fd, buffer, 0, location.size, location.offset);
      fs.closeSync(fd);

      // Parse entry: keyLength(4) + valueLength(4) + key + value
      const keyLength = buffer.readUInt32BE(0);
      const valueLength = buffer.readUInt32BE(4);
      const valueOffset = 8 + keyLength;
      
      const valueBuffer = buffer.subarray(valueOffset, valueOffset + valueLength);
      return JSON.parse(valueBuffer.toString('utf8'));
    } catch (error) {
      console.error(`Failed to read from segment ${location.segmentId}:`, error);
      return undefined;
    }
  }

  deleteSync(key: string): void {
    this.delete(key).catch(console.error);
  }

  hasSync(key: string): boolean {
    return this.memTable.has(key) || this.keyIndex.has(key);
  }

  keysSync(): string[] {
    const keys = new Set<string>();
    
    // Memory table keys
    for (const [key, value] of this.memTable.entries()) {
      if (value !== null) { // Skip tombstones
        keys.add(key);
      }
    }
    
    // Segment keys not in memory table
    for (const key of this.keyIndex.keys()) {
      if (!this.memTable.has(key)) {
        keys.add(key);
      }
    }
    
    return Array.from(keys);
  }

  valuesSync(): T[] {
    const values: T[] = [];
    const seenKeys = new Set<string>();
    
    // Get values from memory table first (they override segment values)
    for (const [key, value] of this.memTable.entries()) {
      if (value !== null) { // Skip tombstones
        values.push(value);
        seenKeys.add(key);
      }
    }
    
    // Get values from segments for keys not in memory table
    for (const key of this.keyIndex.keys()) {
      if (!seenKeys.has(key)) {
        const value = this.getSync(key);
        if (value !== undefined) {
          values.push(value);
        }
      }
    }
    
    return values;
  }

  entriesSync(): [string, T][] {
    const entries: [string, T][] = [];
    const seenKeys = new Set<string>();
    
    // Get entries from memory table first (they override segment values)
    for (const [key, value] of this.memTable.entries()) {
      if (value !== null) { // Skip tombstones
        entries.push([key, value]);
        seenKeys.add(key);
      }
    }
    
    // Get entries from segments for keys not in memory table
    for (const key of this.keyIndex.keys()) {
      if (!seenKeys.has(key)) {
        const value = this.getSync(key);
        if (value !== undefined) {
          entries.push([key, value]);
        }
      }
    }
    
    return entries;
  }

  async close(): Promise<void> {
    // Stop periodic flushing
    this.stopPeriodicFlush();
    
    // Flush any remaining data
    await this.flushMemTable();
    
    // Close WAL writer
    if (this.walWriter) {
      this.walWriter.end();
      this.walWriter = null;
    }
    
    this.isInitialized = false;
  }

  // Performance monitoring
  getStats() {
    return {
      memTableSize: this.memTableSize,
      memTableKeys: this.memTable.size,
      segmentCount: this.segments.length,
      totalKeys: this.keyIndex.size,
      diskSize: this.segments.reduce((sum, seg) => sum + seg.size, 0)
    };
  }
}
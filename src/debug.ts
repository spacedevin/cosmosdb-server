/**
 * Debug logging utility
 * Provides conditional logging based on debug flag
 */

let debugEnabled = false;

/**
 * Set the global debug state
 */
export function setDebugEnabled(enabled: boolean): void {
  debugEnabled = enabled;
}

/**
 * Get the current debug state
 */
export function isDebugEnabled(): boolean {
  return debugEnabled;
}

/**
 * Debug-conditional console.log
 */
export function debugLog(...args: any[]): void {
  if (debugEnabled) {
    console.log(...args);
  }
}

/**
 * Debug-conditional console.warn  
 */
export function debugWarn(...args: any[]): void {
  if (debugEnabled) {
    console.warn(...args);
  }
}

/**
 * Debug-conditional console.error (always shows errors regardless of debug flag)
 */
export function debugError(...args: any[]): void {
  console.error(...args);
}
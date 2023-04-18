import { Readable } from 'stream';
import LRUCache from 'lru-cache';
import fetch from 'node-fetch';

import { logger } from './utils';
import * as errors from './errors';

const AUTH_TIMESTAMP_FILE_NAME = 'authTimestamp';

export class AuthTimestampCache {

  constructor(readUrlPrefix, driver, maxCacheSize) {
    this.currentCacheEvictions = 0;
    this.cache = new LRUCache({
      max: maxCacheSize,
      noDisposeOnSet: true,
      dispose: () => {
        this.currentCacheEvictions++
      },
    });
    this.readUrlPrefix = readUrlPrefix;
    this.driver = driver;

    const tenMinutes = 1000 * 60 * 10;
    this.setupCacheEvictionLogger(tenMinutes);
  }

  setupCacheEvictionLogger(timerInterval) {
    const evictionLogTimeout = setInterval(
      () => this.handleCacheEvictions(), timerInterval
    );
    evictionLogTimeout.unref();
  }

  handleCacheEvictions() {
    if (this.currentCacheEvictions > 0) {
      logger.warn(`Gaia authentication token timestamp cache evicted ${this.currentCacheEvictions} entries in the last 10 minutes. Consider increasing 'authTimestampCacheSize'.`);
      this.currentCacheEvictions = 0;
    }
  }

  getAuthTimestampFileDir(bucketAddress) {
    return `${bucketAddress}-auth`;
  }

  async readAuthTimestamp(bucketAddress) {

    const authTimestampDir = this.getAuthTimestampFileDir(bucketAddress);

    let fetchResponse;
    try {
      const authNumberFileUrl = `${this.readUrlPrefix}${authTimestampDir}/${AUTH_TIMESTAMP_FILE_NAME}`;
      fetchResponse = await fetch(authNumberFileUrl, {
        redirect: 'manual',
        headers: {
          'Cache-Control': 'no-cache',
        },
      });
    } catch (err) {
      // Catch any errors that may occur from network issues during `fetch` async operation.
      const errMsg = (err instanceof Error) ? err.message : err;
      throw new errors.ValidationError(`Error trying to fetch bucket authentication revocation timestamp: ${errMsg}`);
    }

    if (fetchResponse.ok) {
      try {
        const authNumberText = await fetchResponse.text();
        const authNumber = parseInt(authNumberText);
        if (Number.isFinite(authNumber)) {
          return authNumber;
        } else {
          throw new errors.ValidationError(`Bucket contained an invalid authentication revocation timestamp: ${authNumberText}`);
        }
      } catch (err) {
        // Catch any errors that may occur from network issues during `.text()` async operation.
        const errMsg = (err instanceof Error) ? err.message : err;
        throw new errors.ValidationError(`Error trying to read fetch stream of bucket authentication revocation timestamp: ${errMsg}`);
      }
    } else if (fetchResponse.status === 404) {
      // 404 incidates no revocation file has been created.
      return 0;
    } else {
      throw new errors.ValidationError(`Error trying to fetch bucket authentication revocation timestamp: server returned ${fetchResponse.status} - ${fetchResponse.statusText}`);
    }
  }

  async getAuthTimestamp(bucketAddress) {
    // First perform fast check if auth number exists in cache..
    let authTimestamp = this.cache.get(bucketAddress);
    if (authTimestamp) {
      return authTimestamp;
    }

    // Nothing in cache, perform slower driver read.
    authTimestamp = await this.readAuthTimestamp(bucketAddress);

    // Recheck cache for a larger timestamp to avoid race conditions from slow storage.
    const cachedTimestamp = this.cache.get(bucketAddress);
    if (cachedTimestamp && cachedTimestamp > authTimestamp) {
      authTimestamp = cachedTimestamp;
    }

    // Cache result for fast lookup later.
    this.cache.set(bucketAddress, authTimestamp);

    return authTimestamp;
  }

  async writeAuthTimestamp(bucketAddress, timestamp) {
    // Recheck cache for a larger timestamp to avoid race conditions from slow storage.
    let cachedTimestamp = this.cache.get(bucketAddress);
    if (cachedTimestamp && cachedTimestamp > timestamp) {
      return;
    }

    const authTimestampFileDir = this.getAuthTimestampFileDir(bucketAddress);

    // Convert our number to a Buffer.
    const contentBuffer = Buffer.from(timestamp.toString(), 'utf8');

    // Wrap the buffer in a stream for driver consumption.
    const contentStream = new Readable();
    contentStream.push(contentBuffer, 'utf8');
    contentStream.push(null); // Mark EOF

    await this.driver.performWrite({
      storageTopLevel: authTimestampFileDir,
      path: AUTH_TIMESTAMP_FILE_NAME,
      stream: contentStream,
      contentLength: contentBuffer.length,
      contentType: 'text/plain; charset=UTF-8'
    });

    // In a race condition, use the newest timestamp.
    cachedTimestamp = this.cache.get(bucketAddress);
    if (cachedTimestamp && cachedTimestamp > timestamp) {
      timestamp = cachedTimestamp;
    }

    this.cache.set(bucketAddress, timestamp);
  }

  async setAuthTimestamp(bucketAddress, timestamp) {
    await this.writeAuthTimestamp(bucketAddress, (timestamp | 0));
  }
}

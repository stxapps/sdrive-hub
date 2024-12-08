import { LRUCache } from 'lru-cache';

import { PUT_FILE } from './const';
import { isNumber } from './utils';

export class BlacklistCache {

  constructor(driver, maxCacheSize) {
    this.currentCacheEvictions = 0;
    this.cache = new LRUCache({
      max: maxCacheSize,
      noDisposeOnSet: true,
      dispose: () => {
        this.currentCacheEvictions++
      },
      ttl: 15 * 60 * 1000,
      ttlResolution: 60 * 1000,
    });
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
    if (this.currentCacheEvictions > this.cache.max) {
      console.warn(`Blacklist cache evicted ${this.currentCacheEvictions} entries in the last 10 minutes. Consider increasing 'blacklistCacheSize'.`);
    }
    this.currentCacheEvictions = 0;
  }

  async getBlacklistType(address) {
    let type = this.cache.get(address);
    if (isNumber(type)) return type;

    type = await this.driver.performReadBlacklistType({ address });

    this.cache.set(address, type);
    return type;
  }

  async isBlacklisted(address, performType) {
    const type = await this.getBlacklistType(address);
    if (type === 0) return false;
    if (type === 1) return true;
    if (type === 2 && performType === PUT_FILE) return true;
    return false;
  }
}

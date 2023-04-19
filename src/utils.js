import * as stream from 'stream';
import { promisify } from 'util';
import { customAlphabet } from 'nanoid';

const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

export const generateUniqueID = () => {
  const nanoid = customAlphabet(alphabet, 10);
  return nanoid();
};

export const pipelineAsync = promisify(stream.pipeline);

export const logger = {
  error: (msg) => console.error(msg),
  warn: (msg) => console.warn(msg),
  info: (msg) => console.log(msg),
  debug: (msg) => console.debug(msg),
};

export const megabytesToBytes = (megabytes) => {
  return megabytes * 1024 * 1024;
};

export const bytesToMegabytes = (bytes, decimals = 2) => {
  return Number.parseFloat((bytes / (1024 / 1024)).toFixed(decimals));
};

export const dateToUnixTimeSeconds = (date) => {
  return Math.round(date.getTime() / 1000);
};

export const monitorStreamProgress = (inputStream, progressCallback) => {
  // Create a PassThrough stream to monitor streaming chunk sizes.
  let monitoredContentSize = 0;
  const monitorStream = new stream.PassThrough({
    transform: (chunk, _encoding, callback) => {
      monitoredContentSize += chunk.length;
      try {
        progressCallback(monitoredContentSize, chunk.length);
        // Pass the chunk Buffer through, untouched. This takes the fast
        // path through the stream pipe lib.
        callback(null, chunk);
      } catch (error) {
        callback(error);
      }
    },
  });

  // Use the stream pipe API to monitor a stream with correct back pressure
  // handling. This avoids buffering entire streams in memory and hooks up
  // all the correct events for cleanup and error handling.
  // See https://nodejs.org/api/stream.html#stream_three_states
  //     https://nodejs.org/ja/docs/guides/backpressuring-in-streams/
  const monitorPipeline = pipelineAsync(inputStream, monitorStream);

  const result = {
    monitoredStream: monitorStream,
    pipelinePromise: monitorPipeline,
  };

  return result;
};

export class AsyncMutexScope {
  constructor() {
    this._opened = new Set();
  }

  openedCount() {
    return this._opened.size;
  }

  tryAcquire(id, spawnOwner) {
    if (this._opened.has(id)) {
      return false;
    }

    this._opened.add(id);

    try {
      const owner = spawnOwner();

      owner.finally(() => {
        this._opened.delete(id)
      });
    } catch (error) {
      this._opened.delete(id);
      throw error;
    }

    return true;
  }
}

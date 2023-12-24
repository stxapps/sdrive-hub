import * as stream from 'stream';
import { promisify } from 'util';
import { customAlphabet } from 'nanoid';

export const runAsyncWrapper = (callback) => {
  return function (req, res, next) {
    callback(req, res, next).catch(next);
  }
};

export const generateUniqueID = () => {
  const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
  const nanoid = customAlphabet(alphabet, 10);
  return nanoid();
};

export const pipelineAsync = promisify(stream.pipeline);

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

export const sample = (arr) => {
  return arr[Math.floor(Math.random() * arr.length)];
};

export const isObject = (val) => {
  return typeof val === 'object' && val !== null;
};

export const isString = (val) => {
  return typeof val === 'string' || val instanceof String;
};

export const isNumber = (val) => {
  return typeof val === 'number' && isFinite(val);
};

export const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

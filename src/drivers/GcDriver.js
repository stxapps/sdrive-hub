import { Storage } from '@google-cloud/storage';

import { BadPathError, InvalidInputError, DoesNotExist } from '../errors';
import { pipelineAsync, logger, dateToUnixTimeSeconds } from '../utils';

const isPathValid = (path) => {
  // for now, only disallow double dots.
  return !path.includes('..');
};

const formatETagFromMD5 = (md5Hash) => {
  const hex = Buffer.from(md5Hash, 'base64').toString('hex');
  const formatted = `"${hex}"`;
  return formatted;
};

const parseFileMetadataStat = (metadata) => {
  const lastModified = dateToUnixTimeSeconds(new Date(metadata.updated));
  const result = {
    exists: true,
    etag: formatETagFromMD5(metadata.md5Hash),
    contentType: metadata.contentType,
    contentLength: parseInt(metadata.size),
    lastModifiedDate: lastModified,
  };
  return result;
};

class GcDriver {

  constructor(config) {
    this.storage = new Storage();
    this.bucket = config.bucket;
    this.pageSize = config.pageSize ? config.pageSize : 100;
    this.cacheControl = config.cacheControl;
    this.initPromise = this.createIfNeeded();
    this.resumable = config.resumable || false;
    this.supportsETagMatching = false;
  }

  ensureInitialized() {
    return this.initPromise;
  }

  dispose() {
    return Promise.resolve();
  }

  getReadURLPrefix() {
    return `https://storage.googleapis.com/${this.bucket}/`;
  }

  async createIfNeeded() {
    try {
      const bucket = this.storage.bucket(this.bucket);
      const [exists] = await bucket.exists();
      if (!exists) {
        throw new Error('failed to initialize google cloud storage bucket');
      }
    } catch (err) {
      logger.error(`failed to connect to google cloud storage bucket: ${err}`);
      throw err;
    }
  }

  async deleteEmptyBucket() {
    const files = await this.listFiles({ pathPrefix: '' });
    if (files.entries.length > 0) {
      /* istanbul ignore next */
      throw new Error('Tried deleting non-empty bucket');
    }
    await this.storage.bucket(this.bucket).delete();
  }

  async listAllObjects(prefix, page, pageSize) {
    const opts = {
      prefix: prefix,
      maxResults: pageSize || this.pageSize,
      pageToken: page || undefined,
    };

    const getFilesResult = await new Promise((resolve, reject) => {
      this.storage
        .bucket(this.bucket)
        .getFiles(opts, (err, files, nextQuery) => {
          if (err) {
            reject(err);
          } else {
            resolve({ files, nextQuery });
          }
        });
    });
    const fileEntries = getFilesResult.files.map(file => {
      return {
        name: file.name.slice(prefix.length + 1),
        file: file,
      };
    });
    const result = {
      entries: fileEntries,
      page: (getFilesResult.nextQuery && getFilesResult.nextQuery.pageToken) || null,
    };
    return result;
  }

  async listFiles(args) {
    const listResult = await this.listAllObjects(args.pathPrefix, args.page);
    const result = {
      page: listResult.page,
      entries: listResult.entries.map(file => file.name),
    };
    return result;
  }

  async listFilesStat(args) {
    const listResult = await this.listAllObjects(
      args.pathPrefix, args.page, args.pageSize
    );
    const result = {
      page: listResult.page,
      entries: listResult.entries.map(entry => {
        const statResult = parseFileMetadataStat(entry.file.metadata);
        const entryResult = {
          ...statResult,
          name: entry.name,
          exists: true,
        }
        return entryResult;
      }),
    };
    return result;
  }

  async performWrite(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }
    if (args.contentType && args.contentType.length > 1024) {
      throw new InvalidInputError('Invalid content-type');
    }
    const filename = `${args.storageTopLevel}/${args.path}`;
    const publicURL = `${this.getReadURLPrefix()}${filename}`;

    const metadata = {};
    metadata.contentType = args.contentType;
    if (this.cacheControl) {
      metadata.cacheControl = this.cacheControl;
    }

    const fileDestination = this.storage
      .bucket(this.bucket)
      .file(filename);

    /*  > There is some overhead when using a resumable upload that can cause
        > noticeable performance degradation while uploading a series of small
        > files. When uploading files less than 10MB, it is recommended that
        > the resumable feature is disabled.
       For details see https://github.com/googleapis/nodejs-storage/issues/312 */

    const fileWriteStream = fileDestination.createWriteStream({
      public: true,
      resumable: this.resumable,
      metadata,
    });

    try {
      await pipelineAsync(args.stream, fileWriteStream);
      logger.debug(`storing ${filename} in bucket ${this.bucket}`);
      const etag = formatETagFromMD5(fileDestination.metadata.md5Hash);
      return { publicURL, etag };
    } catch (error) {
      logger.error(`failed to store ${filename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to store' +
        ` ${filename} in bucket ${this.bucket}: ${error}`);
    }
  }

  async performDelete(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }
    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename);

    try {
      await bucketFile.delete();
    } catch (error) {
      if (error.code === 404) {
        throw new DoesNotExist('File does not exist');
      }
      logger.error(`failed to delete ${filename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to delete' +
        ` ${filename} in bucket ${this.bucket}: ${error}`);
    }
  }

  async performRead(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }
    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename);
    try {
      const [getResult] = await bucketFile.get({ autoCreate: false });
      const statResult = parseFileMetadataStat(getResult.metadata);
      const dataStream = getResult.createReadStream();
      const result = {
        ...statResult,
        exists: true,
        data: dataStream
      };
      return result;
    } catch (error) {
      if (error.code === 404) {
        throw new DoesNotExist('File does not exist');
      }
      logger.error(`failed to read ${filename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to read' +
        ` ${filename} in bucket ${this.bucket}: ${error}`);
    }
  }

  async performStat(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid Path');
    }
    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename);
    try {
      const [metadataResult] = await bucketFile.getMetadata();
      const result = parseFileMetadataStat(metadataResult);
      return result;
    } catch (error) {
      if (error.code === 404) {
        const result = {
          exists: false,
        };
        return result;
      }
      logger.error(`failed to stat ${filename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to stat ' +
        ` ${filename} in bucket ${this.bucket}: ${error}`);
    }
  }

  async performRename(args) {
    if (!isPathValid(args.path)) {
      throw new BadPathError('Invalid original path');
    }
    if (!isPathValid(args.newPath)) {
      throw new BadPathError('Invalid new path');
    }

    const filename = `${args.storageTopLevel}/${args.path}`;
    const bucketFile = this.storage
      .bucket(this.bucket)
      .file(filename);

    const newFilename = `${args.storageTopLevel}/${args.newPath}`;
    const newBucketFile = this.storage
      .bucket(this.bucket)
      .file(newFilename);

    try {
      await bucketFile.move(newBucketFile);
    } catch (error) {
      if (error.code === 404) {
        throw new DoesNotExist('File does not exist');
      }
      logger.error(`failed to rename ${filename} to ${newFilename} in bucket ${this.bucket}`);
      throw new Error('Google cloud storage failure: failed to rename' +
        ` ${filename} to ${newFilename} in bucket ${this.bucket}: ${error}`);
    }
  }
}

const driver = GcDriver;
export default driver;

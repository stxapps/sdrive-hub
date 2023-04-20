import { validateAuthorizationHeader } from './authentication';
import {
  ValidationError, DoesNotExist, PayloadTooLargeError, PreconditionFailedError,
} from './errors';
import { AuthTimestampCache } from './revocations';
import {
  logger, generateUniqueID, bytesToMegabytes, megabytesToBytes, monitorStreamProgress,
} from './utils';

export class HubServer {

  constructor(driver, config) {
    this.driver = driver;
    this.config = config;
    this.whitelist = config.whitelist;
    this.serverName = config.serverName;
    this.validHubUrls = config.validHubUrls;
    this.readURL = config.readURL;
    this.requireCorrectHubUrl = config.requireCorrectHubUrl || false;
    this.authTimestampCache = new AuthTimestampCache(
      driver, config.authTimestampCacheSize
    );
    this.maxFileUploadSizeMB = (config.maxFileUploadSize || 20);
    // megabytes to bytes
    this.maxFileUploadSizeBytes = megabytesToBytes(this.maxFileUploadSizeMB);
  }

  async handleAuthBump(address, oldestValidTimestamp, requestHeaders) {
    this.validate(address, requestHeaders, null);
    await this.authTimestampCache.setAuthTimestamp(address, oldestValidTimestamp);
  }

  // throws exception on validation error
  //   otherwise returns void.
  validate(address, requestHeaders, oldestValidTokenTimestamp) {
    const authObject = validateAuthorizationHeader(
      requestHeaders.authorization,
      this.serverName,
      address,
      this.requireCorrectHubUrl,
      this.validHubUrls,
      oldestValidTokenTimestamp,
      this.whitelist,
    );
    return authObject;
  }

  async handleListFiles(address, page, stat, requestHeaders) {
    const oldestValidTokenTimestamp =
      await this.authTimestampCache.getAuthTimestamp(address);
    const authObject = this.validate(
      address, requestHeaders, oldestValidTokenTimestamp
    );

    const scopes = authObject.parseAuthScopes();
    const isArchivalRestricted = this.isArchivalRestricted(scopes);

    const listFilesArgs = {
      pathPrefix: address + '/', // to exclude ${address}-auth from revocation
      page: page
    };

    let listFileResult;
    if (stat) {
      listFileResult = await this.driver.listFilesStat(listFilesArgs);
    } else {
      listFileResult = await this.driver.listFiles(listFilesArgs);
    }

    // Filter historical files from results.
    if (isArchivalRestricted && listFileResult.entries.length > 0) {
      if (stat) {
        listFileResult.entries = listFileResult
          .entries
          .filter(entry => !this.isHistoricalFile(entry.name));
      } else {
        listFileResult.entries = listFileResult
          .entries
          .filter(entry => !this.isHistoricalFile(entry));
      }

      // Detect empty page due to all files being historical files.
      if (listFileResult.entries.length === 0 && listFileResult.page) {
        // Insert a null marker entry to indicate that there are more results
        // even though the entry array is empty.
        listFileResult.entries.push(null);
      }
    }

    return listFileResult;
  }

  getReadURLPrefix() {
    if (this.readURL) {
      return this.readURL;
    } else {
      return this.driver.getReadURLPrefix();
    }
  }

  getFileName(filePath) {
    const pathParts = filePath.split('/');
    const fileName = pathParts[pathParts.length - 1];
    return fileName;
  }

  getHistoricalFileName(filePath) {
    const fileName = this.getFileName(filePath);
    const filePathPrefix = filePath.slice(0, filePath.length - fileName.length);
    const historicalName = `.history.${Date.now()}.${generateUniqueID()}.${fileName}`;
    const historicalPath = `${filePathPrefix}${historicalName}`;
    return historicalPath;
  }

  isHistoricalFile(filePath) {
    const fileName = this.getFileName(filePath);
    const isHistoricalFile = fileName.startsWith('.history.');
    return isHistoricalFile;
  }

  async handleDelete(address, path, requestHeaders) {
    const oldestValidTokenTimestamp =
      await this.authTimestampCache.getAuthTimestamp(address);
    const authObject = this.validate(
      address, requestHeaders, oldestValidTokenTimestamp
    );

    // can the caller delete? if so, in what paths?
    const scopes = authObject.parseAuthScopes();
    const isArchivalRestricted = this.checkArchivalRestrictions(address, path, scopes);

    if (scopes.deletePrefixes.length > 0 || scopes.deletePaths.length > 0) {
      // we're limited to a set of prefixes and paths.
      // does the given path match any prefixes?
      let match = !!scopes.deletePrefixes.find((p) => (path.startsWith(p)));

      if (!match) {
        // check for exact paths
        match = !!scopes.deletePaths.find((p) => (path === p));
      }

      if (!match) {
        // not authorized to write to this path
        throw new ValidationError(`Address ${address} not authorized to delete from ${path} by scopes`);
      }
    }

    if (isArchivalRestricted) {
      // if archival restricted then just rename the canonical file to the historical file
      const historicalPath = this.getHistoricalFileName(path);
      const renameCommand = {
        path: path,
        storageTopLevel: address,
        newPath: historicalPath,
      };
      await this.driver.performRename(renameCommand);
    } else {
      const deleteCommand = {
        storageTopLevel: address,
        path,
      };
      await this.driver.performDelete(deleteCommand);
    }
  }

  async handleRequest(address, path, requestHeaders, stream) {
    const oldestValidTokenTimestamp =
      await this.authTimestampCache.getAuthTimestamp(address);
    const authObject = this.validate(
      address, requestHeaders, oldestValidTokenTimestamp
    );

    // can the caller write? if so, in what paths?
    const scopes = authObject.parseAuthScopes();
    const isArchivalRestricted = this.checkArchivalRestrictions(address, path, scopes);

    if (scopes.writePrefixes.length > 0 || scopes.writePaths.length > 0) {
      // we're limited to a set of prefixes and paths.
      // does the given path match any prefixes?
      let match = !!scopes.writePrefixes.find((p) => (path.startsWith(p)));

      if (!match) {
        // check for exact paths
        match = !!scopes.writePaths.find((p) => (path === p));
      }

      if (!match) {
        // not authorized to write to this path
        throw new ValidationError(`Address ${address} not authorized to write to ${path} by scopes`);
      }
    }

    const ifMatchTag = requestHeaders['if-match'];
    const ifNoneMatchTag = requestHeaders['if-none-match'];

    // only one match-tag header should be set
    if (ifMatchTag && ifNoneMatchTag) {
      throw new PreconditionFailedError('Request should not contain both if-match and if-none-match headers');
    }

    // only support using if-none-match for file creation, values that aren't the wildcard are prohibited
    if (ifNoneMatchTag && ifNoneMatchTag !== '*') {
      throw new PreconditionFailedError('Misuse of the if-none-match header. Expected to be * on write requests.');
    }
    // handle etag matching if not supported at the driver level
    if (!this.driver.supportsETagMatching) {
      // allow overwrites if tag is wildcard
      if (ifMatchTag && ifMatchTag !== '*') {
        const currentETag = (await this.driver.performStat({
          path: path,
          storageTopLevel: address
        })).etag;

        if (ifMatchTag !== currentETag) {
          throw new PreconditionFailedError('The provided ETag does not match that of the resource on the server', currentETag);
        }
      } else if (ifNoneMatchTag && ifNoneMatchTag === '*') {
        // only proceed with writing file if the file does not already exist
        const statResult = await this.driver.performStat({
          path: path,
          storageTopLevel: address
        });

        if (statResult.exists) {
          throw new PreconditionFailedError('The entity you are trying to create already exists');
        }
      }
    }

    let contentType = requestHeaders['content-type'];
    if (contentType === null || contentType === undefined) {
      contentType = 'application/octet-stream';
    }

    const contentLengthHeader = requestHeaders['content-length'];
    const contentLengthBytes = parseInt(contentLengthHeader);
    const isLengthFinite = Number.isFinite(contentLengthBytes) && contentLengthBytes > 0;

    // If a valid content-length is specified check to immediately return error
    if (isLengthFinite && contentLengthBytes > this.maxFileUploadSizeBytes) {
      const errMsg = (
        `Max file upload size is ${this.maxFileUploadSizeMB} megabytes. ` +
        `Rejected Content-Length of ${bytesToMegabytes(contentLengthBytes, 4)} megabytes`
      );
      logger.warn(`${errMsg}, address: ${address}`);
      throw new PayloadTooLargeError(errMsg);
    }

    if (isArchivalRestricted) {
      const historicalPath = this.getHistoricalFileName(path);
      try {
        await this.driver.performRename({
          path: path,
          storageTopLevel: address,
          newPath: historicalPath,
        });
      } catch (error) {
        if (error instanceof DoesNotExist) {
          // ignore
          logger.debug(
            '404 on putFileArchival rename attempt -- usually this is okay and ' +
            'only indicates that this is the first time the file was written: ' +
            `${address}/${path}`
          );
        } else {
          logger.error(`Error performing historical file rename: ${address}/${path}`);
          logger.error(error);
          throw error;
        }
      }
    }

    // Use the client reported content-length if available, otheriwse fallback to the
    // max configured length.
    const maxContentLength = (
      Number.isFinite(contentLengthBytes) && contentLengthBytes > 0
        ? contentLengthBytes : this.maxFileUploadSizeBytes
    );

    // Create a PassThrough stream to monitor streaming chunk sizes.
    const { monitoredStream, pipelinePromise } = monitorStreamProgress(
      stream,
      totalBytes => {
        if (totalBytes > maxContentLength) {
          const errMsg = (
            `Max file upload size is ${this.maxFileUploadSizeMB} megabytes. ` +
            `Rejected POST body stream of ${bytesToMegabytes(totalBytes, 4)} megabytes`
          );
          // Log error -- this situation is indicative of a malformed client request
          // where the reported Content-Size is less than the upload size.
          logger.warn(`${errMsg}, address: ${address}`);

          // Destroy the request stream -- cancels reading from the client
          // and cancels uploading to the storage driver.
          const error = new PayloadTooLargeError(errMsg);
          stream.destroy(error);
          throw error;
        }
      }
    );

    const writeCommand = {
      storageTopLevel: address,
      path,
      stream: monitoredStream,
      contentType,
      contentLength: contentLengthBytes,
      ifMatch: ifMatchTag,
      ifNoneMatch: ifNoneMatchTag,
    };

    const [writeResponse] = await Promise.all([
      this.driver.performWrite(writeCommand), pipelinePromise,
    ]);
    const readURL = writeResponse.publicURL;
    const driverPrefix = this.driver.getReadURLPrefix();
    const readURLPrefix = this.getReadURLPrefix();
    if (readURLPrefix !== driverPrefix && readURL.startsWith(driverPrefix)) {
      const postFix = readURL.slice(driverPrefix.length);
      return {
        publicURL: `${readURLPrefix}${postFix}`,
        etag: writeResponse.etag,
      };
    }
    return writeResponse;
  }

  isArchivalRestricted(scopes) {
    return (
      scopes.writeArchivalPaths.length > 0 || scopes.writeArchivalPrefixes.length > 0
    );
  }

  checkArchivalRestrictions(address, path, scopes) {
    const isArchivalRestricted = this.isArchivalRestricted(scopes);
    if (isArchivalRestricted) {
      // we're limited to a set of prefixes and paths.
      // does the given path match any prefixes?
      let match = !!scopes.writeArchivalPrefixes.find((p) => (path.startsWith(p)));

      if (!match) {
        // check for exact paths
        match = !!scopes.writeArchivalPaths.find((p) => (path === p));
      }

      if (!match) {
        // not authorized to write to this path
        throw new ValidationError(`Address ${address} not authorized to modify ${path} by scopes`);
      }
    }
    return isArchivalRestricted;
  }
}

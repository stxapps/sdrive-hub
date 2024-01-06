import express from 'express';
import cors from 'cors';

import { getChallengeText, LATEST_AUTH_VERSION } from './authentication';
import { HubServer } from './server';
import GcDriver from './drivers/GcDriver'
import * as errors from './errors';
import config from './config';
import { runAsyncWrapper, randomString } from './utils';

const getDriverClass = (driver) => {
  if (driver === 'google-cloud') {
    return GcDriver;
  } else {
    throw new Error(`Failed to load driver: driver was set to ${driver}`);
  }
};

const writeResponse = (res, data, statusCode) => {
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.write(JSON.stringify(data));
  res.end();
};

let driver;
if (config.driverInstance) {
  driver = config.driverInstance;
} else if (config.driverClass) {
  driver = new config.driverClass(config);
} else if (config.driver) {
  const driverClass = getDriverClass(config.driver);
  driver = new driverClass(config);
} else {
  throw new Error('Driver option not configured');
}
driver.ensureInitialized().catch(error => {
  console.error(error);
  process.exit();
})

const server = new HubServer(driver, config);

const corsConfig = cors({
  origin: '*',
  // Set the Access-Control-Max-Age header to 365 days.
  maxAge: 60 * 60 * 24 * 365,
  methods: 'DELETE,POST,GET,OPTIONS,HEAD',
  // Allow the client to include match headers in http requests
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Headers
  allowedHeaders: 'Authorization,Content-Type,If-Match,If-None-Match',
});

const app = express();
app.use(corsConfig);

// Enabling CORS Pre-Flight
// https://www.npmjs.com/package/cors#enabling-cors-pre-flight
// When using this middleware as an application level middleware,
//   pre-flight requests are already handled for all routes.
//app.options('*', corsConfig);

// sadly, express doesn't like to capture slashes.
//  but that's okay! regexes solve that problem
app.post(/^\/store\/([a-zA-Z0-9]+)\/(.+)/, runAsyncWrapper(async (req, res) => {
  const logKey = randomString(12);
  console.log(`(${logKey}) /store receives a post request`);

  let filename = req.params[1];
  if (filename.endsWith('/')) {
    filename = filename.substring(0, filename.length - 1);
  }
  const address = req.params[0];
  console.log(`(${logKey}) address: ${address}`);
  console.log(`(${logKey}) filename: ${filename}`);

  try {
    const responseData = await server.handleRequest(
      address, filename, req.headers, req
    );
    console.log(`(${logKey}) /store finished`);
    writeResponse(res, responseData, 202);
  } catch (err) {
    console.log(`(${logKey}) ${err.toString()}, return error`);
    if (err instanceof errors.ValidationError) {
      writeResponse(res, { message: err.message, error: err.name }, 401);
    } else if (err instanceof errors.AuthTokenTimestampValidationError) {
      writeResponse(res, { message: err.message, error: err.name }, 401);
    } else if (err instanceof errors.BadPathError) {
      writeResponse(res, { message: err.message, error: err.name }, 403);
    } else if (err instanceof errors.NotEnoughProofError) {
      writeResponse(res, { message: err.message, error: err.name }, 402);
    } else if (err instanceof errors.ConflictError) {
      writeResponse(res, { message: err.message, error: err.name }, 409);
    } else if (err instanceof errors.PayloadTooLargeError) {
      writeResponse(res, { message: err.message, error: err.name }, 413);
    } else if (err instanceof errors.PreconditionFailedError) {
      writeResponse(
        res, { message: err.message, error: err.name, etag: err.expectedEtag }, 412
      );
    } else {
      console.error(`(${logKey}) Server error`, err);
      writeResponse(res, { message: 'Server Error' }, 500);
    }
  }
}));

app.delete(/^\/delete\/([a-zA-Z0-9]+)\/(.+)/, runAsyncWrapper(async (req, res) => {
  const logKey = randomString(12);
  console.log(`(${logKey}) /delete receives a delete request`);

  let filename = req.params[1];
  if (filename.endsWith('/')) {
    filename = filename.substring(0, filename.length - 1);
  }
  const address = req.params[0];
  console.log(`(${logKey}) address: ${address}`);
  console.log(`(${logKey}) filename: ${filename}`);

  try {
    await server.handleDelete(address, filename, req.headers);
    console.log(`(${logKey}) /delete finished`);
    res.writeHead(202);
    res.end();
  } catch (err) {
    console.log(`(${logKey}) ${err.toString()}, return error`);
    if (err instanceof errors.ValidationError) {
      writeResponse(res, { message: err.message, error: err.name }, 401);
    } else if (err instanceof errors.AuthTokenTimestampValidationError) {
      writeResponse(res, { message: err.message, error: err.name }, 401);
    } else if (err instanceof errors.BadPathError) {
      writeResponse(res, { message: err.message, error: err.name }, 403);
    } else if (err instanceof errors.DoesNotExist) {
      writeResponse(res, { message: err.message, error: err.name }, 404);
    } else if (err instanceof errors.NotEnoughProofError) {
      writeResponse(res, { message: err.message, error: err.name }, 402);
    } else if (err instanceof errors.PreconditionFailedError) {
      writeResponse(
        res, { message: err.message, error: err.name, etag: err.expectedEtag }, 412
      );
    } else {
      console.error(`(${logKey}) Server error`, err);
      writeResponse(res, { message: 'Server Error' }, 500);
    }
  }
}));

app.post(
  /^\/perform-files\/([a-zA-Z0-9]+)\/?/,
  express.json({ limit: server.maxFileUploadSizeBytes }),
  runAsyncWrapper(async (req, res) => {
    const logKey = randomString(12);
    console.log(`(${logKey}) /perform-files receives a post request`);

    const address = req.params[0];
    const requestBody = req.body;
    console.log(`(${logKey}) address: ${address}`);
    //console.log(`(${logKey}) requestBody: ${requestBody}`); // Too big to always log

    try {
      const responseData = await server.handlePerformFiles(
        address, requestBody, req.headers
      );
      console.log(`(${logKey}) Performed ${responseData.length} files`);
      console.log(`(${logKey}) /perform-files finished`);
      writeResponse(res, responseData, 202);
    } catch (err) {
      console.log(`(${logKey}) ${err.toString()}, return error`);
      if (err instanceof errors.ValidationError) {
        writeResponse(res, { message: err.message, error: err.name }, 401);
      } else if (err instanceof errors.AuthTokenTimestampValidationError) {
        writeResponse(res, { message: err.message, error: err.name }, 401);
      } else {
        console.error(`(${logKey}) Server error`, err);
        writeResponse(res, { message: 'Server Error' }, 500);
      }
    }
  })
);

app.post(
  /^\/list-files\/([a-zA-Z0-9]+)\/?/,
  express.json({ limit: 4096 }),
  runAsyncWrapper(async (req, res) => {
    const logKey = randomString(12);
    console.log(`(${logKey}) /list-files receives a post request`);

    const address = req.params[0];
    const requestBody = req.body;
    const page = requestBody.page ? requestBody.page : null;
    const pageSize = requestBody.pageSize ? requestBody.pageSize : null;
    const stat = !!requestBody.stat;
    console.log(`(${logKey}) address: ${address}`);
    console.log(`(${logKey}) page: ${page}, pageSize: ${pageSize}, stat: ${stat}`);

    try {
      const files = await server.handleListFiles(
        address, page, pageSize, stat, req.headers
      );
      console.log(`(${logKey}) Got ${files.entries.length} files`);
      console.log(`(${logKey}) New page: ${files.page}`);
      console.log(`(${logKey}) /list-files finished`);
      writeResponse(res, { entries: files.entries, page: files.page }, 202);
    } catch (err) {
      console.log(`(${logKey}) ${err.toString()}, return error`);
      if (err instanceof errors.ValidationError) {
        writeResponse(res, { message: err.message, error: err.name }, 401);
      } else if (err instanceof errors.AuthTokenTimestampValidationError) {
        writeResponse(res, { message: err.message, error: err.name }, 401);
      } else {
        console.error(`(${logKey}) Server error`, err);
        writeResponse(res, { message: 'Server Error' }, 500);
      }
    }
  })
);

app.post(
  /^\/revoke-all\/([a-zA-Z0-9]+)\/?/,
  express.json({ limit: 4096 }),
  runAsyncWrapper(async (req, res) => {
    const logKey = randomString(12);
    console.log(`(${logKey}) /revoke-all receives a post request`);

    if (!req.body || !req.body.oldestValidTimestamp) {
      console.log(`(${logKey}) No req.body or oldestValidTimestamp, return error`);
      writeResponse(res, { message: 'Invalid JSON: missing oldestValidTimestamp' }, 400);
      return;
    }

    const address = req.params[0];
    const oldestValidTimestamp = parseInt(req.body.oldestValidTimestamp, 10);
    console.log(`(${logKey}) address: ${address}`);
    console.log(`(${logKey}) oldestValidTimestamp: ${oldestValidTimestamp}`);

    if (!Number.isFinite(oldestValidTimestamp) || oldestValidTimestamp < 0) {
      console.log(`(${logKey}) Invalid oldestValidTimestamp, return error`);
      writeResponse(res, {
        message: 'Invalid JSON: oldestValidTimestamp is not a valid integer',
      }, 400);
      return;
    }

    try {
      await server.handleAuthBump(address, oldestValidTimestamp, req.headers);
      console.log(`(${logKey}) /revoke-all finished`);
      writeResponse(res, { status: 'success' }, 202);
    } catch (err) {
      console.log(`(${logKey}) ${err.toString()}, return error`);
      if (err instanceof errors.ValidationError) {
        writeResponse(res, { message: err.message, error: err.name }, 401);
      } else if (err instanceof errors.BadPathError) {
        writeResponse(res, { message: err.message, error: err.name }, 403);
      } else {
        console.error(`(${logKey}) Server error`, err);
        writeResponse(res, { message: 'Server Error' }, 500);
      }
    }
  })
);

app.get('/', (_req, res) => {
  res.send('Welcome to <a href="https://www.stacksdrive.com">Stacks Drive</a>\'s hub!');
});

app.get('/hub_info/', (req, res) => {
  const challengeText = getChallengeText(server.serverName);
  if (challengeText.length < 10) {
    writeResponse(res, { message: 'Server challenge text misconfigured' }, 500);
    return;
  }

  const readURLPrefix = server.getReadURLPrefix();
  writeResponse(res, {
    'challenge_text': challengeText,
    'latest_auth_version': LATEST_AUTH_VERSION,
    'max_file_upload_size_megabytes': server.maxFileUploadSizeMB,
    'read_url_prefix': readURLPrefix,
  }, 200);
});

// Listen to the App Engine-specified port, or 8088 otherwise
const PORT = process.env.PORT || 8088;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}...`);
  console.log('Press Ctrl+C to quit.');
});

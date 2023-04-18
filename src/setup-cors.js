import { Storage } from '@google-cloud/storage';

import config from './config';

const storage = new Storage();
const bucketName = config.bucket;

// cloud.google.com/storage/docs/cross-origin
const getBucketMetadata = async () => {
  const [metadata] = await storage.bucket(bucketName).getMetadata();
  console.log(JSON.stringify(metadata, null, 2));
};

const configureBucketCors = async () => {
  // forum.stacks.org/t/tech-preview-using-your-own-gaia-hub-with-the-cli/6160
  const origin = ['*'];
  const method = ['GET', 'HEAD'];
  await storage.bucket(bucketName).setCorsConfiguration([{ method, origin }]);

  console.log(`Bucket ${bucketName} was updated with a CORS config to allow ${method} requests from ${origin}`);
};

getBucketMetadata();
//configureBucketCors().catch(console.error);

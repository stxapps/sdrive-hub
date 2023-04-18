const config = {
  "serverName": "hub.stacksdrive.com",
  //"port": 4000,
  "requireCorrectHubUrl": true,
  //"validHubUrls": ["https://hub.stacksdrive.com"],
  "driver": "google-cloud",
  "bucket": "sdrive-001.appspot.com",
  /*"gcCredentials": {
    "keyFilename": "YOUR_KEY_FILE_PATH"
  },*/
  "readURL": "https://storage.googleapis.com/sdrive-001.appspot.com/",
  /*"proofsConfig": {
    "proofsRequired": 0
  },*/
  "pageSize": 1000,
  "cacheControl": "",
  "maxFileUploadSize": 20,
  "authTimestampCacheSize": 3000,
  //"whitelist": [],
  /*"argsTransport": {
    "level": "debug",
    "handleExceptions": true,
    "stringify": true,
    "timestamp": true,
    "colorize": false,
    "json": true,
  },*/
};

export default config;

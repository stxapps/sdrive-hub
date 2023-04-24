const config = {
  'serverName': 'hub.stacksdrive.com',
  //'port': 4000,
  'requireCorrectHubUrl': true,
  //'validHubUrls': ['http://localhost:8088'],
  'driver': 'google-cloud',
  'bucket': 'sdrive-001.appspot.com',
  /*'gcCredentials': {
    'keyFilename': 'YOUR_KEY_FILE_PATH'
  },*/
  'readURL': 'https://storage.googleapis.com/sdrive-001.appspot.com/',
  /*'proofsConfig': {
    'proofsRequired': 0
  },*/
  'pageSize': 1000,
  'cacheControl': 'public, max-age=1',
  'maxFileUploadSize': 20,
  'authTimestampCacheSize': 800,
  //'whitelist': [],
  /*'argsTransport': {
    'level': 'debug',
    'handleExceptions': true,
    'stringify': true,
    'timestamp': true,
    'colorize': false,
    'json': true,
  },*/
};

export default config;

var request = require('request');
var crypto = require('crypto');
var async = require('async');
var FilesystemAdapter = require('parse-server-fs-adapter');
var S3Adapter = require('parse-server-s3-adapter');
var AzureStorageAdapter = require('parse-server-azure-storage').AzureStorageAdapter;
var MongoClient = require('mongodb').MongoClient;

// regex that matches old legacy Parse hosted files
var legacyFilesPrefixRegex = new RegExp("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}-");

var db, config, alreadyCount = 0;

module.exports.init = init;
module.exports.run = run;
module.exports.reset = reset;

function init(options) {
  console.log('Initializing transfer configuration...');
  config = options;
  return new Promise(function(resolve, reject) {
    if (config.renameInDatabase || config.checkIfNameIsAlreadyChanged) {
      console.log('Connecting to MongoDB');
      MongoClient.connect(config.mongoURL, function(error, database) {
        if (error) {
          console.log("failed to connect ot mongo with error ");
          console.log(error);
          return reject(error);
        }
        console.log('Successfully connected to MongoDB');
        db = database;
        _setup().then(resolve, reject);
      });
    } else {
      _setup().then(resolve, reject);
    }
  });
}

function reset() {
  alreadyCount = 0;
}

function _setup() {
  config.adapterName = config.transferTo || config.filesAdapter.constructor.name;
  console.log('Initializing '+config.adapterName+' adapter');
  if (config.filesAdapter && config.filesAdapter.createFile) {
    return Promise.resolve();
  } else if (config.transferTo == 'print') {
    return Promise.resolve();
  } else if (config.transferTo == 'filesystem') {
    config.filesAdapter = new FilesystemAdapter({
      filesSubDirectory: config.filesystemPath
    });
  } else if (config.transferTo == 's3') {
    config.filesAdapter = new S3Adapter({
      accessKey: config.aws_accessKeyId,
      secretKey: config.aws_secretAccessKey,
      bucket: config.aws_bucket,
      bucketPrefix: config.aws_bucketPrefix,
      directAccess: true
    });
  } else if (config.transferTo == 'gcs') {
    
  } else if (config.transferTo == 'azure') {

    var account = config.azure_account;
    var container = config.azure_container;
    var options = {
        accessKey: config.azure_accessKey,
        directAccess: false // If set to true, files will be served by Azure Blob Storage directly
    }
    config.filesAdapter = new AzureStorageAdapter(account, container, options);

  } else {
    return Promise.reject('Invalid files adapter');
  }
  return Promise.resolve();
}

function run(files) {
  console.log('Processing '+files.length+' files');
  console.log('Saving files with '+config.adapterName);
  return _processFiles(files);
}

/**
 * Handle error from requests
 */
function _requestErrorHandler(error, response, file) {
  if (error) {
    return error;
  } else if (response.statusCode >= 300) {
    return true;
  }
  return false;
}

/**
 * Converts a file into a non Parse file name
 * @param  {String} fileName
 * @return {String}
 */

var filenameCount = 0;
function _createNewFileName(fileName) {
  if (!config.renameFiles) {
    return fileName;
  }
  if (_isParseHostedFile(fileName)) {
    fileName = fileName.replace('tfss-', '');
    if (config.transformFileNames){
      var sections = fileName.split('-');
      var givenName = sections[sections.length-1];
      var prefix = fileName.substring(0,fileName.length-givenName.length);
      fileName = prefix.split('-').join('') + "-" + givenName;
    }
    else {
      var newPrefix = crypto.randomBytes(32/2).toString('hex');
      fileName = newPrefix + fileName.replace(legacyFilesPrefixRegex, '');
    }
  }

  var prefix;
  if (config.filePrefix) {
    prefix = config.filePrefix;
  }
  else {
    prefix = "mfp_"; //mfp stands for migrated file prefix
    if (filenameCount % 2 == 1) {
      prefix = "mfp2_"
    }  
    filenameCount++;
  }
  
  var newName = prefix + fileName;
  if (config.removeSpaces) {
    newName = newName.replace(/ /g,"_")
  }
  return newName;
}

function _isParseHostedFile(fileName) {
  if (fileName.indexOf('tfss-') === 0 || legacyFilesPrefixRegex.test(fileName)) {
    return true;
  }
  return false;
}

/**
 * Loops through n files at a time and calls handler
 * @param  {Array}    files     Array of files
 * @param  {Function} handler   handler function for file
 * @return {Promise}
 */
function _processFiles(files, handler) {
  var asyncLimit = config.asyncLimit || 5;
  return new Promise(function(resolve, reject) {
    async.eachOfLimit(files, asyncLimit, function(file, index, callback,error) {
      process.stdout.write('Processing '+(index+1)+'/'+files.length+' Skipped:' + alreadyCount+ '\r');
      file.newFileName = _createNewFileName(file.fileName);
      _shouldTransferFile(file).then(function(shouldTransfer) {
        if (shouldTransfer) {
          return _transferFile(file);
        }
        else {
          alreadyCount++;
          return Promise.resolve();
        }
      }).then(callback,callback)
    }, function(error) {
      if (error) {
        return reject(error);
      }
      resolve('\n\nComplete!');
    });
  })
}

function _alreadyTransfered(file) {
  return new Promise(function(resolve, reject) {
    if (!config.checkIfNameIsAlreadyChanged) {
      return resolve(false);
    }
  
    db.collection(file.className).findOne(
      { _id : file.objectId },
      function(error, result ) {
        if (error) {
          return reject(error);
        }
        if (result && result[file.fieldName] == file.newFileName) {
          return resolve(true);
        }
        else {
          return resolve(false);  
        }
        
      }
    );
  });
}


/**
 * Changes the file name that is saved in MongoDB
 * @param  {Object}   file     the file info
 */
function _changeDBFileField(file) {
  return new Promise(function(resolve, reject) {
    if (file.fileName == file.newFileName || !config.renameInDatabase) {
      return resolve();
    }
    var update = {$set:{}};
    update.$set[file.fieldName] = file.newFileName;
    if (config.addTagForChunkCapability) {
      update.$set["transfered_files"] = true;
    }
    db.collection(file.className).update(
      { _id : file.objectId },
      update,
      function(error, result ) {
        if (error) {
          return reject(error);
        }
        resolve();
      }
    );
  });
}

function _markAsTransferedAnyway(file) {
  return new Promise(function(resolve, reject) {
    if (file.fileName == file.newFileName || !config.renameInDatabase) {
      return resolve();
    }
    var update = {$set:{}};
    update.$set["transfered_files"] = false;
    update.$set["issues"] = "bad transfer file location";
    db.collection(file.className).update(
      { _id : file.objectId },
      update,
      function(error, result ) {
        if (error) {
          return reject(error);
        }
        resolve();
      }
    );
  });
}

/**
 * Determines if a file should be transferred based on configuration
 * @param  {Object} file the file info
 */
function _shouldTransferFile(file) {
  return new Promise(function(resolve, reject) {
    if (file.fileName  === "DELETE" && config.chunckSize > 0) {
      resolve(true);
    }
    else if (config.filesToTransfer == 'all') {
      resolve(true);
    } else if (config.filesToTransfer == 'parseOnly' && _isParseHostedFile(file.fileName)) {
      resolve(true);
    } else if (config.filesToTransfer == 'parseServerOnly' && !_isParseHostedFile(file.fileName)) {
      resolve(true);
    }
    resolve(false);
  })
}

/**
 * Request file from URL and upload with filesAdapter
 * @param  {Object}   file     the file info object
 */
function _transferFile(file) {
  return new Promise(function(resolve, reject) {
    if (file.fileName === 'DELETE') {
      //need to mark it as transfered
      console.log("deleted"); 
      _markAsTransferedAnyway(file).then(function(){
        resolve();
      });
    }
    else {
      _alreadyTransfered(file).then(function(alreadyResult) {
        if (alreadyResult) {
          alreadyCount++;
          return process.nextTick(resolve);
        }
        if (config.transferTo == 'print') {
          // Use process.nextTick to avoid max call stack error
          console.log(file.objectId + " " + file.url);
          return process.nextTick(resolve);
        }
        request({ url: file.url, encoding: null }, function(error, response, body) {
          if (_requestErrorHandler(error, response, file)) {
            if (config.chunkSize > 0) {
              if (response) {
                _markAsTransferedAnyway(file).then(function() {
                  console.log('Failed request ('+response.statusCode+') skipping: '+' '+file.className + " " + file.objectId + ' ' + response.request.href+' '+file.className +":"+file.objectId);
                  reject(error);        
                }).catch(function(updateError){
                  reject(updateError);
                });  
              }
              else {
                reject(error);
              }
            }
            else {
              reject(error);
            }
          }
          else {
            config.filesAdapter.createFile(
              file.newFileName, body, response.headers['content-type']
            ).then(function() {
              return _changeDBFileField(file);
            }).then(resolve).catch(function(error){
              console.log(file.url);
              console.log(file.objectId);
              reject(error);
            });  
          }
        });
      })
    }
  });
}

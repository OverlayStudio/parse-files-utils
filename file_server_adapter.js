'use strict'

var request = require("request");

// Adapter classes must implement the following functions:
// * createFile(config, filename, data)
// * getFileData(config, filename)
// * getFileLocation(config, request, filename)

function FileServerAdapter(options) {
  if (options.fileServerURLs) {
    this._serverURLs = options.fileServerURLs;
  }
  else {
    this._serverURLs = process.env.FILE_SERVER_URLS;  
  }
  
  if (options.projectName) {
    this._projectName = options.projectName;
  }
  else  {
    this._projectName = process.env.FILE_SERVER_PROJECT;  
  }

  if (options.fileServerKey) {
    this._saveKey = options.fileServerKey;
  }
  else {
    this._saveKey = process.env.FILE_SERVER_KEY;  
  }
  
  if (options.fileServerSaveURLs) {
    this._saveURLs = options.fileServerSaveURLs;
  }
  else {
    this._saveURLs = process.env.FILE_SERVER_SAVE_URLs;  
  }
  
}

FileServerAdapter.prototype.createFile = function(filename, data, contentType) {
  //do request where we post the data to the
  console.log("location to save to: " + this.getFileLocation({},filename));
  var serverIndex = this.serverIndexForFilename(filename);
  var serverURL = this._saveURLs[serverIndex];
  var options = { method: 'POST',
    uri: serverURL,
    headers: 
      {
       'save-key': this._saveKey,
       'save-file-name': filename,
       'save-project': this._projectName,
      } };

  return new Promise((resolve, reject) => {
    var req = request(options, function (error, response, body) {
      if (error) {
        reject(error);
      }
      else {
        resolve(data)
      }
    });
    var form = req.form();
    form.append('file',data, { filename: filename, contentType: contentType});
  });
}

FileServerAdapter.prototype.getFileData = function(filename) {
  return new Promise((resolve, reject) => {
      let filepath = this.getFileLocation(filename);
      fs.readFile( filepath , function (err, data) {
        if(err !== null) {
          return reject(err);
        }
        fs.unlink(filepath, (unlinkErr) => {
        if(err !== null) {
            return reject(unlinkErr);
          }
          resolve(data);
        });
      });

    });
  }

FileServerAdapter.prototype.deleteFile = function(filename) {
   var serverIndex = this.serverIndexForFilename(filename);
   var serverURL = this._saveURLs[serverIndex];
   var options = { method: 'DELETE',
    url: serverURL,
    headers: 
      { 'cache-control': 'no-cache',
       'save-key': this._saveKey,
       'save-file-name': decodeURI(filename),
       'save-project': this._projectName } };

  return new Promise((resolve, reject) => {
    request(options, function (error, response, body) {
      if (error) {
        reject(error);
      }
      else {
        resolve(body)
      }
    });
  });
}

FileServerAdapter.prototype.serverIndexForFilename = function(filename) {
  if (filename.startsWith("mfp2_")) {
    return 1;
  }
  return 0;
}

FileServerAdapter.prototype.getFileLocation = function(config, filename) {
  var serverIndex = this.serverIndexForFilename(filename);
  var serverURL = this._serverURLs[serverIndex];
  filename = encodeURIComponent(filename);
  return serverURL+ '/' + this._projectName + '/' + filename;
}

module.exports = FileServerAdapter;
module.exports.default = FileServerAdapter;

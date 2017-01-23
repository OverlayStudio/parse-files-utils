'use strict'

var request = require("request");

// Adapter classes must implement the following functions:
// * createFile(config, filename, data)
// * getFileData(config, filename)
// * getFileLocation(config, request, filename)

function FileServerAdapter(options) {
  if (options.fileServerURL) {
    this._serverURL = options.fileServerURL;
  }
  else {
    this._serverURL = process.env.FILE_SERVER_URL;  
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
  
  if (options.fileServerSaveURL) {
    this._saveURL = options.fileServerSaveURL;
  }
  else {
    this._saveURL = process.env.FILE_SERVER_SAVE_URL;  
  }
  
}

FileServerAdapter.prototype.createFile = function(filename, data, contentType) {
  //do request where we post the data to the
 // console.log("location to save to: " + this.getFileLocation({},filename));
  var options = { method: 'POST',
    uri: this._saveURL,
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
   var options = { method: 'DELETE',
    url: this._saveURL,
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

FileServerAdapter.prototype.getFileLocation = function(config, filename) {
  filename = encodeURIComponent(filename);
  return this._serverURL + '/' + this._projectName + '/' + filename;
}

module.exports = FileServerAdapter;
module.exports.default = FileServerAdapter;

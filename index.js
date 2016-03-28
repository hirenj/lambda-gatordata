var AWS = require('aws-sdk');
var s3 = new AWS.S3({region:'us-east-1'});
var dynamo = new AWS.DynamoDB.DocumentClient({region:'us-east-1'});
var JSONStream = require('JSONStream');
var fs = require('fs');
var crypto = require('crypto');

require('es6-promise').polyfill();

var bucket_name = 'test-gator';

var upload_metadata_dynamodb = function upload_metadata_dynamodb(set,group,meta) {
  var item = {};
  return new Promise(function(resolve,reject) {
    dynamo.put({'TableName' :'test-datasets', 'Item' : {
      'accessions' : meta.accessions,
      'id' : set,
      'group_id' : group
    }},function(err,result) {
      if (err) {
        console.log("Failed Dynamodb upload",err);
        reject(err);
        return;
      }
      resolve(result);
    });
  });
};

var upload_data_record_s3 = function upload_data_record_s3(key,data) {
  return new Promise(function(resolve,reject) {
    var params = {
      'Bucket': bucket_name,
      'Key': key,
      'ContentType': 'application/json'
    };
    var datablock = JSON.stringify(data);
    params.Body = datablock;
    params.ContentMD5 = new Buffer(crypto.createHash('md5').update(datablock).digest('hex'),'hex').toString('base64');
    var options = {partSize: 15 * 1024 * 1024, queueSize: 1};
    s3.upload(params, options, function(err, data) {
      if (err) {
        console.log("Failed uploading to S3", err);
        reject(err);
        return;
      }
      resolve(data);
    });
  });
};

var upload_data_record = function upload_data_record(key,data) {
  return upload_data_record_s3(key,data);
};

var retrieve_file_s3 = function retrieve_file_s3(filekey) {
  var params = {
    'Key' : filekey,
    'Bucket' : bucket_name
  };
  return s3.getObject(params).createReadStream();
}

var retrieve_file_local = function retrieve_file_local(filekey) {
  return fs.createReadStream(filekey);
}

var retrieve_file = function retrieve_file(filekey) {
  return retrieve_file_s3(filekey);
}

var split_file = function split_file(filekey) {
  var rs = retrieve_file(filekey);
  var upload_promises = [];

  var filekey_components = filekey.split('/');
  var group_id = filekey_components[1];
  var dataset_id = filekey_components[2];
  if ( ! dataset_id ) {
    return Promise.reject(new Error('No dataset id'));
  }
  var accessions = [];
  console.log(group_id,dataset_id);
  rs.pipe(JSONStream.parse(['data', {'emitKey': true}])).on('data',function(dat) {
    // Output data should end up looking like this:
    // {  'data': dat.value,
    //    'retrieved' : "ISO timestamp",
    //    'title' : "Title" }

    var datablock = {'data': dat.value };

    accessions.push(dat.key);

    upload_promises.push(upload_data_record("data/latest/"+group_id+":"+dataset_id+"/"+dat.key, datablock));
  });

  //FIXME - upload metadata as the last part of the upload, marker of done.
  //        should be part of api request
  rs.pipe(JSONStream.parse(['metadata'])).on('data',function(dat) {
    upload_promises.push(upload_metadata_dynamodb(dataset_id,group_id,{'metadata': dat, 'accessions' : accessions}));
  });
  return new Promise(function(resolve,reject) {
    rs.on('end',function() {
      resolve(Promise.all(upload_promises));
    });
    rs.on('error',function(err) {
      reject(err);
    });
  });
};

var readAllData = function readAllData(event,context) {
  // Decode JWT
  // Get groups/datasets that can be read
  // Get metadata entries that contain the desired accession
  // Filter metadata by the JWT permissions
  // Get data from S3 and combine
};

var splitFile = function splitFile(event,context) {
  var filekey = event.Records[0].s3.object.key;
  split_file(filekey).then(function(done) {
    console.log("Uploaded all components");
    context.succeed('OK');
    // Upload the metadata at the end of a successful decomposition
  }).catch(function(err) {
    console.error(err);
    console.error(err.stack);
    context.succeed('NOT-OK');
  });
};

exports.splitFile = splitFile;
exports.readAllData = readAllData;
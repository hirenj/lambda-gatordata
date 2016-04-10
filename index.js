var AWS = require('aws-sdk');
var s3 = new AWS.S3({region:'us-east-1'});
var dynamo = new AWS.DynamoDB.DocumentClient({region:'us-east-1'});
var JSONStream = require('JSONStream');
var fs = require('fs');
var crypto = require('crypto');

require('es6-promise').polyfill();

var bucket_name = 'test-gator';
var dynamodb_table = 'test-datasets';

try {
    var config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    dynamodb_table = config.tables.datasets;
} catch (e) {
}


var upload_metadata_dynamodb = function upload_metadata_dynamodb(set,group,meta) {
  var item = {};
  return new Promise(function(resolve,reject) {
    dynamo.put({'TableName' : dynamodb_table, 'Item' : {
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

    accessions.push(dat.key.toLowerCase());

    upload_promises.push(upload_data_record("data/latest/"+group_id+":"+dataset_id+"/"+dat.key.toLowerCase(), datablock));
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

var base64urlDecode = function(str) {
  return new Buffer(base64urlUnescape(str), 'base64').toString();
};

var base64urlUnescape = function(str) {
  str += new Array(5 - str.length % 4).join('=');
  return str.replace(/\-/g, '+').replace(/_/g, '/');
};

var datasets_containing_acc = function(acc) {
  var params = {
    TableName : dynamodb_table,
    FilterExpression : 'contains(#accessions,:acc)',
    ProjectionExpression : 'id,group_id',
    ExpressionAttributeNames : { '#accessions' : 'accessions'},
    ExpressionAttributeValues : {':acc' : acc.toLowerCase() }
  };
  return new Promise(function(resolve,reject) {
    dynamo.scan(params, function(err, data) {
     if (err) {
      reject(err);
      return;
     }
     resolve(data.Items);
    });
  });
};

var download_set_s3 = function(set) {
  var params = {
    'Key' : 'data/latest/'+set,
    'Bucket' : bucket_name
  };
  return new Promise(function(resolve,reject) {
    s3.getObject(params,function(err,data) {
      if (err) {
        reject(err);
        return;
      }
      resolve(JSON.parse(data.Body));
    });
  });
};

var combine_sets = function(entries) {
  var results = {"data" : []};
  results.data = entries.map(function(entry) { return entry.data });
  return results;
};

/*
Test event
{
  "acc": "Q9VZF9",
  "authorizationToken": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2Nlc3MiOnsiZm9vZ3JvdXAvdGVzdGluZyI6WyIqIl0sImZvb2dyb3VwLzBCeTQ4S0tEdTlsZUNjWE5hUXpaelZGOW1hR00iOlsiUDEyMzQ1Il19fQ.l3kBNPB1p5kXfobGhCoXP9taUvbbn3vloqQYgd3P6cA"
}
*/

var readAllData = function readAllData(event,context) {
  console.log("readAllData");
  console.log(event);
  var token = event.authorizationToken.split(' ');
  var accession = event.acc.toLowerCase();

  if(token[0] !== 'Bearer'){
    context.succeed('NOT-OK');
    return;
  }

  var grants = {};

  // Decode JWT
  // Get groups/datasets that can be read

  grants = JSON.parse(base64urlDecode(token[1].split('.')[1])).access;

  // Get metadata entries that contain the desired accession

  datasets_containing_acc(accession).then(function(sets) {
    console.log(sets);
    var valid_sets = [];
    sets.forEach(function(set) {

      // Filter metadata by the JWT permissions

      if (grants[set.group_id+'/'+set.id]) {
        var valid_prots = grants[set.group_id+'/'+set.id];
        if (valid_prots.filter(function(id) { return id == '*' || id.toLowerCase() == accession; }).length > 0) {
          valid_sets.push(set.group_id+':'+set.id);
        }
      }
      if (grants[set.group_id+'/*']) {
        var valid_prots = grants[set.group_id+'/*'];
        if (valid_prots.filter(function(id) { return id == '*' || id.toLowerCase() == accession; }).length > 0) {
          valid_sets.push(set.group_id+':'+set.id);
        }
      }
    });
    console.log(valid_sets.join(','));
    return valid_sets;
  }).then(function(sets) {

    // Get data from S3 and combine
    return Promise.all(sets.map(function (set) { return download_set_s3(set+'/'+accession); }));
  }).then(combine_sets).then(function(combined) {
    context.succeed(combined);
  }).catch(function(err) {
    console.error(err);
    console.error(err.stack);
    context.succeed('NOT-OK');
  });
};

var splitFile = function splitFile(event,context) {
  var filekey = require('querystring').unescape(event.Records[0].s3.object.key);
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
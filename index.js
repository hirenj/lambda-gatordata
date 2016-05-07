var AWS = require('aws-sdk');
var s3 = new AWS.S3({region:'us-east-1'});
var dynamo = new AWS.DynamoDB.DocumentClient({region:'us-east-1'});
var JSONStream = require('JSONStream');
var fs = require('fs');
var crypto = require('crypto');

require('es6-promise').polyfill();

var promisify = function(aws) {
  aws.Request.prototype.promise = function() {
    return new Promise(function(accept, reject) {
      this.on('complete', function(response) {
        if (response.error) {
          reject(response.error);
        } else {
          accept(response);
        }
      });
      this.send();
    }.bind(this));
  };
};

promisify(AWS);

var bucket_name = 'test-gator';
var metadata_table = 'test-datasets';
var data_table = 'data';

try {
    var config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    metadata_table = config.tables.datasets;
    data_table = config.table.data;
} catch (e) {
}


var upload_metadata_dynamodb_from_s3 = function upload_metadata_dynamodb_from_s3(set,group,meta) {
  var item = {};
  return new Promise(function(resolve,reject) {
    dynamo.put({'TableName' : metadata_table, 'Item' : {
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

var upload_metadata_dynamodb_from_db = function upload_metadata_dynamodb_from_db(set_id,group_id,meta) {
  // Update item (set: acc, set_id, data) add group_id.
  if (meta && meta.accessions.length < 1) {
    return Promise.resolve(true);
  }
  var params = {
   'TableName' : data_table,
   'Key' : {'acc' : 'metadata', 'dataset' : set_id },
   'UpdateExpression': 'ADD #gids :group',
    'ExpressionAttributeValues': {
        ':group': dynamo.createSet([ group_id ]),
    },
    'ExpressionAttributeNames' : {
      '#gids' : 'group_ids'
    }
  };
  console.log("Adding ",group_id," to set ",set_id);
  return dynamo.update(params).promise();
};

var upload_metadata_dynamodb = function(set,group,meta) {
  return upload_metadata_dynamodb_from_db(set,group,meta);
}

var upload_queue_db = [];

var upload_data_record_db = function upload_data_record_db(key,data) {
  var key_elements = key ? key.split('/') : [];
  var set_ids = (key_elements[2] || '').split(':');
  var group_id = set_ids[0];
  var set_id = set_ids[1];
  var accession = key_elements[3];
  if (key) {
    upload_queue_db.push({
      'PutRequest' : {
        'Item' : {
          'acc' : accession,
          'dataset' : set_id,
          'data' : data.data
        }
      }
    });
  }
  if ( ! key || upload_queue_db.length == 25) {
    var params = { 'RequestItems' : {} };
    params.RequestItems[data_table] = [].concat(upload_queue_db);
    upload_queue_db = [];
    return dynamo.batchWrite(params).promise();
  } else {
    return Promise.resolve(true);
  }
};

var upload_data_record_s3 = function upload_data_record_s3(key,data) {
  if ( ! key ) {
    return Promise.resolve(true);
  }
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
  return upload_data_record_db(key,data);
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

var remove_folder = function remove_folder(setkey) {
  return remove_folder_db(setkey);
};

var remove_folder_db = function remove_folder_db(setkey) {
  var group_id, dataset_id;
  var ids = setkey.split(':');
  group_id = ids[0];
  set_id = ids[1];
  // We should remove the group from the entries in the dataset
  // Possibly another vacuum step to remove orphan datasets?
  // Maybe just get rid of the group in the datasets
  // table and then do a batch write?
  console.log("Removing from set ",set_id,"group",group_id);
  var params = {
   'TableName' : data_table,
   'Key' : { 'dataset' : set_id, 'acc' : 'metadata' },
   'UpdateExpression': 'DELETE #gids :group',
    'ExpressionAttributeValues': {
        ':group': dynamo.createSet([ group_id ])
    },
    'ExpressionAttributeNames' : {
      '#gids' : 'group_ids'
    }
  };
  return dynamo.update(params).promise();
};

var remove_folder_s3 = function remove_folder_s3(setkey) {
  var params = {
    Bucket: bucket_name,
    Prefix: "data/latest/"+setkey+"/"
  };
  console.log(params);
  return s3.listObjects(params).promise().then(function(result) {
    var params = {Bucket: bucket_name, Delete: { Objects: [] }};
    params.Delete.Objects = result.data.Contents.map(function(content) { return { Key: content.Key }; });
    if (params.Delete.Objects.length < 1) {
      return Promise.resolve({"data" : { "Deleted" : [] }});
    }
    return s3.deleteObjects(params).promise();
  }).then(function(result) {
    if (result.data.Deleted.length === 1000) {
      return remove_folder(setkey);
    }
    return true;
  });
}

var remove_data = function remove_data(filekey) {
  var filekey_components = filekey.split('/');
  var group_id = filekey_components[2];
  var dataset_id = filekey_components[1];
  return remove_folder(group_id+":"+dataset_id).then(function() {
    return upload_metadata_dynamodb(dataset_id,group_id,{'metadata': {}, 'accessions' : []});
  });
};

var split_file = function split_file(filekey,skip_remove) {
  skip_remove = true;
  var filekey_components = filekey.split('/');
  var group_id = filekey_components[2];
  var dataset_id = filekey_components[1];

  if ( ! dataset_id ) {
    return Promise.reject(new Error('No dataset id'));
  }

  if (! skip_remove) {
    return remove_folder(group_id+":"+dataset_id).then(function() {
      return split_file(filekey,true);
    });
  }

  var rs = retrieve_file(filekey);
  var upload_promises = [];

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
    upload_promises.push(upload_data_record(null,null));
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
    TableName : metadata_table,
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

var download_all_data_db = function(accession,grants) {
  var params = {
    TableName: data_table,
    KeyConditionExpression: 'acc = :acc',
    ExpressionAttributeValues: {
      ':acc': accession
    }
  };
  return dynamo.query(params).promise().then(function(data) {
    return(data.data.Items.map(function(item) { return item; }));
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
      var result = JSON.parse(data.Body);
      result.dataset = set;
      resolve(result);
    });
  });
};

var download_all_data_s3 = function(accession,grants) {
  // Get metadata entries that contain the desired accession
  var start_time = (new Date()).getTime();
  console.log("datasets_containing_acc start ");
  return datasets_containing_acc(accession).then(function(sets) {
    console.log("datasets_containing_acc end ",(new Date()).getTime() - start_time);
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
    start_time = (new Date()).getTime();
    // Get data from S3 and combine
    console.log("download_set_s3 start");
    return Promise.all(sets.map(function (set) { return download_set_s3(set+'/'+accession); })).then(function(entries) {
      console.log("download_set_s3 end ",(new Date()).getTime() - start_time);
      return entries;
    });
  });
};

var download_all_data = function(accession,grants) {
  return download_all_data_db(accession,grants);
};

var combine_sets = function(entries) {
  var results = {"data" : []};
  results.data = entries.map(function(entry) { return entry });
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
  grants = {};
  // grants = JSON.parse(base64urlDecode(token[1].split('.')[1])).access;

  download_all_data(accession,grants).then(function(entries) {
    start_time = (new Date()).getTime();
    console.log("combine_sets start");
    return entries;
  }).then(combine_sets).then(function(combined) {
    console.log("combine_sets end ",(new Date()).getTime() - start_time);
    context.succeed(combined);
  }).catch(function(err) {
    console.error(err);
    console.error(err.stack);
    context.succeed('NOT-OK');
  });
};

var splitFile = function splitFile(event,context) {
  var filekey = require('querystring').unescape(event.Records[0].s3.object.key);
  var result_promise = Promise.resolve(true);
  if (event.Records[0].eventName.match(/ObjectRemoved/)) {
    console.log("Remove data at ",filekey);
    result_promise = remove_data(filekey);
  }
  if (event.Records[0].eventName.match(/ObjectCreated/)) {
    console.log("Splitting data at ",filekey);
    result_promise = split_file(filekey);
  }
  result_promise.then(function(done) {
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
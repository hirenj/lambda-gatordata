'use strict';
/*jshint esversion: 6, node:true */

const AWS = require('lambda-helpers').AWS;
const s3 = new AWS.S3();
const dynamo = new AWS.DynamoDB.DocumentClient();
const JSONStream = require('JSONStream');
const fs = require('fs');
const crypto = require('crypto');
const zlib = require('zlib');

const Queue = require('lambda-helpers').queue;
const Events = require('lambda-helpers').events;

var bucket_name = 'test-gator';
var metadata_table = 'test-datasets';
var data_table = 'data';
var split_queue = 'SplitQueue';

try {
    var config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    metadata_table = config.tables.datasets;
    data_table = config.tables.data;
    split_queue = config.queue.SplitQueue;
} catch (e) {
}


var get_current_md5 = function get_current_md5(filekey) {
  var filekey_components = filekey.split('/');
  var group_id = filekey_components[2];
  var dataset_id = filekey_components[1];
  var params_metadata = {
    TableName: data_table,
    KeyConditionExpression: 'acc = :acc and dataset = :dataset',
    ExpressionAttributeValues: {
      ':acc': 'metadata',
      ':dataset' : dataset_id
    }
  };
  return dynamo.query(params_metadata).promise().then(function(data) {
    return data.Items & data.Items.length > 0 ? data.Items[0].md5 : null;
  });
};

var upload_metadata_dynamodb_from_s3 = function upload_metadata_dynamodb_from_s3(set,group,meta) {
  return dynamo.put({'TableName' : metadata_table, 'Item' : {
    'accessions' : meta.accessions,
    'id' : set,
    'group_id' : group
  }}).promise();
};

var upload_metadata_dynamodb_from_db = function upload_metadata_dynamodb_from_db(set_id,group_id,meta) {
  if (meta.remove) {
    // Don't need to remove the group id as it's already deleted
    return Promise.resolve(true);
  }
  var params;
  if (meta.md5 && ! meta.notmodified) {
    params = {
     'TableName' : data_table,
     'Key' : {'acc' : 'metadata', 'dataset' : set_id },
     'UpdateExpression': 'SET #md5 = :md5 ADD #gids :group',
      'ExpressionAttributeValues': {
          ':group': dynamo.createSet([ group_id ]),
          ':md5'  : meta.md5
      },
      'ExpressionAttributeNames' : {
        '#gids' : 'group_ids',
        '#md5' : 'md5'
      }
    };
  } else {
    params = {
     'TableName' : data_table,
     'Key' : {'acc' : 'metadata', 'dataset' : set_id },
     'UpdateExpression': 'ADD #gids :group',
      'ExpressionAttributeValues': {
          ':group': dynamo.createSet([ group_id ])
      },
      'ExpressionAttributeNames' : {
        '#gids' : 'group_ids'
      }
    };
  }
  console.log("Adding ",group_id," to set ",set_id);
  return dynamo.update(params).promise();
};

var upload_metadata_dynamodb = function(set,group,meta) {
  return upload_metadata_dynamodb_from_db(set,group,meta);
}

let uploader = null;

// This should really be a stream so that
// we don't end up keeping all the elements
// in memory.. filter?

var upload_data_record_db = function upload_data_record_db(key,data) {
  if ( ! uploader ) {
    uploader = require('./dynamodb_rate').createUploader(data_table);
    uploader.start();
  }

  var key_elements = key ? key.split('/') : [];
  var set_ids = (key_elements[2] || '').split(':');
  var group_id = set_ids[0];
  var set_id = set_ids[1];
  var accession = key_elements[3];
  if (key) {
    if ( accession && set_id && data.data ) {
      data.data.forEach(function(obj) {
        if (obj.spectra) {
          delete obj.spectra;
        }
        if (obj.interpro) {
          delete obj.interpro;
        }
      });
      var block = {
              'acc' : accession,
              'dataset' : set_id
      };
      block.data = zlib.deflateSync(new Buffer(JSON.stringify(data.data),'utf8')).toString('binary');
      uploader.queue.push({
        'PutRequest' : {
          'Item' : block
        }
      });

    }
  }
  if ( ! key ) {
    uploader.setQueueReady();
    return uploader.finished.promise.then(function() {
      uploader = null;
    });
  } else {
    return Promise.resolve(true);
  }
};

var upload_data_record_s3 = function upload_data_record_s3(key,data) {
  if ( ! key ) {
    return Promise.resolve(true);
  }
  var params = {
    'Bucket': bucket_name,
    'Key': key,
    'ContentType': 'application/json'
  };
  var datablock = JSON.stringify(data);
  params.Body = datablock;
  params.ContentMD5 = new Buffer(crypto.createHash('md5').update(datablock).digest('hex'),'hex').toString('base64');
  var options = {partSize: 15 * 1024 * 1024, queueSize: 1};
  return s3.upload(params, options).promise();
};

var upload_data_record = function upload_data_record(key,data) {
  return upload_data_record_db(key,data);
};

var retrieve_file_s3 = function retrieve_file_s3(filekey,md5_result) {
  var params = {
    'Key' : filekey,
    'Bucket' : bucket_name,
    'IfNoneMatch' : md5_result.old+"",
  };
  var request = s3.getObject(params);
  var stream = request.createReadStream();
  stream.on('finish',function() {
    md5_result.md5 = request.response.data.ETag;
  });
  return stream;
}

var retrieve_file_local = function retrieve_file_local(filekey) {
  return fs.createReadStream(filekey);
}

var retrieve_file = function retrieve_file(filekey,md5_result) {
  return retrieve_file_s3(filekey,md5_result);
}

var remove_folder = function remove_folder(setkey) {
  return remove_folder_db(setkey);
};

var remove_folder_db = function remove_folder_db(setkey) {
  let group_id, dataset_id;
  let ids = setkey.split(':');
  group_id = ids[0];
  let set_id = ids[1];
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
    params.Delete.Objects = result.Contents.map(function(content) { return { Key: content.Key }; });
    if (params.Delete.Objects.length < 1) {
      return Promise.resolve({"data" : { "Deleted" : [] }});
    }
    return s3.deleteObjects(params).promise();
  }).then(function(result) {
    if (result.Deleted.length === 1000) {
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
    return upload_metadata_dynamodb(dataset_id,group_id,{'remove': true});
  });
};

var split_file = function split_file(filekey,skip_remove,current_md5) {
  var filekey_components = filekey.split('/');
  var group_id = filekey_components[2];
  var dataset_id = filekey_components[1];

  if ( ! dataset_id ) {
    return Promise.reject(new Error('No dataset id'));
  }

  if (! skip_remove) {
    return remove_folder(group_id+":"+dataset_id).then(function() {
      return split_file(filekey,true,current_md5);
    });
  }

  var md5_result = { old: current_md5 };

  var rs = retrieve_file(filekey,md5_result);
  var upload_promises = [];

  var accessions = [];
  console.log(group_id,dataset_id,md5_result);

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
    upload_promises.push( upload_data_record(null,null).then(function() {
      return upload_metadata_dynamodb(dataset_id,group_id,{'metadata': dat, 'md5' : md5_result.md5, 'accessions' : accessions});
    }));
  });
  return new Promise(function(resolve,reject) {
    rs.on('end',function() {
      resolve(Promise.all(upload_promises));
    });
    rs.on('error',function(err) {
      reject(err);
    });
  }).catch(function(err) {
    if (err.statusCode == 304) {
      return upload_metadata_dynamodb(dataset_id,group_id,{'notmodified' : true});
    }
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
  return dynamo.scan(params).promise().then(function(result) {
    return result.Items;
  });
};

var inflate_item = function(item) {
  return new Promise(function(resolve,reject) {
      if (! item.data) {
        item.data = [];
        resolve(item);
        return;
      }
      zlib.inflate(new Buffer(item.data,'binary'),function(err,result) {
        if (err) {
          reject(err); 
          console.log(err);
          return;         
        }
        item.data = JSON.parse(result.toString('utf8'));
        resolve(item);
      });
  });

};

var download_all_data_db = function(accession,grants) {
  var params = {
    TableName: data_table,
    KeyConditionExpression: 'acc = :acc',
    ExpressionAttributeValues: {
      ':acc': accession,
    }
  };
  var params_metadata = {
    TableName: data_table,
    KeyConditionExpression: 'acc = :acc',
    ExpressionAttributeValues: {
      ':acc': 'metadata',
    }
  };
  return Promise.all([
    dynamo.query(params).promise(),
    dynamo.query(params_metadata).promise()
  ]).then(function(data) {
    var meta_data = data[1];
    var db_data = data[0];
    return Promise.all(db_data.Items.map(inflate_item)).then(function(items) {
      return meta_data.Items.concat(items);
    });
  }).then(filter_db_datasets.bind(null,grants));
};

var filter_db_datasets = function(grants,data) {
  var sets = [];
  var accession = null;
  data.filter(function(data) {
    if (data.acc !== 'metadata') {
      accession = data.acc;
    }
    return data.acc == 'metadata';
  }).forEach(function(set) {
    sets = sets.concat(set.group_ids.values.map(function(group) { return { group_id: group, id: set.dataset }}));
  });
  var valid_sets = [];
  // Filter metadata by the JWT permissions
  sets.forEach(function(set) {
    if (grants[set.group_id+'/'+set.id]) {
      var valid_prots = grants[set.group_id+'/'+set.id];
      if (valid_prots.filter(function(id) { return id == '*' || id.toLowerCase() == accession; }).length > 0) {
        valid_sets.push(set.id);
      }
    }
    if (grants[set.group_id+'/*']) {
      var valid_prots = grants[set.group_id+'/*'];
      if (valid_prots.filter(function(id) { return id == '*' || id.toLowerCase() == accession; }).length > 0) {
        valid_sets.push(set.id);
      }
    }
  });
  return data.filter(function(dat) {
    return dat.acc !== 'metadata' && valid_sets.indexOf(dat.dataset) >= 0;
  });
};

var download_set_s3 = function(set) {
  var params = {
    'Key' : 'data/latest/'+set,
    'Bucket' : bucket_name
  };
  s3.getObject(params).promise().then(function(data) {
    var result = JSON.parse(data.Body);
    result.dataset = set;
    return result;
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

// Timings for runQueue

// Run runQueue every 8 hours
//   Always setTimeout(5 mins) (from start of execution)
//   - On empty queue setTimeout(+ 8 hours)
//   - On early finish of upload, setTimeout(2 seconds) (from end of upload)
//   - If it doesn't look like we will finish - pause execution, push the
//     offset back onto the queue.
//   - Discard item from the queue (unless there's an error, so put it back)

var runQueue = function() {
  let queue = new Queue(split_queue);
  Events.setTimeout('runSplitFiles',new Date(new Date().getTime() + 5*60*1000));
  queue.shift(1).then(function(message) {
    if ( ! message ) {
      // Modify table, reducing write capacity
      Events.setTimeout('runSplitFiles',new Date(new Date().getTime() + 8*60*60*1000));
      return;
    }

    // Modify table, increasing write capacity if needed

    uploader = splitFile(message.path,message.offset);
    uploader.on('finished',function() {
      message.finalise();
      Events.setTimeout('runSplitFiles',new Date(new Date().getTime() + 1*1000));
    });
    setTimeout(function() {
      // Wait for any requests to finalise
      // then look at the queue.
      uploader.pause().then(function() {
        return queue.sendMessage({'path' : path, 'offset' : offset });
      }).then(function() {
        message.finalise();
      });
    },4.5*60*1000);
  });
  // Consume item from queue
  // Item should go back on queue if it is > 5 minutes old
  // offset to start of queue
  // s3 path
};

var enQueue = function(event) {
  // Push the S3 object onto the queue with a null start
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

  var grants = event.grants ? JSON.parse(event.grants) : {};

  // Decode JWT
  // Get groups/datasets that can be read
  // grants = JSON.parse(base64urlDecode(token[1].split('.')[1])).access;
  let start_time = null;

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
    result_promise = get_current_md5(filekey).then( split_file.bind(null,filekey,null) );
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
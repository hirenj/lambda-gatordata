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

const MetadataExtractor = require('./dynamodb_rate').MetadataExtractor;
const Offsetter = require('./dynamodb_rate').Offsetter;

const MIN_WRITE_CAPACITY = 1;
const MAX_WRITE_CAPACITY = 200;

var bucket_name = 'test-gator';
var metadata_table = 'test-datasets';
var data_table = 'data';
var split_queue = 'SplitQueue';

let config = {};

try {
    config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    metadata_table = config.tables.datasets;
    data_table = config.tables.data;
    split_queue = config.queue.SplitQueue;
} catch (e) {
}

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
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
    return (data.Items && data.Items.length > 0) ? data.Items[0].md5 : null;
  });
};

var upload_metadata_dynamodb_from_s3 = function upload_metadata_dynamodb_from_s3(set,group,options) {
  return dynamo.put({'TableName' : metadata_table, 'Item' : {
    'accessions' : options.accessions,
    'id' : set,
    'group_id' : group
  }}).promise();
};

var upload_metadata_dynamodb_from_db = function upload_metadata_dynamodb_from_db(set_id,group_id,options) {
  if (options.remove) {
    // Don't need to remove the group id as it's already deleted
    return Promise.resolve(true);
  }
  var params;
  if (options.md5 && ! options.notmodified) {
    let metadata = {};
    metadata.mimetype = (options.metadata || {}).mimetype || 'application/json';
    metadata.title = (options.metadata || {}).title || 'Untitled';
    if ( (options.metadata || {}).doi ) {
      metadata.doi = [].concat( (options.metadata || {}).doi );
    }
    console.log("Derived metadata to be ",metadata);
    // This is new data being inserted into
    // the database
    params = {
     'TableName' : data_table,
     'Key' : {'acc' : 'metadata', 'dataset' : set_id },
     'UpdateExpression': 'SET #md5 = :md5, #metadata = :metadata ADD #gids :group',
      'ExpressionAttributeValues': {
          ':group': dynamo.createSet([ group_id ]),
          ':md5'  : options.md5,
          ':metadata' : metadata
      },
      'ExpressionAttributeNames' : {
        '#gids' : 'group_ids',
        '#md5' : 'md5',
        '#metadata' : 'metadata'
      }
    };
  } else {
    // We should only update the group membership for the
    // file because we have the data inserted already
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
  console.log("Adding ",group_id," to set ",set_id," with meta ",options.md5,options.notmodified ? "Not modified" : "Modified");
  return dynamo.update(params).promise();
};

var upload_metadata_dynamodb = function(set,group,meta) {
  return upload_metadata_dynamodb_from_db(set,group,meta);
};

let uploader = null;

// This should really be a stream so that
// we don't end up keeping all the elements
// in memory.. filter?

var upload_data_record_db = function upload_data_record_db(key,data,offset,byte_offset) {

  if ( ! key ) {
    return uploader.finished.promise;
  }

  var key_elements = key ? key.split('/') : [];
  var set_ids = (key_elements[2] || '').split(':');
  var group_id = set_ids[0];
  var set_id = set_ids[1];

  if ( ! uploader ) {
    console.log("Starting uploader");
    uploader = require('./dynamodb_rate').createUploadPipe(data_table,set_id,group_id,offset);
    uploader.capacity = MAX_WRITE_CAPACITY;
    uploader.byte_offset = byte_offset;
    data.pipe(uploader.data);
    uploader.start();
  }

  return uploader.finished.promise.then(function() {
    uploader = null;
  });
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

var upload_data_record = function upload_data_record(key,data,offset,byte_offset) {
  return upload_data_record_db(key,data,offset,byte_offset);
};

var retrieve_file_s3 = function retrieve_file_s3(filekey,md5_result,byte_offset) {
  var params = {
    'Key' : filekey,
    'Bucket' : bucket_name,
    'IfNoneMatch' : md5_result.old+"",
  };
  if (byte_offset) {
    params.Range = 'bytes='+byte_offset+'-'
  }
  var request = s3.getObject(params);
  var stream = request.createReadStream();
  stream.on('finish',function() {
    md5_result.md5 = request.response.data.ETag;
  });
  return stream;
};

var retrieve_file_local = function retrieve_file_local(filekey) {
  return fs.createReadStream(filekey);
};

var retrieve_file = function retrieve_file(filekey,md5_result,byte_offset) {
  return retrieve_file_s3(filekey,md5_result,byte_offset);
};

var remove_folder = function remove_folder(setkey) {
  return remove_folder_db(setkey);
};

var remove_folder_db = function remove_folder_db(setkey) {
  let group_id, dataset_id;
  let ids = setkey.split(':');
  group_id = ids[0];
  let set_id = ids[1];
  if ( ! group_id ) {
    return Promise.resolve();
  }
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
};

var remove_data = function remove_data(filekey) {
  var filekey_components = filekey.split('/');
  var group_id = filekey_components[2];
  var dataset_id = filekey_components[1];
  return remove_folder(group_id+":"+dataset_id).then(function() {
    return upload_metadata_dynamodb(dataset_id,group_id,{'remove': true});
  });
};

var split_file = function split_file(filekey,skip_remove,current_md5,offset,byte_offset) {
  var filekey_components = filekey.split('/');
  var group_id = filekey_components[2];
  var dataset_id = filekey_components[1];

  if ( ! dataset_id ) {
    return Promise.reject(new Error('No dataset id'));
  }

  if (! skip_remove) {
    return remove_folder(group_id+":"+dataset_id).then(function() {
      return split_file(filekey,true,current_md5,offset,byte_offset);
    });
  }
  if ( ! group_id ) {
    return Promise.resolve();
  }
  var md5_result = { old: current_md5 };

  let byte_offsetter = new Offsetter(byte_offset);

  var rs = retrieve_file(filekey,md5_result,byte_offset).pipe(byte_offsetter);
  var upload_promises = [];

  console.log("Performing an upload for ",group_id,dataset_id,md5_result," reading starting at ",byte_offset);


  let entry_data = rs.pipe(JSONStream.parse(['data', {'emitKey': true}]));
  upload_promises.push( upload_data_record("data/latest/"+group_id+":"+dataset_id, entry_data,offset,byte_offsetter) );

  rs.pipe(new MetadataExtractor()).on('data',function(dat) {
    let metadata_uploaded = Promise.all([].concat(upload_promises)).then(function() {
      return upload_metadata_dynamodb(dataset_id,group_id,{'metadata': dat, 'md5' : md5_result.md5 });
    }).catch(function(err) {
      console.log(err);
      throw err;
    });
    upload_promises.push(metadata_uploaded);
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
      entry_data.end();
      console.log("File not modified, skipping splitting");
      return upload_metadata_dynamodb(dataset_id,group_id,{'notmodified' : true});
    }
  });
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

var download_all_data_db = function(accession,grants,dataset) {
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
  if (dataset) {
    params.KeyConditionExpression = 'acc = :acc and dataset = :dataset';
    params_metadata.KeyConditionExpression = 'acc = :acc and dataset = :dataset';
    params.ExpressionAttributeValues[':dataset'] = dataset;
    params_metadata.ExpressionAttributeValues[':dataset'] = dataset;
  }
  return Promise.all([
    dynamo.query(params).promise(),
    dynamo.query(params_metadata).promise()
  ]).then(function(data) {
    var meta_data = data[1];
    var db_data = data[0];
    return Promise.all(db_data.Items.map(inflate_item)).then(function(items) {
      return meta_data.Items.concat(items);
    });
  }).then(filter_db_datasets.bind(null,grants)).then(function(results) {
    if (results.length <= 1 && dataset) {
      return results[0];
    }
    return results;
  });
};

var filter_db_datasets = function(grants,data) {
  var sets = [];
  var accession = null;

  let metadatas = {};

  data.filter(function(data) {
    if (data.acc !== 'metadata') {
      accession = data.acc;
    }
    return data.acc == 'metadata';
  }).forEach(function(set) {
    metadatas[set.dataset] = set.metadata;
    sets = sets.concat((set.group_ids || {'values':[]}).values.map(function(group) { return { group_id: group, id: set.dataset }; }));
  });
  console.log("Metadatas for data is ",JSON.stringify(metadatas),JSON.stringify(sets));
  var valid_sets = [];
  // Filter metadata by the JWT permissions
  sets.forEach(function(set) {
    let valid_prots = null;
    if (grants[set.group_id+'/'+set.id]) {
      valid_prots = grants[set.group_id+'/'+set.id];
      if (accession == 'publications' || valid_prots.filter(function(id) { return id == '*' || id.toLowerCase() == accession; }).length > 0) {
        valid_sets.push(set.id);
      }
    }
    if (grants[set.group_id+'/*']) {
      valid_prots = grants[set.group_id+'/*'];
      if (accession == 'publications' || valid_prots.filter(function(id) { return id == '*' || id.toLowerCase() == accession; }).length > 0) {
        valid_sets.push(set.id);
      }
    }
  });
  console.log("Valid sets are ",valid_sets.join(','));
  console.log("Returned sets are ",data.map(function(dat) { return dat.dataset; }));
  let valid_data = data.filter(function(dat) {
    dat.metadata = metadatas[dat.dataset];
    return dat.acc !== 'metadata' && valid_sets.indexOf(dat.dataset) >= 0;
  });
  console.log("We allowed ",valid_data.length," entries ");
  return valid_data;
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

var download_all_data_s3 = function(accession,grants,dataset) {
  // Get metadata entries that contain the desired accession
  var start_time = (new Date()).getTime();
  console.log("datasets_containing_acc start ");
  return datasets_containing_acc(accession).then(function(sets) {
    console.log("datasets_containing_acc end ",(new Date()).getTime() - start_time);
    console.log(sets);
    var valid_sets = [];
    sets.forEach(function(set) {
      let valid_prots = null;
      // Filter metadata by the JWT permissions

      if (grants[set.group_id+'/'+set.id]) {
        valid_prots = grants[set.group_id+'/'+set.id];
        if (acc == 'publications' || valid_prots.filter(function(id) { return id == '*' || id.toLowerCase() == accession; }).length > 0) {
          valid_sets.push(set.group_id+':'+set.id);
        }
      }
      if (acc == 'publications' || grants[set.group_id+'/*']) {
        valid_prots = grants[set.group_id+'/*'];
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

var download_all_data = function(accession,grants,dataset) {
  return download_all_data_db(accession,grants,dataset);
};

var combine_sets = function(entries) {
  if ( ! entries || ! entries.map ) {
    return entries;
  }
  var results = {"data" : []};
  results.data = entries.map(function(entry) { return entry; });
  return results;
};

let set_write_capacity = function(capacity) {
  var params = {
    TableName: data_table,
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: capacity
    }
  };
  let dynamo_client = new AWS.DynamoDB();
  return dynamo_client.updateTable(params).promise();
};


let reset_split_queue = function(queue,arn) {
  return queue.getActiveMessages().then(function(counts) {
    if (counts[0] > 0) {
      throw new Error("Already running");
    }
    if (counts[1] < 1) {
      throw new Error("No messages");
    }
  })
  .then( () => console.log("Increasing capacity to ",Math.floor(3/4*MAX_WRITE_CAPACITY)+10))
  .then( () => set_write_capacity(Math.floor(3/4*MAX_WRITE_CAPACITY)+10))
  .catch(function(err) {
    if (err.message == 'No messages') {
      return shutdown_split_queue().then(function() {
        throw err;
      });
    }
    if (err.code !== 'ValidationException') {
      throw err;
    }
  })
  .then( () => Events.setTimeout('runSplitQueue',new Date(new Date().getTime() + 2*60*1000)) )
  .then(function(newrule) {
      console.log("Making sure function is subscribed to event");
      return Events.subscribe('runSplitQueue',arn,{ 'time' : 'triggered' });
  }).catch(function(err) {
    console.log(err);
  });
};

let shutdown_split_queue = function() {
  console.log("Reducing capacity down to ",MIN_WRITE_CAPACITY);
  return set_write_capacity(MIN_WRITE_CAPACITY)
  .catch(function(err) {
    if (err.code !== 'ValidationException') {
      throw err;
    }
  }).then(function() {
    console.log("Clearing event rule");
    return Events.setTimeout('runSplitQueue',new Date(new Date().getTime() - 60*1000));
  });
};


// Timings for runQueue

// Run runQueue every 8 hours
//   Always setTimeout(5 mins) (from start of execution)
//   - On empty queue setTimeout(+ 8 hours)
//   - On early finish of upload, setTimeout(2 seconds) (from end of upload)
//   - If it doesn't look like we will finish - pause execution, push the
//     offset back onto the queue.
//   - Discard item from the queue (unless there's an error, so put it back)

var runSplitQueue = function(event,context) {

  let queue = new Queue(split_queue);

  if (! event.time ) {
    console.log("Initialising scheduler");
    Events.setInterval('scheduleSplitQueue','8 hours').then(function(newrule) {
      if (newrule) {
        return Events.subscribe('scheduleSplitQueue',context.invokedFunctionArn,{ 'time' : 'scheduled' });
      }
    }).then(function() {
      context.succeed('OK');
    });
    return;
  }

  if (event.time && event.time == 'scheduled') {
    reset_split_queue(queue,context.invokedFunctionArn).then(function() {
      context.succeed('OK');
    });
    return;
  }

  console.log("Getting queue object");
  let timelimit = null;
  uploader = null;

  // We should do a pre-emptive subscribe for the event here
  console.log("Setting timeout for event");
  let self_event = Events.setTimeout('runSplitQueue',new Date(new Date().getTime() + 6*60*1000));
  return self_event.then(() => queue.shift(1)).then(function(messages) {
    console.log("Got queue messages ",messages.map((message) => message.Body));
    if ( ! messages || ! messages.length ) {
      // Modify table, reducing write capacity
      console.log("No more messages. Reducing capacity");
      return shutdown_split_queue();
    }

    let message = messages[0];
    let message_body = JSON.parse(message.Body);
    // Modify table, increasing write capacity if needed

    timelimit = setTimeout(function() {
      // Wait for any requests to finalise
      // then look at the queue.
      console.log("Ran out of time splitting file");
      uploader.stop().then(function() {
        let last_item = uploader.queue[0];
        if (! last_item ) {
          last_item = uploader.last_acc;
        } else {
          last_item = last_item.PutRequest.Item.acc;
        }
        console.log("First item on queue ",last_item );
        let current_byte_offset = uploader.byte_offset.offset;
        if (current_byte_offset < 0) {
          current_byte_offset = 0;
        }
        return queue.sendMessage({'path' : message_body.path, 'offset' : last_item, 'byte_offset' : current_byte_offset });
      }).catch(function(err) {
        console.log(err.stack);
        console.log(err);
      }).then(function() {
        uploader = null;
        return message.finalise();
      });
    },(60*5 - 10)*1000);

    let result = get_current_md5(message_body.path)
    .then((md5) => split_file(message_body.path,null,md5,message_body.offset,message_body.byte_offset))
    .then(function() {
      uploader = null;
      clearTimeout(timelimit);
      console.log("Finished reading file, calling runSplitQueue immediately, will run again at ",new Date(new Date().getTime() + 2*60*1000));
      return message.finalise().then( () => Events.setTimeout('runSplitQueue',new Date(new Date().getTime() + 2*60*1000)));
    });
    return result;
  }).then(function(ok) {
    clearTimeout(timelimit);
    context.succeed('OK');
  }).catch(function(err) {
    clearTimeout(timelimit);
    uploader = null;
    console.log(err.stack);
    console.log(err);
    context.succeed('NOT-OK');
  });
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
  var token = event.Authorization.split(' ');
  var accession = event.acc.toLowerCase();

  if(token[0] !== 'Bearer'){
    context.succeed('NOT-OK');
    return;
  }

  var grants = event.grants ? JSON.parse(event.grants) : {};

  var dataset = (event.dataset || '').split(':')[1];

  // Decode JWT
  // Get groups/datasets that can be read
  let start_time = null;

  download_all_data(accession,grants,dataset).then(function(entries) {
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
    let queue = new Queue(split_queue);
    result_promise = queue.sendMessage({'path' : filekey });
    // result_promise = get_current_md5(filekey)
    // .then((md5) => split_file(filekey,null,md5))
    // .then(function() {
    //   uploader = null;
    // });
  }
  result_promise.then(function(done) {
    console.log("Processed all components");
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
exports.runSplitQueue = runSplitQueue;
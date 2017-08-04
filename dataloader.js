'use strict';
/*jshint esversion: 6, node:true */

const fs = require('fs');
const JSONStream = require('JSONStream');
const crypto = require('crypto');
const zlib = require('zlib');

const Queue = require('lambda-helpers').queue;
const Events = require('lambda-helpers').events;

const MetadataExtractor = require('./dynamodb_rate').MetadataExtractor;
const Offsetter = require('./dynamodb_rate').Offsetter;

const dataremover = require('./dataremover');

const MIN_WRITE_CAPACITY = 1;
const MAX_WRITE_CAPACITY = 200;
const DEFAULT_READ_CAPACITY = process.env.DEFAULT_READ_CAPACITY ? process.env.DEFAULT_READ_CAPACITY : 1;

let bucket_name = 'test-gator';
let data_table = 'data';
let split_queue = 'SplitQueue';
let runSplitQueueRule = 'runSplitQueueRule';
let split_queue_machine = 'StateSplitQueue';
let split_queue_topic = 'splitQueueTopic';

let config = {};

try {
    config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    data_table = config.tables.data;
    split_queue_machine = config.stepfunctions.StateSplitQueue;
    split_queue = config.queue.SplitQueue;
    runSplitQueueRule = config.rule.runSplitQueueRule;
    split_queue_topic = config.queue.SplitQueueTopic;
} catch (e) {
}

const AWS = require('lambda-helpers').AWS;

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
}

const s3 = new AWS.S3();
const dynamo = new AWS.DynamoDB.DocumentClient();
const sns = require('lambda-helpers').sns;
const stepfunctions = new AWS.StepFunctions();

let read_capacity = 1;


const get_current_md5 = function get_current_md5(filekey) {
  let filekey_components = filekey.split('/');
  let group_id = filekey_components[2];
  let dataset_id = filekey_components[1];
  let params_metadata = {
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

const upload_metadata_dynamodb_from_s3 = function upload_metadata_dynamodb_from_s3(set,group,options) {
  return dynamo.put({'TableName' : metadata_table, 'Item' : {
    'accessions' : options.accessions,
    'id' : set,
    'group_id' : group
  }}).promise();
};

const fix_empty_strings = function(meta) {
  if (Array.isArray(meta)) {
    meta.forEach(function(val,idx) {
      if (val === '' || val === null) {
        meta[idx] = ' ';
      }
      if (typeof val === 'object') {
        fix_empty_strings(val);
      }
    });
    return;
  }
  Object.keys(meta).forEach(function(key) {
    if (typeof meta[key] === 'object') {
      fix_empty_strings(meta[key]);
    } else {
      if (meta[key] === null || meta[key] === '') {
        console.log("Removing key ",key);
        delete meta[key];
      }
    }
  });
};

const upload_metadata_dynamodb_from_db = function upload_metadata_dynamodb_from_db(set_id,group_id,options) {
  if (options.remove) {
    // Don't need to remove the group id as it's already deleted
    return Promise.resolve(true);
  }
  let params;
  let doi_promise = Promise.resolve();
  let dataset_promise = append_dataset_to_list_dynamodb(group_id+'/'+set_id);

  if (options.md5 && ! options.notmodified) {
    let metadata = options.metadata || {};
    metadata.mimetype = metadata.mimetype || 'application/json';
    metadata.title = metadata.title || 'Untitled';
    let notify_promise = sns.publish({topic: split_queue_topic, 'Message' : JSON.stringify({Bucket: bucket_name, Key: 'uploads/'+set_id+'/'+group_id }) });
    let append_doi_promise = append_doi_dynamodb(set_id,metadata.doi);
    doi_promise = Promise.all([notify_promise,append_doi_promise]);

    fix_empty_strings(metadata);

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
  return dynamo.update(params).promise().then(() => dataset_promise ).then(() => doi_promise);
};

var append_dataset_to_list_dynamodb = function append_dataset_to_list_dynamodb(set_id,remove) {
  let params = {
   'TableName' : data_table,
   'Key' : {'acc' : 'metadata', 'dataset' : 'datasets' }
  };
  params['UpdateExpression'] = remove? 'DELETE #sets :set' : 'ADD #sets :set';
  params['ExpressionAttributeValues'] = {
      ':set': dynamo.createSet([ set_id ]),
  };
  params['ExpressionAttributeNames'] = {
    '#sets' : 'sets'
  };
  console.log((remove ? "Removing" : "Adding"),set_id,"to list of sets");
  return dynamo.update(params).promise().then( () => console.log("Updated list of sets ",set_id));
};

var append_doi_dynamodb = function append_doi_dynamodb(set_id,doi) {
  let params = {
   'TableName' : data_table,
   'Key' : {'acc' : 'publications', 'dataset' : set_id }
  };
  if (doi) {
    params['UpdateExpression'] = 'ADD #dois :doi';
    params['ExpressionAttributeValues'] = {
        ':doi': dynamo.createSet([ doi ]),
    };
    params['ExpressionAttributeNames'] = {
      '#dois' : 'dois'
    };
    console.log("Setting DOI",doi,"to set",set_id);
  } else {
    console.log("Adding dummmy publication entry");
  }
  return dynamo.update(params).promise();
};

var upload_metadata_dynamodb = function(set,group,meta) {
  return upload_metadata_dynamodb_from_db(set,group,meta);
};

let uploader = null;

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

  return uploader.finished.promise;
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
  return dynamo.update(params).promise().then( () => append_dataset_to_list_dynamodb(group_id+'/'+set_id,true));
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
    console.log("No group id, not uploading");
    return Promise.resolve();
  }
  var md5_result = { old: current_md5 };

  let byte_offsetter = new Offsetter(byte_offset);

  var data_stream = retrieve_file(filekey,md5_result,byte_offset)
  var rs = data_stream.pipe(byte_offsetter);
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
      resolve();
    });
    rs.on('error',function(err) {
      reject(err);
    });
    data_stream.on('error',function(err) {
      reject(err);
    });
  }).catch(function(err) {
    console.log("Removing uploader in error handler");
    uploader = null;
    if (err.statusCode == 404) {
      entry_data.end();
      console.log("File no longer exists, skipping splitting");
      upload_promises.length = 0;
    } else if (err.statusCode == 304) {
      entry_data.end();
      console.log("File not modified, skipping splitting");
      upload_promises.length = 0;
      upload_promises.push(upload_metadata_dynamodb(dataset_id,group_id,{'notmodified' : true}));
    } else {
      throw err;
    }
  }).then(() => Promise.all(upload_promises))
    .then(function() {
      console.log("Removing uploader in split_file");
      uploader = null;
    })
    .then( () => "All upload promises resolved");
};

let set_write_capacity = function(capacity) {
  let target_read_capacity = Math.max(read_capacity,DEFAULT_READ_CAPACITY);
  var params = {
    TableName: data_table,
    ProvisionedThroughput: {
      ReadCapacityUnits: target_read_capacity,
      WriteCapacityUnits: capacity
    }
  };
  let dynamo_client = new AWS.DynamoDB();
  return dynamo_client.updateTable(params).promise();
};

let startSplitQueue = function(event,context) {
  let count = 0;
  let queue = new Queue(split_queue);
  Promise.all([ queue.getActiveMessages(), dataremover.setsToRemove() ]).then(function(messages) {
    let counts = messages[0];
    let sets_to_remove = messages[1];

    if (sets_to_remove.length > 0) {
      read_capacity = 50;
    }

    if (counts[0] > 0) {
      throw new Error("Already running");
    }
    if (counts[1] < 1 && sets_to_remove.length < 1) {
      throw new Error("No messages");
    }
    count = counts[1] + sets_to_remove.length;
  })
  .then( () => console.log("Increasing capacity to ",Math.floor(3/4*MAX_WRITE_CAPACITY)+10))
  .then( () => set_write_capacity(Math.floor(3/4*MAX_WRITE_CAPACITY)+10))
  .then( () => context.succeed({status: 'OK', messageCount: count }))
  .catch(function(err) {
    if (err.message == 'No messages') {
      context.succeed({ status: 'OK', messageCount: 0 });
      return;
    }
    if (err.message == 'Already running') {
      context.fail({ status: 'running' });
      return;
    }
    if (err.code !== 'ValidationException') {
      context.fail({ status: err.message });
      return;
    } else {
      context.succeed({ status: 'OK', messageCount: count })
    }
  });
};

let endSplitQueue = function(event,context) {
  set_write_capacity(MIN_WRITE_CAPACITY)
  .catch(function(err) {
    if (err.code !== 'ValidationException') {
      throw err;
    }
  }).then( () => {
    context.succeed({status: 'OK'});
  }).catch( err => {
    context.fail({ status: err.message });
  });
};

let stepSplitQueue = function(event,context) {

  let queue = new Queue(split_queue);

  console.log("Getting queue object");
  let timelimit = null;
  uploader = null;
  let message_promise = Promise.resolve([]);
  if (event.status && event.status == 'unfinished') {
    message_promise = Promise.resolve([event.message]);
  } else {
    message_promise = queue.shift(1).then( messages => {
      console.log("Got queue messages ",messages.map((message) => message.Body));
      return messages;
    });
  }


  return message_promise.then(function(messages) {
    if ( ! messages || ! messages.length ) {
      throw new Error('No messages');
    }

    let message = messages[0];
    if ( ! message.finalise ) {
      message.finalise = function() {
        return Promise.resolve();
      };
    }
    let message_body = message.Body ? JSON.parse(message.Body) : message;
    let last_item = null;
    let current_byte_offset = null;

    timelimit = setTimeout(function() {
      // Wait for any requests to finalise
      // then look at the queue.
      console.log("Ran out of time splitting file");
      uploader.stop().then(function() {
        last_item = uploader.queue[0];
        if (! last_item ) {
          last_item = uploader.last_acc;
        } else {
          last_item = last_item.PutRequest.Item.acc;
        }
        console.log("First item on queue ",last_item );
        current_byte_offset = uploader.byte_offset.offset;
        if (current_byte_offset < 0) {
          current_byte_offset = 0;
        }
        console.log("Finalising message");
        return message.finalise();
      }).catch(function(err) {
        console.log(err.stack);
        console.log(err);
      }).then(function() {
        console.log("Removing uploader after timeout");
        uploader = null;
        console.log("Sending state");
        return context.succeed({ status: 'unfinished',
                          messageCount: 1,
                          message: {'path' : message_body.path,
                                    'offset' : last_item,
                                    'byte_offset' : current_byte_offset
                                   }
                        });
      }).catch(function(err) {
        console.log(err.stack);
        console.log(err);
        context.fail({state: err.message });
      });
    },(60*5 - 10)*1000);

    let result = get_current_md5(message_body.path)
    .then((md5) => split_file(message_body.path,null,message_body.offset === 'dummy' ? '0' : md5,message_body.offset,message_body.byte_offset))
    .then(function() {
      console.log("Done uploading");
      clearTimeout(timelimit);
      console.log("Removing uploader after finished");
      uploader = null;
      return message.finalise().then( () => {
        return queue.getActiveMessages()
      }).then(function(counts) {
        context.succeed({state: 'OK', messageCount: counts[1] })
      });
    });
    return result;
  }).catch(function(err) {
    console.log("Hit an error",err);
    clearTimeout(timelimit);
    console.log("Removing uploader in error handler");
    uploader = null;
    if (err.message == 'No messages') {
      context.succeed({ status: 'OK', messageCount: 0 });
      return;
    }
    console.log(err.stack);
    console.log(err);
    context.fail({state: err.message });
  });
};

// Timings for runQueue

// Run runQueue every 8 hours
var runSplitQueue = function(event,context) {
  let params = {
    stateMachineArn: split_queue_machine,
    input: '{}',
    name: ('SplitQueue '+(new Date()).toString()).replace(/[^A-Za-z0-9]/g,'_')
  };
  stepfunctions.startExecution(params).promise().then( () => {
    context.succeed({'status' : 'OK'});
  }).catch( err => {
    console.log(err);
    context.fail({'status' : err.message });
  });
};

const extract_changed_keys = function(event) {
  if ( ! event.Records ) {
    return [];
  }
  let results = event.Records
  .filter( rec => rec.Sns )
  .map( rec => {
    let sns_message = JSON.parse(rec.Sns.Message);
    return sns_message.Records.filter(sns_rec => sns_rec.s3 ).map( sns_rec => {
      return { bucket : sns_rec.s3.bucket.name, key: sns_rec.s3.object.key, operation: sns_rec.eventName };
    });
  });
  results = [].concat.apply([],results);
  return results.filter( obj => obj.bucket == bucket_name );
};

var splitFiles = function splitFiles(event,context) {
  let changes = extract_changed_keys(event);
  console.log("Changed files ",changes);
  let queue = new Queue(split_queue);
  let promises = changes.map( change => {
    if (change.operation.match(/ObjectRemoved/)) {
      console.log("Remove data at ",change.key);
      return remove_data(change.key);
    }
    if (change.operation.match(/ObjectCreated/)) {
      console.log("Splitting data at ",change.key);
      return queue.sendMessage({'path' : change.key });
    }
  });
  Promise.all(promises).then(function(done) {
    console.log("Processed all components");
    context.succeed('OK');
  }).catch(function(err) {
    console.error(err);
    console.error(err.stack);
    context.succeed('NOT-OK');
  });
};

var refreshMetadata = function() {
  var s3 = new AWS.S3();
  var params = {
    Bucket: bucket_name,
    Prefix: "uploads/"
  };
  return s3.listObjectsV2(params).promise().then(function(result) {
    let messages = result.Contents.map( (dataset) => { return { "path" : dataset.Key, "offset" : "dummy", "byte_offset" : dataset.Size > (1024*50) ? dataset.Size - (1024*50) : 0 }; } );
    messages = messages.filter( (message) => message.path.indexOf('o_man') >= 0 );
    console.log(messages);
    return messages;
  }).then((messages) => {
    let queue = new Queue(split_queue);
    Promise.all(messages.map( message => queue.sendMessage(message) ))
  }).then( () => {
    return require('lambda-helpers').lambda_promise(runSplitQueue)({'time' : 'triggered'});
  });
};

var refreshData = function() {
  filter_db_datasets = function(grants,data) {
    return data.filter((dat) => dat.acc === 'metadata');
  };
  return new Promise(function(resolve,reject) {
    download_all_data('dummy',{}).then(function(metas) {
      return Promise.all(metas.filter( meta => meta.group_ids ).map(function(meta) {
        return upload_metadata_dynamodb_from_db(meta.dataset,meta.group_ids.values[0],{'notmodified' : false, 'metadata' : metas.metadata, 'md5' : '0' })
        .then(() => (new Queue(split_queue)).sendMessage({'path' : 'uploads/'+meta.dataset+'/'+meta.group_ids.values[0] }));
      }));
    }).then(() => console.log("Starting queue up") && runSplitQueue({'time' : 'scheduled'},{'succeed' : resolve})).catch(reject);
  });
};

exports.runSplitQueue = runSplitQueue;

exports.startSplitQueue = startSplitQueue;
exports.endSplitQueue = endSplitQueue;
exports.stepSplitQueue = stepSplitQueue;

exports.refreshData = refreshData;
exports.refreshMetadata = refreshMetadata;
exports.splitFiles = splitFiles;
'use strict';
/*jshint esversion: 6, node:true */

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
const DEFAULT_READ_CAPACITY = process.env.DEFAULT_READ_CAPACITY ? process.env.DEFAULT_READ_CAPACITY : 1;
const USE_BATCH_RETRIEVE = process.env.ENABLE_BATCH_RETRIEVE ? true : false;


var bucket_name = 'test-gator';
var metadata_table = 'test-datasets';
var data_table = 'data';
var split_queue = 'SplitQueue';
var runSplitQueueRule = 'runSplitQueueRule';
var split_queue_machine = 'StateSplitQueue';
var split_queue_topic = 'splitQueueTopic';

let config = {};

try {
    config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    metadata_table = config.tables.datasets;
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
const all_sets = [];

// We wish to lazily load the metadataConverter whenever we
// are splitting files, so we only require the
// module from within the split_files method
let metadataConverter = null;

let datasetnames = Promise.resolve();

if (USE_BATCH_RETRIEVE) {
  datasetnames = dynamo.get({'TableName' : data_table, 'Key' : { 'acc' : 'metadata', 'dataset' : 'datasets' }}).promise().then( (data) => {
    console.log('Populating data sets');
    all_sets.length = 0;
    data.Item.sets.values.forEach( set => all_sets.push(set));
    console.log('We have ',all_sets.length, 'sets in total');
  });
}

const onlyUnique = function(value, index, self) {
    return self.indexOf(value) === index;
};

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

var fix_empty_strings = function(meta) {
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

var upload_metadata_dynamodb_from_db = function upload_metadata_dynamodb_from_db(set_id,group_id,options) {
  if (options.remove) {
    // Don't need to remove the group id as it's already deleted
    return Promise.resolve(true);
  }
  var params;
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

var update_metadata = function(metadata) {
  if (! metadata.sample) {
    return Promise.resolve();
  }
  if (metadata.sample.tissue) {
    return metadataConverter.convert( metadata.sample.tissue ).then( converted => {
      if ( ! converted.root ) {
        return;
      }
      metadata.sample.uberon = converted.root;
      metadata.sample.description = converted.name;
    });
  }
  return Promise.resolve();
};

var upload_metadata_dynamodb = function(set,group,meta) {
  if (meta.metadata) {
    return update_metadata(meta.metadata).then( () => upload_metadata_dynamodb_from_db(set,group,meta) );
  }
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
  metadataConverter = require('node-uberon-mappings');

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

var get_homologues_db = function(accession) {
  return download_all_data_db(accession,{'homology/homology': ['*']},'homology')
         .then(function(homologues) {
            if (! homologues || ! homologues.data) {
              return {'homology' : []};
            }
            let family = homologues.data.family;
            homologues = {'homology' : homologues.data.homology.filter( (id) => id.toLowerCase() !== accession ) };
            return download_all_data_db(family, { 'homology/homology_alignment' : ['*']},'homology_alignment').then(function(alignments) {
              homologues.alignments = alignments;
              return homologues;
            });
         });
};

var metadata_promise;

var download_all_data_db = function(accession,grants,dataset) {

  if (USE_BATCH_RETRIEVE) {
    return download_all_data_db_batch(accession,grants,dataset);
  }

  return download_all_data_db_query(accession,grants,dataset);
};

let dynamo_process_items = function(params,existing,data) {
    if (data.UnprocessedKeys[data_table]) {
      params.RequestItems[data_table].Keys = data.UnprocessedKeys[data_table].Keys;
      return dynamo.batchGet(params).promise().then(dynamo_process_items.bind(null,params,data.Responses[data_table]));
    }
    return existing ? existing.concat(data.Responses[data_table]) : data.Responses[data_table];
};

var download_all_data_db_batch = function(accession,grants,dataset) {
  console.time('download_all_data_db_batch');
  let total_sets = [ dataset ];
  let set_names = all_sets.map( (set) => set.split('/')[1] ).filter(onlyUnique);
  if (! dataset) {
    total_sets = set_names;
  }
  let query_keys = total_sets.map( (set) => { return { 'acc' : accession, 'dataset' : set } });
  let meta_keys = set_names.map( (set) => { return { 'acc' : 'metadata', 'dataset' : set } });
  if (metadata_promise) {
    meta_keys = [];
  }
  if (accession === 'metadata') {
    query_keys = [];
  }
  let params = {
    RequestItems: {},
    ExpressionAttributeNames: {
      '#sample': 'sample',
      '#title' : 'title',
      '#data' : 'data'
    },
    ProjectionExpression : 'acc,dataset,group_ids,dois,#data,metadata.mimetype,metadata.#sample,metadata.#title'
  };
  params.RequestItems[data_table] = { 'Keys' : query_keys.concat(meta_keys)};
  let all_items;
  if(params.RequestItems[data_table].Keys.length == 0) {
    all_items = Promise.resolve([]);
  } else {
    all_items = dynamo.batchGet(params).promise()
    .then(dynamo_process_items.bind(null,params,null))
    .then(function(items) {
      console.timeEnd('download_all_data_db_batch');
      return items;
    });
  }
  if (! metadata_promise) {
    metadata_promise = all_items.then( (items) => {
      return items.filter( (item) => item.acc === 'metadata');
    });
  }
  let inflated_items = all_items.then( (items) => {
    console.time('download_all_data_inflate');
    return Promise.all(items.filter((item) => item.acc !== 'metadata').map(inflate_item)).then( (inflated) => {
      console.timeEnd('download_all_data_inflate');
      return inflated;
    });
  })
  return Promise.all([ inflated_items , metadata_promise ])
  .then( items => items[0].concat(items[1]) )
  .then(filter_db_datasets.bind(null,grants)).then(function(results) {
    if (results.length <= 1 && dataset) {
      return results[0];
    }
    return results;
  });

};

var download_all_data_db_query = function(accession,grants,dataset) {
  console.time('download_all_data_db_query');
  var params = {
    TableName: data_table,
    KeyConditionExpression: 'acc = :acc',
    ExpressionAttributeValues: {
      ':acc': accession,
    },
    ExpressionAttributeNames: {
      '#sample': 'sample',
      '#title' : 'title',
      '#data' : 'data'
    },
    ProjectionExpression : 'acc,dataset,group_ids,dois,#data,metadata.mimetype,metadata.#sample,metadata.#title'
  };
  var params_metadata = {
    TableName: data_table,
    KeyConditionExpression: 'acc = :acc',
    ExpressionAttributeValues: {
      ':acc': 'metadata'
    },
    ExpressionAttributeNames: {
      '#sample': 'sample',
      '#title' : 'title',
      '#rdata' : 'rdata_file'
    },
    ProjectionExpression : 'acc,dataset,group_ids,metadata.mimetype,metadata.#sample,metadata.#title,#rdata'
  };
  if (dataset) {
    params.KeyConditionExpression = 'acc = :acc and dataset = :dataset';
    params_metadata.KeyConditionExpression = 'acc = :acc and dataset = :dataset';
    params.ExpressionAttributeValues[':dataset'] = dataset;
    params_metadata.ExpressionAttributeValues[':dataset'] = dataset;
  }
  if ( ! metadata_promise && ! dataset ) {
    metadata_promise = dynamo.query(params_metadata).promise();
  }
  return Promise.all([
    dynamo.query(params).promise(),
    dataset ? dynamo.query(params_metadata).promise() : metadata_promise
  ]).then(function(data) {
    console.timeEnd('download_all_data_db_query');
    var meta_data = data[1];
    var db_data = data[0];
    console.time('download_all_data_inflate');
    return Promise.all(db_data.Items.map(inflate_item)).then(function(items) {
      console.timeEnd('download_all_data_inflate');
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
    (metadatas[set.dataset] || {}).rpackage = set.rdata_file;
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

var get_homologues = function(accession) {
  return get_homologues_db(accession);
};

var combine_sets = function(entries) {
  if ( ! entries || ! entries.map ) {
    return entries;
  }
  var results = {"data" : [], "retrieved" : new Date().toISOString()};
  results.data = entries.map(function(entry) { return entry; });
  return results;
};

let set_write_capacity = function(capacity) {
  var params = {
    TableName: data_table,
    ProvisionedThroughput: {
      ReadCapacityUnits: DEFAULT_READ_CAPACITY,
      WriteCapacityUnits: capacity
    }
  };
  let dynamo_client = new AWS.DynamoDB();
  return dynamo_client.updateTable(params).promise();
};

let startSplitQueue = function(event,context) {
  let count = 0;
  let queue = new Queue(split_queue);
  queue.getActiveMessages().then(function(counts) {
    if (counts[0] > 0) {
      throw new Error("Already running");
    }
    if (counts[1] < 1) {
      throw new Error("No messages");
    }
    count = counts[1];
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
  var accession = event.acc.toLowerCase();

  var grants = event.grants ? JSON.parse(event.grants) : {};

  Object.keys(grants).forEach( (set) => {
    if (grants.proteins[ grants[set][0] ]) {
      grants[set] = grants.proteins[ grants[set][0] ];
    }
  });
  delete grants.proteins;

  event.dataset = event.dataset || '';
  var dataset = (event.dataset.indexOf(':') < 0 ) ? event.dataset : event.dataset.split(':')[1];

  // Decode JWT
  // Get groups/datasets that can be read
  let start_time = null;

  let entries_promise = Promise.resolve([]);

  if (event.homology) {
    entries_promise = datasetnames.then( () => get_homologues(accession)).then(function(homologue_data) {
      console.time('homology');
      let homologues = homologue_data.homology;
      let alignments = homologue_data.alignments;
      return Promise.all( [ Promise.resolve([alignments]) ].concat(homologues.map( homologue => download_all_data(homologue.toLowerCase(),grants,dataset) ) ) );
    })
    .then( (sets) => { console.timeEnd('homology'); console.time('combine_sets'); return sets; })
    .then( (entrysets) => entrysets.reduce( (a,b) => a.concat(b) ) );
  } else {
    console.time('download_all_data')
    entries_promise = datasetnames.then( () => download_all_data(accession,grants,dataset)).then(function(entries) {
      console.timeEnd('download_all_data')
      console.time('combine_sets');
      return entries;
    });
  }

  entries_promise
  .then(combine_sets).then(function(combined) {
    console.timeEnd('combine_sets');
    context.succeed(combined);
  }).catch(function(err) {
    console.error(err);
    console.error(err.stack);
    context.succeed('NOT-OK');
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

exports.splitFiles = splitFiles;
exports.readAllData = readAllData;
exports.runSplitQueue = runSplitQueue;

exports.startSplitQueue = startSplitQueue;
exports.endSplitQueue = endSplitQueue;
exports.stepSplitQueue = stepSplitQueue;

exports.refreshData = refreshData;
exports.refreshMetadata = refreshMetadata;
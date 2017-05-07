'use strict';
/*jshint esversion: 6, node:true */

const zlib = require('zlib');

const USE_BATCH_RETRIEVE = process.env.ENABLE_BATCH_RETRIEVE ? true : false;

var bucket_name = 'test-gator';
var data_table = 'data';

let config = {};

try {
    config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    data_table = config.tables.data;
} catch (e) {
}

const AWS = require('lambda-helpers').AWS;

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
}

const s3 = new AWS.S3();
const dynamo = new AWS.DynamoDB.DocumentClient();
const all_sets = [];

let datasetnames = Promise.resolve();

if (USE_BATCH_RETRIEVE) {
  datasetnames = dynamo.get({'TableName' : data_table, 'Key' : { 'acc' : 'metadata', 'dataset' : 'datasets' }}).promise().then( (data) => {
    console.log('Populating data sets');
    all_sets.length = 0;
    data.Item.sets.values.forEach( set => all_sets.push(set));
    console.log('We have ',all_sets.length, 'sets in total');
  });
}

const fetch_query = function(params) {
  let results = [];
  let result_handler = items => {
    results = results.concat(items.Items);
    if (items.LastEvaluatedKey) {
      params.ExclusiveStartKey = items.LastEvaluatedKey;
      return dynamo.query(params).promise().then(result_handler);
    } else {
      return { Items: results };
    }
  };
  return dynamo.query(params).promise().then( result_handler );
};

const onlyUnique = function(value, index, self) {
    return self.indexOf(value) === index;
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
    FilterExpression: 'attribute_exists(group_ids)',
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
    metadata_promise = fetch_query(params_metadata);
  }
  return Promise.all([
    fetch_query(params),
    dataset ? fetch_query(params_metadata) : metadata_promise
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

exports.readAllData = readAllData;
exports.runSplitQueue = runSplitQueue;

exports.startSplitQueue = startSplitQueue;
exports.endSplitQueue = endSplitQueue;
exports.stepSplitQueue = stepSplitQueue;


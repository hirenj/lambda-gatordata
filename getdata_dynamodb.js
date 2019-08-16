'use strict';
/*jshint esversion: 6, node:true */

const USE_BATCH_RETRIEVE = process.env.ENABLE_BATCH_RETRIEVE ? true : false;

const zlib = require('zlib');

let bucket_name = 'test-gator';
let data_table = 'data';

let config = {};

try {
    config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    data_table = config.tables.data;
} catch (e) {
}

const AWS = require('lambda-helpers').AWS;

const dynamo = new AWS.DynamoDB.DocumentClient();

const all_sets = [];

let datasetnames = Promise.resolve();

let metadata_promise = null;

const onlyUnique = function(value, index, self) {
    return self.indexOf(value) === index;
};

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
    ProjectionExpression : 'acc,dataset,group_ids,metadata.mimetype,metadata.#sample,metadata.#title,#rdata,metadata.quantitation,,metadata.channel_samples'
  };
  if (dataset) {
    params.KeyConditionExpression = 'acc = :acc and dataset = :dataset';
    params_metadata.KeyConditionExpression = 'acc = :acc and dataset = :dataset';
    params.ExpressionAttributeValues[':dataset'] = dataset;
    params_metadata.ExpressionAttributeValues[':dataset'] = dataset;
    if (accession === 'metadata') {
      params.ProjectionExpression = 'acc,dataset,metadata';
      delete params.ExpressionAttributeNames;
    }
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
      for (let item of (meta_data.Items || [])) {
        item.is_meta = true;
      }
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
  let valid_data = data.filter(function(dat,idx) {
    dat.metadata = metadatas[dat.dataset];
    return (! dat.is_meta ) && valid_sets.indexOf(dat.dataset) >= 0;
  });
  console.log("We allowed ",valid_data.length," entries ");
  return valid_data;
};

exports.download_all_data = download_all_data_db;
exports.datasetnames = datasetnames;
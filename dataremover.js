'use strict';
/*jshint esversion: 6, node:true */

let data_table = 'data';

let config = {};

try {
    config = require('./resources.conf.json');
    data_table = config.tables.data;
} catch (e) {
}

const AWS = require('lambda-helpers').AWS;

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
}

const onlyUnique = function(value, index, self) {
    return self.indexOf(value) === index;
};

const dynamo = new AWS.DynamoDB.DocumentClient();

const rateLimit = require('rate-limit-promise');

let last_scan_key = null;
let current_scan_key = null;
let current_dataset = null;
let timed_out = false;

const execution_timeout = (4*60 + 45)*1000;

const MAX_READ_CAPACITY = 100;

const TimeoutPromise = function(ms, callback) {
  return new Promise(function(resolve, reject) {
    var self = this;
    // Set up the timeout
    let timer = setTimeout(() => {
      reject(new Error('Timed out'));
    }, ms);
    let cancelTimer = _ => {
      if (timer) {
        clearTimeout(timer);
        timer = 0;
      }
    };

    // Set up the real work
    callback(
        value => {
            cancelTimer();
            resolve(value);
        },
        error => {
            cancelTimer();
            reject(error);
        }
    );
  });
};

const get_datasets_to_remove = function() {
  let params = {
    TableName: data_table,
    KeyConditionExpression: 'acc = :acc',
    FilterExpression: 'attribute_not_exists(group_ids) and attribute_not_exists(#sets)',
    ProjectionExpression: 'acc,dataset',
    ExpressionAttributeValues: {
      ':acc': 'metadata'
    },
    ExpressionAttributeNames : {
      '#sets' : 'sets'
    }

  };
  return dynamo.query(params).promise().then(function(data) {
    return (data.Items && data.Items.length > 0) ? data.Items.map( item => item.dataset ) : [];
  });
};

const write_limiter = rateLimit(4, 1000);
let read_limiter = rateLimit(3,1000);

const delete_items = function(items) {
  if (items.length == 0) {
    return Promise.resolve(null);
  }
  let params = { 'RequestItems' : {},
                  ReturnConsumedCapacity: 'TOTAL',
                };
  console.log('Doing delete request with',items.length,'items for dataset ',items.map( item => item.dataset ).filter(onlyUnique));
  params.RequestItems[data_table] = items.map( item => { return { DeleteRequest: { Key: item } } });
  return write_limiter().then( () => dynamo.batchWrite(params).promise());
};



const handle_delete = function(items_to_delete,result) {
  if ( ! result ) {
    return;
  }
  if (result.UnprocessedItems[data_table]) {
    console.log('We have',result.UnprocessedItems[data_table].length,'unprocessed items, replacing');
    result.UnprocessedItems[data_table].map( item => item.DeleteRequest.Key ).forEach( key => items_to_delete.push(key));
  }
  // console.log('Deletion capacity',result.ConsumedCapacity[0].CapacityUnits);
  if (items_to_delete.length > 0) {
    console.log('Still need to remove',items_to_delete.length,'items from current scan');
    return delete_items(items_to_delete.splice(0,25)).then(handle_delete.bind(null,items_to_delete));
  }
};

const remove_single_set_entries = function(dataset) {
  let params = {
    TableName: data_table,
    FilterExpression: 'dataset = :dataset and NOT acc = :acc',
    ProjectionExpression: 'acc',
    ReturnConsumedCapacity: 'TOTAL',
    ExpressionAttributeValues: {
      ':acc': 'metadata',
      ':dataset': dataset
    }
  };
  if (last_scan_key) {
    params.ExclusiveStartKey = last_scan_key;
  }
  console.log('Removing',dataset);
  let handle_scan = function(data) {
    if (timed_out) {
      timed_out = false;
      console.log("Stopping scan retrieval");
      throw new Error('Timed out');
    }
    let items_to_delete = data.Items.map( item => { return { acc: item.acc, dataset: dataset }; } );
    let new_rate_per_10s = Math.floor(10*MAX_READ_CAPACITY / data.ConsumedCapacity.CapacityUnits);
    if (read_limiter.rate_per_10s !== new_rate_per_10s) {
      console.log("Limiting to",new_rate_per_10s,"calls per 10 seconds");
      read_limiter = rateLimit(new_rate_per_10s,10000);
      read_limiter.rate_per_10s = new_rate_per_10s;
    }
    // console.log('Scan / Read capacity',data.ConsumedCapacity.CapacityUnits);
    // console.log('Items returned after scan',data.Items.length);

    console.log('Need to remove',items_to_delete.length,'items from current scan on',dataset);
    let delete_done = delete_items(items_to_delete.splice(0,25)).then( handle_delete.bind(null,items_to_delete));

    // Push items to be deleted
    if (typeof data.LastEvaluatedKey != 'undefined') {
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      last_scan_key = current_scan_key;
      current_scan_key = data.LastEvaluatedKey;
      return delete_done.then(() => read_limiter()).then(() => dynamo.scan(params).promise()).then(handle_scan);
    } else {
      last_scan_key = null;
      current_scan_key = null;
    }

    return delete_done;
  };

  return read_limiter().then( () => dynamo.scan(params).promise().then(handle_scan));
};

const remove_single_set_metadata = function(dataset) {
  console.log('Removing metadata for ',dataset);
  return delete_items([ { acc: 'metadata', dataset: dataset } ]).then( (result) => {
    if (Object.keys(result.UnprocessedItems).length == 0) {
      console.log('Removed metadata for ',dataset);
    }
  });
};

const remove_single_set = function(dataset) {
  current_dataset = dataset;
  if ( ! dataset ) {
    return;
  }
  return remove_single_set_entries(dataset).then( remove_single_set_metadata.bind(null,dataset) );
};

//get_datasets_to_remove().then( sets => { sets = ['glycodomain_glycodomain_10090'].concat(sets); console.log(sets); return remove_single_set(sets[0]);  }).catch(err => console.error(err));

const loop_promise = function(func,items) {
  if (items.length == 0) {
    return Promise.resolve();
  }
  console.log("Loop promise, running func on first item from",items);
  return func(items.shift()).then(loop_promise.bind(null,func,items));
};

const remove_sets_with_timeout = function(last_status) {
  if (last_status && last_status.dataset) {
    last_scan_key = last_status.last_scan_key;
    current_dataset = last_status.dataset;
  }
  let deleter_promise = null;

  let timeout_promise = new TimeoutPromise( execution_timeout, (resolve,reject) => {
    deleter_promise = get_datasets_to_remove().then( sets => {
      if (current_dataset) {
        sets = [current_dataset].concat(sets);
      }
      return loop_promise(remove_single_set,sets.filter(onlyUnique));
    });
    deleter_promise.then(resolve).catch(reject);
  });
  return timeout_promise.catch( err => {
    console.log("Timed out with error",err);
    timed_out = true;
    let current_status = {
      dataset: current_dataset,
      last_scan_key: last_scan_key,
      messageCount: 1,
      status: 'RUNNING'
    };
    console.log("Waiting to wind up execution");
    return deleter_promise.catch( err => console.log(err) ).then(() => current_status);
  }).then( (status) => {
    console.log("Execution finished");
    timed_out = false;
    return status ? status : { status: 'OK', messageCount: 0 };
  });
};

const datasetCleanup = function(event,context) {
  remove_sets_with_timeout(event).then( message => {
    console.log("Suceeding with",message);
    context.succeed(message);
  })
  .catch( err => {
    console.log("Failed with",err);
    context.fail({status: err.message});
  });
};

exports.MAX_READ_CAPACITY = MAX_READ_CAPACITY;
exports.setsToRemove = get_datasets_to_remove;
exports.datasetCleanup = datasetCleanup;

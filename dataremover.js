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

const execution_timeout = (4*60 + 30)*1000;

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
  if (! items || items.length == 0) {
    console.log("Exiting delete items because we got no items");
    return Promise.resolve(null);
  }
  let params = { 'RequestItems' : {},
                  ReturnConsumedCapacity: 'TOTAL',
                };
  console.log('Doing delete request with',items.length,'items for dataset ',items.map( item => item.dataset ).filter(onlyUnique));
  params.RequestItems[data_table] = items.map( item => { return { DeleteRequest: { Key: item } } });
  console.log("Waiting to write delete request");
  return write_limiter().then( () => {
    console.log("Executing deletion");
    return dynamo.batchWrite(params).promise();
  });
};



const handle_delete = function(items_to_delete,result) {
  if ( ! result ) {
    console.log("Exiting handle_delete because we got no result");
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
    console.log("Handling scan results");
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
    console.log("Sending delete request");
    let delete_done = delete_items(items_to_delete.splice(0,25)).then( handle_delete.bind(null,items_to_delete));

    // Push items to be deleted
    if (typeof data.LastEvaluatedKey != 'undefined') {
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      last_scan_key = current_scan_key;
      current_scan_key = data.LastEvaluatedKey;
      console.log("Performing a new scan");
      return delete_done.then(() => {
        console.log("Waiting for read limiter");
        return read_limiter().then( () => console.log("Ready to run"));
      }).then(() => {
        console.log("Performing scan");
        return dynamo.scan(params).promise();
      }).then(handle_scan)
      .catch(err => {
        console.log("Error during scan",err);
        throw err;
      });
    } else {
      last_scan_key = null;
      current_scan_key = null;
    }

    return delete_done;
  };
  console.log("Kicking off scans");
  return read_limiter().then( () => dynamo.scan(params).promise() ).then(handle_scan);
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
    return new TimeoutPromise( 5000, (resolve,reject) => {
      deleter_promise.catch( err => console.log(err) ).then(resolve);
    }).catch( err => console.log("Cleanup err",err) ).then(() => current_status);
  }).then( (status) => {
    console.log("Execution finished");
    timed_out = false;
    return status ? status : { status: 'OK', messageCount: 0 };
  });
};

const datasetCleanup = function(event,context) {

// const sets_to_cleanup = [
// 'glycoproteome_081cho_t10ko_CHOGSbto0000457_WTC1galt1c1KOC1galt1c1Galnt10',
// 'glycoproteome_081cho_t11ko_CHOGSbto0000457_WTC1galt1c1KOC1galt1c1Galnt11',
// 'glycoproteome_081cho_t1ko_CHOGSbto0000457_WTC1galt1c1KOC1galt1c1Galnt1',
// 'glycoproteome_081cho_t1kot2ko_CHOGSbto0000457_WTC1galt1c1KOC1galt1c1Galnt1Galnt2',
// 'glycoproteome_081cho_t2ko_CHOGSbto0000457_WTC1galt1c1KOC1galt1c1Galnt2',
// 'glycoproteome_081cho_t3ko_CHOGSbto0000457_WTC1galt1c1KOC1galt1c1Galnt3',
// 'glycoproteome_081cho_t7ko_CHOGSbto0000457_WTC1galt1c1KOC1galt1c1Galnt7',
// 'glycoproteome_081cho_t7kot10ko_CHOGSbto0000457_WTC1galt1c1KOC1galt1c1Galnt7Galnt10',
// 'glycoproteome_081drosophila_7227bto0001489',
// 'glycoproteome_081drosophila_ogako_7227bto0001489_KOOga',
// 'glycoproteome_081hacat_sc_HaCaTbto0000552_KOC1GALT1C1',
// 'glycoproteome_081hacat_t1ko_HaCaTbto0000552_WTC1GALT1C1KOC1GALT1C1GALNT1',
// 'glycoproteome_081hacat_t2ko_HaCaTbto0000552_WTC1GALT1C1KOC1GALT1C1GALNT2',
// 'glycoproteome_081hacat_t3ko_HaCaTbto0000552_WTC1GALT1C1KOC1GALT1C1GALNT3',
// 'glycoproteome_081hek293_pomt1kopomt2ko_HEK293bto0000007_KOC1GALT1C1POMT1POMT2POMGNT1',
// 'glycoproteome_081hek293_stt3bko_HEK293bto0000007_KOSTT3B',
// 'glycoproteome_081hek293_t10ko_HEK293bto0000007_WTC1GALT1C1KOC1GALT1C1GALNT10',
// 'glycoproteome_081hek293_t11ko_HEK293bto0000007_WTC1GALT1C1KOC1GALT1C1GALNT11',
// 'glycoproteome_081hek293_t1ko_HEK293bto0000007_WTC1GALT1C1KOC1GALT1C1GALNT1',
// 'glycoproteome_081hek293_t2ko_HEK293bto0000007_WTC1GALT1C1KOC1GALT1C1GALNT2',
// 'glycoproteome_081hek293_t3ko_HEK293bto0000007_WTC1GALT1C1KOC1GALT1C1GALNT3',
// 'glycoproteome_081hek293_t7ko_HEK293bto0000007_WTC1GALT1C1KOC1GALT1C1GALNT7',
// 'glycoproteome_081hek293_t7kot10ko_HEK293bto0000007_WTC1GALT1C1KOC1GALT1C1GALNT7GALNT10',
// 'glycoproteome_081hela_erk8_HeLabto0000567_KOMAPK15',
// 'glycoproteome_081hela_erk8_HeLabto0000567_WTC1GALT1C1KOMAPK15',
// 'glycoproteome_081hepg2_t2partial_HepG2bto0000599_KOGALNT2',
// 'glycoproteome_081human_prostate_9606bto0001129_KOC1GALT1C1GALNT1OtherCancer',
// 'glycoproteome_081huvec_HUVECCbto0001949',
// 'glycoproteome_081ls174_t3ko_LS174Tbto0001553_WTC1GALT1C1KOC1GALT1C1GALNT3',
// 'glycoproteome_081ls174_t3kot6ko_LS174Tbto0001553_WTC1GALT1C1KOC1GALT1C1GALNT3GALNT6',
// 'glycoproteome_081ls174_t3kovst6ko_LS174Tbto0001553_WTC1GALT1C1GALNT3KOC1GALT1C1GALNT6',
// 'glycoproteome_081ls174_t6ko_LS174Tbto0001553_WTC1GALT1C1KOC1GALT1C1GALNT6',
// 'glycoproteome_081pig_braindec2015_9823bto0000142',
// 'glycoproteome_081pig_braindec2015_9823bto0000232',
// 'glycoproteome_081pig_heart_9823bto0000862',
// 'glycoproteome_081pig_heart_9823bto0000903',
// 'glycoproteome_081pig_pancreas_9823bto0000988',
// 'glycoproteome_081plasma_9606bto0000131',
// 'glycoproteome_081platelet_9606bto0000132',
// 'glycoproteome_081sc_CHOGSbto0000457_KOC1galt1c1',
// 'glycoproteome_081t1_HepG2bto0000599_KOC1GALT1C1GALNT1',
// 'glycoproteome_081t2_HepG2bto0000599_KOC1GALT1C1GALNT2',
// 'glycoproteome_081t3ki_HepG2bto0000599_KIGALNT3KOC1GALT1C1',
// 'glycoproteome_081viral_msf_9606pustule_OtherVZVinfection',
// 'glycoproteome_081viral_msf_HELFbto0003034_OtherHCMVTowneinfection',
// 'glycoproteome_081viral_msf_HELFbto0003034_OtherVZVinfection',
// 'glycoproteome_081viral_msf_HaCaTbto0000552_OtherHSV2infection',
// 'glycoproteome_081viral_msf_P3HR1bto0000164_OtherEBVinfection',
// 'glycoproteome_081wt_CHOGSbto0000457',
// 'glycoproteome_10090Muscle_Phospho',
// 'glycoproteome_10090Muscle_WGA',
// 'glycoproteome_7227',
// 'glycoproteome_7227_KOOga',
// 'glycoproteome_9606Plasma',
// 'glycoproteome_9606Platelet',
// 'glycoproteome_9606whole',
// 'glycoproteome_9606whole_OtherVZVinfection',
// 'glycoproteome_9823Brain',
// 'glycoproteome_9823Heartatrial',
// 'glycoproteome_9823Heartventricle',
// 'glycoproteome_9823Pancreas',
// 'glycoproteome_CHO_GS',
// 'glycoproteome_CHO_GSKOC1galt1c1',
// 'glycoproteome_CHO_GSKOC1galt1c1Galnt1',
// 'glycoproteome_CHO_GSKOC1galt1c1Galnt10',
// 'glycoproteome_CHO_GSKOC1galt1c1Galnt11',
// 'glycoproteome_CHO_GSKOC1galt1c1Galnt1Galnt2',
// 'glycoproteome_CHO_GSKOC1galt1c1Galnt2',
// 'glycoproteome_CHO_GSKOC1galt1c1Galnt3',
// 'glycoproteome_CHO_GSKOC1galt1c1Galnt7',
// 'glycoproteome_CHO_GSKOC1galt1c1Galnt7Galnt10',
// 'glycoproteome_HEK293_KOC1GALT1C1GALNT1',
// 'glycoproteome_HEK293_KOC1GALT1C1GALNT10',
// 'glycoproteome_HEK293_KOC1GALT1C1GALNT11',
// 'glycoproteome_HEK293_KOC1GALT1C1GALNT2',
// 'glycoproteome_HEK293_KOC1GALT1C1GALNT3',
// 'glycoproteome_HEK293_KOC1GALT1C1GALNT7',
// 'glycoproteome_HEK293_KOC1GALT1C1GALNT7GALNT10',
// 'glycoproteome_HEK293_KOC1GALT1C1POMT1POMT2POMGNT1'
// ];


  console.log("Dataset cleanup disabled until it is running faster");
  context.succeed({status: 'OK', messageCount: 0 });

  return;

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

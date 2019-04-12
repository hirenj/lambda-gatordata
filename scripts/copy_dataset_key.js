const fs = require('fs');
let config = {};
let data_table = 'data';

try {
    config = JSON.parse(fs.readFileSync('./resources.conf.json'));
    data_table = config.tables.data;
} catch (e) {
  console.log(e);
}

const AWS = require('lambda-helpers').AWS;

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
}

const onlyUnique = function(value, index, self) {
    return self.indexOf(value) === index;
};

const dynamo = new AWS.DynamoDB.DocumentClient();

let last_scan_key = null;
let current_scan_key = null;

const rateLimit = require('rate-limit-promise');
let read_limiter = rateLimit(3,1000);

const MAX_READ_CAPACITY = 100;

const scan_whole_table = function(callback) {
  let params = {
    TableName: data_table,
    FilterExpression: 'NOT acc = :acc',
    ProjectionExpression: 'acc,dataset',
    ReturnConsumedCapacity: 'TOTAL',
    ExpressionAttributeValues: {
      ':acc': 'metadata'
    }
  };
  console.log(params);
  if (last_scan_key) {
    params.ExclusiveStartKey = last_scan_key;
  }

  let handle_scan = function(data) {
    console.log("Handling scan results");

    let new_rate_per_10s = Math.floor(10*MAX_READ_CAPACITY / data.ConsumedCapacity.CapacityUnits);
    if (read_limiter.rate_per_10s !== new_rate_per_10s) {
      console.log("Limiting to",new_rate_per_10s,"calls per 10 seconds");
      read_limiter = rateLimit(new_rate_per_10s,10000);
      read_limiter.rate_per_10s = new_rate_per_10s;
    }

    let handler_done = callback(data.Items);

    // Push items to be deleted
    if (typeof data.LastEvaluatedKey != 'undefined') {
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      last_scan_key = current_scan_key;
      current_scan_key = data.LastEvaluatedKey;
      console.log("Performing a new scan");
      return handler_done.then(() => {
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

    return handler_done;
  };

  console.log("Kicking off scans");
  return read_limiter().then( () => dynamo.scan(params).promise() ).then(handle_scan);
};

let updater_callback = (items) => {
  let promises = [];
  for (let item of items) {
    let params = {
      TableName: data_table,
      Key: { acc: item.acc, dataset: item.dataset },
      UpdateExpression: 'set #setname = :dataset',
      ExpressionAttributeNames: {'#setname' : 'datasetid'},
      ExpressionAttributeValues: {
        ':dataset' : item.dataset
      }
    }

    promises.push(dynamo.update(params).promise().then( res => console.log('Updated ',item.acc,item.dataset,res)));
  }
  return Promise.all(promises);
};

scan_whole_table(updater_callback).catch( err => console.log(err) );

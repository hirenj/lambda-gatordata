const AWS = require('lambda-helpers').AWS;
const dynamo = new AWS.DynamoDB.DocumentClient();
const zlib = require('zlib');
const fs = require('fs');

var bucket_name = 'test-gator';
var data_table = 'data';

let config = {};

try {
    config = require('./resources.conf.json');
    bucket_name = config.buckets.dataBucket;
    data_table = config.tables.data;
} catch (e) {
}

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
}

let acc = 'reactions9606';
let setid = 'reactions_latest';
let data = JSON.parse(fs.readFileSync('ccg')).data.reactions9606;

let block = {
  'acc' : acc,
  'dataset' : setid
};

block.data = zlib.deflateSync(new Buffer(JSON.stringify(data),'utf8')).toString('binary');

let req = {
    'PutRequest' : {
      'Item' : block
    }
  };


var params = { 'RequestItems' : {} };

params.RequestItems[data_table] = [];

params.RequestItems[data_table].push( req );

console.log(params);

dynamo.batchWrite(params, (err,res) => console.log(err,res));
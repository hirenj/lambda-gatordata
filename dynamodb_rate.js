'use strict';
/*jshint esversion: 6, node:true */

const AWS = require('lambda-helpers').AWS;
const dynamo = new AWS.DynamoDB.DocumentClient();
const zlib = require('zlib');

const EventEmitter = require('events').EventEmitter;

const queue_size = function queue_size(queue) {
  return JSON.stringify(queue).length
};

const interval_uploader = function(uploader,data_table,queue) {
  var params = { 'RequestItems' : {} };
  console.log("Upload queue length is ",queue.length);
  params.RequestItems[data_table] = [];
  while (queue.length > 0 && params.RequestItems[data_table].length < 25  && queue_size(params.RequestItems[data_table].concat(queue[0])) < 15000) {
    let next_item = queue.shift();
    while (next_item && JSON.stringify(next_item).length > 12000) {
      console.log("Size of data too big for ",next_item.PutRequest.Item.acc," skipping ",JSON.stringify(next_item).length);
      next_item = queue.shift();
    }
    if (next_item) {
      params.RequestItems[data_table].push( next_item );
    }
  }
  if (params.RequestItems[data_table].length > 0 ) {
    console.log("Uploading "+queue_size(params.RequestItems[data_table]),"worth of data");
    var write_request = dynamo.batchWrite(params);
    write_request.on('retry',function(resp) {
      resp.error.retryable = false;
      params.RequestItems[data_table].forEach((item) => queue.push(item));
    });
    write_request.promise().catch(function(err) {
      console.log("BatchWriteErr",err);
    }).then(function(result) {
      if ( ! result.UnprocessedItems ) {
        return;
      }
      console.log("Adding ",result.UnprocessedItems[data_table].length,"items back onto queue");
      result.UnprocessedItems[data_table]
        .map((dat) => ({'PutRequest': { 'Item' : dat.PutRequest.Item } }) )
        .forEach((item) => queue.push(item));
    });
  } else {
    if (uploader.queue_ready && queue.length == 0) {      
      uploader.finished.resolve();
    }
  }
  if (uploader.running) {
    setTimeout(interval_uploader.bind(null,uploader,data_table,queue),1000);
  }
};

class TriggeredPromise extends EventEmitter {
  constructor() {
    super()
  }
  get promise() {
    let self = this;
    this._promise = this._promise || new Promise(function(resolve) {
      self.on('resolved',function() {
        resolve();
      });
    });
    return this._promise;
  }
  resolve() {
    this.emit('resolved');
  }
};

class Uploader {
  constructor(data_table) {
    this.data_table = data_table;
    this.queue = [];
  }
  start() {
    AWS.events.on('retry', function(resp) {
      if (resp.error) resp.error.retryDelay = 1000;
      resp.error.retryable = false;
    });
    this.queue.length = 0;
    this.running = true;
    this.finished = new TriggeredPromise();
    this.finished.on('resolved',() => this.running = false );
    interval_uploader(this,this.data_table,this.queue);
  }
  setQueueReady() {
    this.queue_ready = true;
  }
}

exports.createUploader = function(table) { return new Uploader(table); };

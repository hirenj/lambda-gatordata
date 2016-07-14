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
  while (queue.length > 0 && params.RequestItems[data_table].length < 25  && queue_size(params.RequestItems[data_table].concat(queue[0])) < uploader.maxTheoreticalCapacity()) {
    let next_item = queue.shift();
    while (next_item && JSON.stringify(next_item).length > Math.floor(0.8*uploader.maxTheoreticalCapacity()) ) {
      console.log("Size of data too big for ",next_item.PutRequest.Item.acc," skipping ",JSON.stringify(next_item).length);
      next_item = queue.shift();
    }
    if (next_item) {
      params.RequestItems[data_table].push( next_item );
    }
  }
  let last_batch_size = queue_size(params.RequestItems[data_table]);
  if (params.RequestItems[data_table].length > 0 ) {
    console.log("Uploading",last_batch_size,"worth of data for",params.RequestItems[data_table].length,"entries");
    var write_request = dynamo.batchWrite(params);
    write_request.on('retry',function(resp) {
      resp.error.retryable = false;
      params.RequestItems[data_table].reverse().forEach((item) => queue.unshift(item));
    });
    console.log("Setting last write promise");
    let write_promise = write_request.promise().catch(function(err) {
      console.log("BatchWriteErr",err);
    }).then(function(result) {
      console.log(Object.keys(result.UnprocessedItems));
      if ( ! result.UnprocessedItems || ! result.UnprocessedItems[data_table] ) {
        return;
      }
      console.log("Adding ",result.UnprocessedItems[data_table].length,"items back onto queue");
      result.UnprocessedItems[data_table]
        .map((dat) => ({'PutRequest': { 'Item' : dat.PutRequest.Item } }) )
        .reverse()
        .forEach((item) => queue.unshift(item));
    }).then(function() {
      console.log("Write finished");      
    }).catch(function(err) {
      console.log(err.stack);
      console.log(err);
    });
    if (uploader.last_write && uploader.last_write.resolved) {
      console.log("Flattening chain");
      delete uploader.last_write;
    }
    let writes_finished = (uploader.last_write || Promise.resolve(true))
                          .then( () => write_promise )
                          .then( () => writes_finished.resolved = true )
    if (uploader.stopped) {
      writes_finished.then( () => uploader.stopped.resolve() );
    }
    uploader.last_write = writes_finished;
  } else {
    if (uploader.queue_ready && queue.length == 0) {
      console.log("Finishing uploader");
      uploader.finished.resolve();
    }
  }
  if (uploader.running) {
    let timeout =  Math.floor(1000 * last_batch_size / uploader.maxTheoreticalCapacity());
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
  stop() {
    this.running = false;
    this.stopped = new TriggeredPromise();
    return this.stopped.promise;
  }
  maxTheoreticalCapacity() {
    return 15*1024;
  }
  setQueueReady() {
    this.queue_ready = true;
  }
}

exports.createUploader = function(table) { return new Uploader(table); };

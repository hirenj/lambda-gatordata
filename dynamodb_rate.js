'use strict';
/*jshint esversion: 6, node:true */

const AWS = require('lambda-helpers').AWS;
const dynamo = new AWS.DynamoDB.DocumentClient();

import { EventEmitter } from 'events';

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
        .map((dat) => {'PutRequest': { 'Item' : dat.PutRequest.Item } })
        .forEach((item) => queue.push(item));
    });
  } else {
    if (uploader.queue_ready && queue.length == 0) {      
      uploader.finished.resolve();
    }
  }
  if (uploader.running) {
    setTimeout(arguments.callee.bind(null,uploader,data_table,queue),1000);    
  }
};

class TriggeredPromise extends EventEmitter {
  constructor() {
    super()
  }
  get promise() {
    this._promise = this._promise || new Promise(function(resolve) {
      this.on('resolved',function() {
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
    this.upload_promises = [];
  }
  start() {
    AWS.events.on('retry', function(resp) {
      if (resp.error) resp.error.retryDelay = 1000;
      resp.error.retryable = false;
    });
    this.finished = new TriggeredPromise();
    this.finished.on('resolved',() => this.running = false );
    interval_uploader(this,this.data_table,this.queue);
  }
  setQueueReady() {
    this.queue_ready = true;
  }
}

var upload_data_record_db = function upload_data_record_db(key,data) {

  uploader.start();

  var key_elements = key ? key.split('/') : [];
  var set_ids = (key_elements[2] || '').split(':');
  var group_id = set_ids[0];
  var set_id = set_ids[1];
  var accession = key_elements[3];
  if (key) {
    if ( accession && set_id && data.data ) {
      data.data.forEach(function(pep) {
        if (pep.spectra) {
          delete pep.spectra;
        }
      });
      var block = {
              'acc' : accession,
              'dataset' : set_id
      };
      block.data = zlib.deflateSync(new Buffer(JSON.stringify(data.data),'utf8')).toString('binary');
      uploader.queue.push({
        'PutRequest' : {
          'Item' : block
        }
      });

    }
  }
  if ( ! key ) {
    uploader.setQueueReady();
    console.log("We have",interval_upload_promises.length,"upload batches");
    return uploader.finished.promise;
  } else {
    return Promise.resolve(true);
  }
};

exports.createUploader = function(table) { return new Uploader(table); };

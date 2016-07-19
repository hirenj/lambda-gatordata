'use strict';
/*jshint esversion: 6, node:true */

const AWS = require('lambda-helpers').AWS;
const dynamo = new AWS.DynamoDB.DocumentClient();
const zlib = require('zlib');

const EventEmitter = require('events').EventEmitter;

const Transform = require('stream').Transform;
const inherits = require('util').inherits;

function JSONGrouper(size,options) {
  if ( ! (this instanceof JSONGrouper))
    return new JSONGrouper(options);

  if (! options) options = {};
  options.objectMode = true;
  this.queue = [];
  this.size = size;
  Transform.call(this, options);
}

inherits(JSONGrouper, Transform);

JSONGrouper.prototype._transform = function _transform(obj, encoding, callback) {

  this.queue.push(obj);

  if (this.queue.length >= this.size) {
    this.push([].concat(this.queue));
    this.queue.length = 0;
  }

  callback();

};

function JSONtoDynamodb(set_id,offset) {
  if ( ! (this instanceof JSONtoDynamodb))
    return new JSONtoDynamodb(options);

  if (! options) options = {};
  options.objectMode = true;
  this.set_id = set_id;
  this.offset = offset;
  Transform.call(this, options);
}

inherits(JSONtoDynamodb, Transform);

JSONtoDynamodb.prototype._transform = function(obj,encoding,callback) {

  if (this.offset && obj.key.toLowerCase() === this.offset) {
    console.log("Using offset to start at ",offset);
    delete this.offset;
  }

  if (this.offset) {
    callback();
    return;
  }

  let data = {'data': obj.value, 'acc' : obj.key.toLowerCase() };

  data.data.forEach(function(dat) {
    if (dat.spectra) {
      delete dat.spectra;
    }
    if (dat.interpro) {
      delete dat.interpro;
    }
  });

  var block = {
    'acc' : data.acc,
    'dataset' : this.set_id
  };

  block.data = zlib.deflateSync(new Buffer(JSON.stringify(data.data),'utf8')).toString('binary');

  this.push({
    'PutRequest' : {
      'Item' : block
    }
  });

};


const queue_size = function queue_size(queue) {
  return JSON.stringify(queue).length
};

const interval_uploader = function(uploader,data_table,queue) {
  var params = { 'RequestItems' : {} };
  console.log("Upload queue length is ",queue.length);

  let result = [];
  if (queue.length == 0) {
    let val = uploader.data.read();
    if (! val && ! uploader.data.readable ) {
      val = [].concat(uploader.data.queue);
      uploader.data.queue.length = 0;
    }
    if (val) {
      val.forEach(function(value) {
        queue.push(value);
      });
    }
  }
  if (val && val.length == 0 && ! uploader.data.readable) {
    clearInterval(interval);
  }

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
  let item_count = params.RequestItems[data_table].length;
  if (params.RequestItems[data_table].length > 0 ) {
    console.log("Uploading",last_batch_size,"worth of data for",params.RequestItems[data_table].length,"entries");
    var write_request = dynamo.batchWrite(params);
    write_request.on('retry',function(resp) {
      resp.error.retryable = false;
      params.RequestItems[data_table].reverse().forEach((item) => queue.unshift(item));
    });
    let write_promise = write_request.promise().catch(function(err) {
      console.log("BatchWriteErr",err);
    }).then(function(result) {
      if ( ! result || ! result.UnprocessedItems || ! result.UnprocessedItems[data_table] ) {
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
    let timeout =  Math.floor(1000 * (item_count*1024*Math.ceil((last_batch_size / item_count) / 1024 )) / uploader.maxTheoreticalCapacity());
    console.log("Working out rate, we think it should be ",timeout);
    setTimeout(interval_uploader.bind(null,uploader,data_table,queue),timeout);
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
  set capacity(capacity) {
    this._theoretical = capacity;
  }
  maxTheoreticalCapacity() {
    return (this._theoretical || 1)*1024;
  }
  setQueueReady() {
    this.queue_ready = true;
  }
}
exports.createUploadPipe = function(table,setid,groupid) {
  let inpipe = (new JSONtoDynamodb(setid,offset)).pipe(new JSONGrouper(25));

  let uploader = new Uploader(table);
  uploader.data = inpipe;
  return uploader;
};
exports.createUploader = function(table) { return new Uploader(table); };

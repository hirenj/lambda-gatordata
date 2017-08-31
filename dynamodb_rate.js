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
    return new JSONGrouper(size,options);

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

function MetadataExtractor(options) {
  if ( ! (this instanceof MetadataExtractor))
    return new MetadataExtractor(options);

  if (! options) options = {};
  options.objectMode = true;
  let self = this;
  let Parser = require('JSONStream/node_modules/jsonparse');
  let p = new Parser();

  this.parser = p;

  p.push = function(){
    if (this.stack && this.stack[1] && this.stack[1].key == 'data') {
      this.value = null;
    }
    this.stack.push({value: this.value, key: this.key, mode: this.mode});
  };

  p.onValue = function(val) {
    if (! val) {
      return;
    }
    if (val.metadata) {
      self.metadata = val.metadata;
      self.push(val.metadata);
    }
  };

  Transform.call(this, options);
}

inherits(MetadataExtractor, Transform);

MetadataExtractor.prototype._transform = function _transform(obj, encoding, callback) {
  this.parser.write(obj);
  callback();
};


function Offsetter(offset,options) {
  if ( ! (this instanceof Offsetter))
    return new Offsetter(offset,options);

  if (! options) options = {};
  options.objectMode = true;
  this.startOffset = offset;
  if ( ! this.startOffset ) {
    this.startOffset = 0;
    this.done = true;
  }
  console.log("Created offsetter, state is ",this.done,this.startOffset);
  Transform.call(this, options);
}

inherits(Offsetter, Transform);

Offsetter.prototype._transform = function _transform(obj, encoding, callback) {
  this.offset = this.startOffset + (this.bytesConsumed || 0) - 2*1024*1024;
  this.bytesConsumed = (this.bytesConsumed || 0) + obj.length;
  if (this.done) {
    this.push(obj);
    callback();
    return;
  }

  let chunk = obj.toString();
  let newline = chunk.indexOf('\n');

  if (newline < 0) {
    callback();
    return;
  }
  this.done = true;
  this.push('{ "data" : {'+chunk.substring(newline));
  callback();
};

function JSONtoDynamodb(set_id,offset,options) {
  if ( ! (this instanceof JSONtoDynamodb))
    return new JSONtoDynamodb(set_id,offset,options);

  if (! options) options = {};
  options.objectMode = true;
  this.set_id = set_id;
  this.offset = offset;
  console.log("Waiting for ID",this.offset,this.set_id,typeof this.offset);
  Transform.call(this, options);
}

inherits(JSONtoDynamodb, Transform);

JSONtoDynamodb.prototype._transform = function(obj,encoding,callback) {
  if (this.offset && obj.key.toLowerCase() === this.offset) {
    console.log("Using offset to start at ",this.offset);
    delete this.offset;
  }

  if (this.offset) {
    callback();
    return;
  }
  let data = {'data': obj.value, 'acc' : obj.key.toLowerCase() };
  if (Array.isArray(data.data)) {
    data.data.forEach(function(dat) {
      if (dat.spectra) {
        delete dat.spectra;
      }
      if (dat.interpro) {
        delete dat.interpro;
      }
    });
  }

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

  callback();

};


const queue_size = function queue_size(queue) {
  return JSON.stringify(queue).length
};

const interval_uploader = function(uploader,data_table,queue) {
  var params = { 'RequestItems' : {} };
  if (queue.length > 0) {
    console.log("Upload queue length is ",queue.length);
  }

  let result = [];
  if (queue.length == 0) {
    uploader.data.resume();
    let val = null;
    while(val = uploader.outdata.read()) {
      if (! val && ! uploader.outdata.readable ) {
        console.log("No value to read and not readable");
        val = [].concat(uploader.outdata.queue);
        uploader.outdata.queue.length = 0;
      }
      if (val) {
        uploader.data.pause();
        uploader.total = (uploader.total || 0 ) + val.length;
        val.forEach(function(value) {
          queue.push(value);
          uploader.last_acc = value.PutRequest.Item.acc;
        });
      }
      if (val && val.length == 0 && ! uploader.data.readable) {
        console.log("Empty value from pipe");
        if (queue.length == 0) {
          console.log("Finishing uploader");
          uploader.finished.resolve();
        }
      }
    }
    if (! queue.length && ! uploader.data.readable) {
      console.log("Finishing uploader outside of while");
      uploader.finished.resolve();
    }
  }

  params.RequestItems[data_table] = [];
  let seen_keys = [];
  while (queue.length > 0 && params.RequestItems[data_table].length < 25  && queue_size(params.RequestItems[data_table].concat(queue[0])) < uploader.maxTheoreticalCapacity()) {
    let next_item = queue.shift();
    while (next_item && seen_keys.indexOf(next_item.PutRequest.Item.acc) >= 0) {
      console.log("Skipping duplicate accession ",next_item.PutRequest.Item.acc);
      next_item = queue.shift();
    }
    while (next_item && JSON.stringify(next_item).length > Math.floor(0.8*uploader.maxTheoreticalCapacity()) ) {
      console.log("Size of data too big for ",next_item.PutRequest.Item.acc," skipping ",JSON.stringify(next_item).length);
      next_item = queue.shift();
    }
    if (next_item) {
      params.RequestItems[data_table].push( next_item );
      seen_keys.push(next_item.PutRequest.Item.acc);
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
      console.log("Wrote ",uploader.total - queue.length," entries so far, queue length",queue.length);
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
  }
  if (uploader.running) {
    let timeout =  Math.floor(1000 * (item_count*1024*Math.ceil((last_batch_size / item_count) / 1024 )) / uploader.maxTheoreticalCapacity());
    if (isNaN(timeout)) {
      timeout = 1000;
    }
    if (uploader.current_timeout !== timeout) {
      console.log("Working out rate, we think it should be ",timeout);
    }
    uploader.current_timeout = timeout;
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
    console.log("Queue length at stop is ",this.queue.length);
    console.log("Bytes consumed at stop is ",this.byte_offset.bytesConsumed);
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

exports.MetadataExtractor = MetadataExtractor;
exports.Offsetter = Offsetter;

exports.createUploadPipe = function(table,setid,groupid,offset) {
  let inpipe = new JSONtoDynamodb(setid,offset);
  let uploader = new Uploader(table);
  uploader.data = inpipe;
  uploader.outdata = inpipe.pipe(new JSONGrouper(25));
  return uploader;
};
exports.createUploader = function(table) { return new Uploader(table); };

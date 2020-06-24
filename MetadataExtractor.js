const Transform = require('stream').Transform;
const inherits = require('util').inherits;
const Parser = require('jsonparse');


function MetadataExtractor(options) {
  if ( ! (this instanceof MetadataExtractor))
    return new MetadataExtractor(options);

  if (! options) options = {};
  options.objectMode = true;
  let self = this;
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

exports.MetadataExtractor = MetadataExtractor;
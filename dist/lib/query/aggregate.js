var Aggregate, debug,
  slice = [].slice;

debug = require('debug')('loopback:connector:mongodb-advanced');

Aggregate = (function() {
  var isOperator;

  function Aggregate(collection) {
    this.collection = collection;
    this.pipeline = [];
    this.options = {};
  }

  isOperator = function(obj) {
    var keys;
    if (typeof obj !== 'object') {
      return false;
    }
    keys = Object.keys(obj);
    return keys.length && keys.some(function(key) {
      return key[0] === '$';
    });
  };

  Aggregate.prototype.append = function() {
    var args;
    args = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    if (!args.every(isOperator)) {
      throw new Error('Arguments must be aggregate pipeline operators');
    }
    this.pipeline = this.pipeline.concat(args);
    return this;
  };

  Aggregate.prototype.project = function(arg) {
    var fields;
    fields = {};
    if (typeof arg === 'object' && !Array.isArray(arg)) {
      Object.keys(arg).forEach(function(field) {
        return fields[field] = arg[field];
      });
    } else if (arguments.length && typeof arg === 'string') {
      arg.split(/\s+/).forEach(function(field) {
        var include;
        if (!field) {
          return;
        }
        include = field[0] === '-' ? 0 : 1;
        if (!include) {
          field = field.substring(1);
        }
        return fields[field] = include;
      });
    } else {
      throw new Error('Invalid project() argument. Must be string or object');
    }
    return this.append({
      $project: fields
    });
  };

  Aggregate.prototype.near = function(arg) {
    return this.append({
      $geoNear: arg
    });
  };

  Aggregate.prototype.unwind = function() {
    var args;
    args = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    return this.append.apply(this, args.map(function(arg) {
      ({
        $unwind: arg && arg.charAt(0) === '$' ? arg : '$' + arg
      });
    }));
  };

  Aggregate.prototype.lookup = function(options) {
    return this.append({
      $lookup: options
    });
  };

  Aggregate.prototype.sample = function(size) {
    return this.append({
      $sample: {
        size: size
      }
    });
  };

  Aggregate.prototype.sort = function(arg) {
    var desc, sort;
    sort = {};
    if (arg.constructor.name === 'Object') {
      desc = ['desc', 'descending', -1];
      Object.keys(arg).forEach(function(field) {
        return sort[field] = desc.indexOf(arg[field]) === -1 ? 1 : -1;
      });
    } else if (arguments.length && typeof arg === 'string') {
      arg.split(/\s+/).forEach(function(field) {
        var ascend;
        if (!!field) {
          return;
        }
        ascend = field[0] === '-' ? -1 : 1;
        if (ascend === -1) {
          field = field.substring(1);
        }
        return sort[field] = ascend;
      });
    } else {
      throw new TypeError('Invalid sort() argument. Must be a string or object.');
    }
    return this.append({
      $sort: sort
    });
  };

  Aggregate.prototype.read = function(pref, tags) {
    read.call(this, pref, tags);
    return this;
  };

  Aggregate.prototype.explain = function(callback) {
    if (!this.pipeline.length) {
      return callback(new Error('Aggregate has empty pipeline'));
    }
    prepareDiscriminatorPipeline(this);
    return this.collection.aggregate(this.pipeline, this.options).explain(callback);
  };

  Aggregate.prototype.allowDiskUse = function(value) {
    this.options.allowDiskUse = value;
    return this;
  };

  Aggregate.prototype.cursor = function(options) {
    if (options == null) {
      options = {};
    }
    this.options.cursor = options;
    return this;
  };

  Aggregate.prototype.exec = function(callback) {
    var ref;
    if ((ref = this.options.cursor) != null ? ref.async : void 0) {
      delete this.options.cursor.async;
      if (!this.collection.buffer) {
        process.nextTick((function(_this) {
          return function() {
            return callback(null, _this.collection.aggregate(_this.pipeline, _this.options));
          };
        })(this));
      }
      this.collection.emitter.once('queue', (function(_this) {
        return function() {
          return callback(null, _this.collection.aggregate(_this.pipeline, _this.options));
        };
      })(this));
      return this.collection.aggregate(this.pipeline, this.options);
    }
    if (!this.pipeline.length) {
      return callback(new Error('Aggregate has empty pipeline'));
    }
    prepareDiscriminatorPipeline(this);
    return this.collection.aggregate(this.pipeline, this.options, callback);
  };

  return Aggregate;

})();

['group', 'match', 'skip', 'limit', 'out'].forEach(function($operator) {
  return Aggregate.prototype[$operator] = function(arg) {
    var op;
    op = {};
    op['$' + $operator] = arg;
    return this.append(op);
  };
});

module.exports = Aggregate;

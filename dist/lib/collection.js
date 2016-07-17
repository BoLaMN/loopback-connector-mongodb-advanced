var Bulk, Collection, Cursor, ObjectID, clone, debug, extend, isBoolean, isFunction, noop, ref, writeOpts;

debug = require('debug')('loopback:connector:mongodb-advanced');

Cursor = require('./cursor');

Bulk = require('./bulk');

ObjectID = require('mongodb').ObjectID;

ref = require('lodash'), clone = ref.clone, extend = ref.extend, isFunction = ref.isFunction, isBoolean = ref.isBoolean;

writeOpts = {
  writeConcern: {
    w: 1
  },
  ordered: true
};

noop = function() {};

Collection = (function() {
  function Collection(collection) {
    this.collection = collection;
  }

  Collection.prototype.find = function(query, projection, opts, callback) {
    var cursor;
    if (isFunction(query)) {
      return this.find({}, null, null, query);
    }
    if (isFunction(projection)) {
      return this.find(query, null, null, projection);
    }
    if (isFunction(opts)) {
      return this.find(query, projection, null, opts);
    }
    cursor = new Cursor(this.collection.find(query, projection, opts));
    if (callback) {
      return cursor.toArray().asCallback(callback);
    }
    return cursor;
  };

  Collection.prototype.findOne = function(query, projection, callback) {
    if (isFunction(query)) {
      return this.findOne({}, null, query);
    }
    if (isFunction(projection)) {
      return this.findOne(query, null, projection);
    }
    return this.find(query, projection).next().asCallback(callback);
  };

  Collection.prototype.findAndModify = function(query, update, sort, opts, callback) {
    var params;
    if (!opts && !callback) {
      return this.findAndModify(query, update, [], {}, noop);
    }
    if (isFunction(sort)) {
      return this.findAndModify(query, update, [], {}, opts);
    }
    if (isFunction(opts)) {
      return this.findAndModify(query, update, sort, {}, opts);
    }
    params = {
      query: query,
      update: update,
      sort: sort
    };
    return this.execute('findAndModify', params, extend(writeOpts, opts)).then(function(results) {
      return [
        results.value, results.lastErrorObject || {
          n: 0
        }
      ];
    }).asCallback(callback, {
      spread: true
    });
  };

  Collection.prototype.findOneAndUpdate = function(query, data, opts, callback) {
    if (!opts && !callback) {
      return this.findOneAndUpdate(query, data, {}, noop);
    }
    if (isFunction(opts)) {
      return this.findOneAndUpdate(query, data, {}, opts);
    }
    return this.execute('findOneAndUpdate', query, data, opts).then(function(results) {
      return [
        result.value, result.lastErrorObject || {
          n: 0
        }
      ];
    }).asCallback(callback, {
      spread: true
    });
  };

  Collection.prototype.count = function(query, callback) {
    if (isFunction(query)) {
      return this.count({}, query);
    }
    return this.find(query).count().asCallback(callback);
  };

  Collection.prototype.distinct = function(field, query, callback) {
    var params;
    params = {
      key: field,
      query: query
    };
    return this.execute('distinct', params).then(function(results) {
      return results.values;
    }).asCallback(callback);
  };

  Collection.prototype.insert = function(docs, opts, callback) {
    if (!opts && !callback) {
      return this.insert(docs, {}, noop);
    }
    if (isFunction(opts)) {
      return this.insert(docs, {}, opts);
    }
    if (opts && !callback) {
      return this.insert(docs, opts, noop);
    }
    return this.collection.insert(docs, extend(writeOpts, opts)).asCallback(callback);
  };

  Collection.prototype.update = function(query, update, opts, callback) {
    if (!opts && !callback) {
      return this.update(query, update, {}, noop);
    }
    if (isFunction(opts)) {
      return this.update(query, update, {}, opts);
    }
    return this.collection.update(query, update, extend(writeOpts, opts)).then(function(results) {
      return results.result;
    }).asCallback(callback);
  };

  Collection.prototype.save = function(doc, opts, callback) {
    if (!opts && !callback) {
      return this.save(doc, {}, noop);
    }
    if (isFunction(opts)) {
      return this.save(doc, {}, opts);
    }
    if (!callback) {
      return this.save(doc, opts, noop);
    }
    if (doc._id) {
      return this.update({
        _id: doc._id
      }, doc, extend({
        upsert: true
      }, opts)).asCallback(callback);
    } else {
      return this.insert(doc, opts).asCallback(callback);
    }
  };

  Collection.prototype.remove = function(query, opts, callback) {
    var deleteOperation;
    if (isFunction(query)) {
      return this.remove({}, {
        justOne: false
      }, query);
    }
    if (isFunction(opts)) {
      return this.remove(query, {
        justOne: false
      }, opts);
    }
    if (isBoolean(opts)) {
      return this.remove(query, {
        justOne: opts
      }, callback);
    }
    if (!opts) {
      return this.remove(query, {
        justOne: false
      }, callback);
    }
    if (!callback) {
      return this.remove(query, opts, noop);
    }
    deleteOperation = opts.justOne ? 'deleteOne' : 'deleteMany';
    return this.collection[deleteOperation](query, extend(opts, writeOpts)).then(function(results) {
      return results.result;
    }).asCallback(callback);
  };

  Collection.prototype.rename = function(name, opts, callback) {
    if (isFunction(opts)) {
      return this.rename(name, {}, opts);
    }
    if (!opts) {
      return this.rename(name, {}, noop);
    }
    if (!callback) {
      return this.rename(name, noop);
    }
    return this.collection.rename(name, opts).asCallback(callback);
  };

  Collection.prototype.drop = function(callback) {
    return this.execute('drop').asCallback(callback);
  };

  Collection.prototype.stats = function(callback) {
    return this.execute('collStats').asCallback(callback);
  };

  Collection.prototype.mapReduce = function(map, reduce, opts, callback) {
    if (isFunction(opts)) {
      return this.mapReduce(map, reduce, {}, opts);
    }
    if (!callback) {
      return this.mapReduce(map, reduce, opts, noop);
    }
    return this.collection.mapReduce(map, reduce, opts).asCallback(callback);
  };

  Collection.prototype.execute = function(cmd, opts, callback) {
    var obj;
    if (isFunction(opts)) {
      return this.execute(cmd, null, opts);
    }
    opts = opts || {};
    obj = {};
    obj[cmd] = this.collection.s.name;
    Object.keys(opts).forEach(function(key) {
      obj[key] = opts[key];
    });
    return this.collection.s.db.command(obj).asCallback(callback);
  };

  Collection.prototype.toString = function() {
    return this.collection.s.name;
  };

  Collection.prototype.dropIndexes = function(callback) {
    return this.execute('dropIndexes', {
      index: '*'
    }).asCallback(callback);
  };

  Collection.prototype.dropIndex = function(index, callback) {
    return this.execute('dropIndexes', {
      index: index
    }).asCallback(callback);
  };

  Collection.prototype.createIndex = function(index, opts, callback) {
    if (isFunction(opts)) {
      return this.createIndex(index, {}, opts);
    }
    if (!opts) {
      return this.createIndex(index, {}, noop);
    }
    if (!callback) {
      return this.createIndex(index, opts, noop);
    }
    return this.collection.createIndex(index, opts).asCallback(callback);
  };

  Collection.prototype.ensureIndex = function(index, opts, callback) {
    if (isFunction(opts)) {
      return this.ensureIndex(index, {}, opts);
    }
    if (!opts) {
      return this.ensureIndex(index, {}, noop);
    }
    if (!callback) {
      return this.ensureIndex(index, opts, noop);
    }
    return this.collection.ensureIndex(index, opts).asCallback(callback);
  };

  Collection.prototype.getIndexes = function(callback) {
    return this.collection.indexes().asCallback(callback);
  };

  Collection.prototype.reIndex = function(callback) {
    return this.execute('reIndex').asCallback(callback);
  };

  Collection.prototype.isCapped = function(callback) {
    return this.collection.isCapped().asCallback(callback);
  };

  Collection.prototype.group = function(doc, callback) {
    var key;
    key = doc.key || doc.keyf;
    return this.collection.group(key, doc.cond, doc.initial, doc.reduce, doc.finalize).asCallback(callback);
  };

  Collection.prototype.aggregate = function(pipeline, opts, callback) {
    var strm;
    strm = new Cursor(this.collection.aggregate(pipeline, opts));
    if (callback) {
      return strm.toArray().asCallback(callback);
    }
    return strm;
  };

  Collection.prototype.bulk = function(ordered) {
    if (ordered == null) {
      ordered = true;
    }
    return new Bulk(this.collection, ordered);
  };

  return Collection;

})();

module.exports = Collection;

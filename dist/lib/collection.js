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

  Collection.prototype.find = function(query, projection, opts, mapFn, callback) {
    var cursor;
    if (isFunction(query)) {
      return this.find({}, null, null, null, query);
    }
    if (isFunction(projection)) {
      return this.find(query, null, null, null, projection);
    }
    if (isFunction(opts)) {
      return this.find(query, projection, null, null, opts);
    }
    console.log('args length', arguments.length, mapFn);
    if (arguments.length === 4) {
      return this.find(query, projection, opts, null, callback);
    }
    cursor = new Cursor(this.collection.find(query, projection, opts));
    if (callback) {
      if (mapFn) {
        return cursor.map(mapFn).asCallback(callback);
      } else {
        return cursor.toArray().asCallback(callback);
      }
    }
    return cursor;
  };

  Collection.prototype.findOne = function(query, projection, mapFn, callback) {
    if (isFunction(query)) {
      return this.findOne({}, null, query);
    }
    if (isFunction(projection)) {
      return this.findOne(query, null, projection);
    }
    if (isFunction(mapFn && !isFunction(callback))) {
      return this.findOne(query, projection, null, callback);
    }
    return this.find(query, projection, mapFn).next().asCallback(callback);
  };

  Collection.prototype.findAndModify = function(opts, callback) {
    var done;
    done = function(err, result) {
      if (err) {
        return callback(err);
      }
      return callback(null, result.value, result.lastErrorObject || {
        n: 0
      });
    };
    return this.execute('findAndModify', opts).asCallback(done);
  };

  Collection.prototype.count = function(query, callback) {
    if (isFunction(query)) {
      return this.count({}, query);
    }
    return this.find(query).count().asCallback(callback);
  };

  Collection.prototype.distinct = function(field, query, callback) {
    var done, params;
    params = {
      key: field,
      query: query
    };
    done = function(err, result) {
      if (err) {
        return callback(err);
      }
      return callback(null, result.values);
    };
    return this.execute('distinct', params).asCallback(done);
  };

  Collection.prototype.insert = function(docOrDocs, opts, callback) {
    var docs, i, id, instances;
    if (!opts && !callback) {
      return this.insert(docOrDocs, {}, noop);
    }
    if (isFunction(opts)) {
      return this.insert(docOrDocs, {}, opts);
    }
    if (opts && !callback) {
      return this.insert(docOrDocs, opts, noop);
    }
    docs = Array.isArray(docOrDocs) ? docOrDocs : [docOrDocs];
    instances = clone(docs);
    i = 0;
    while (i < docs.length) {
      if (!docs[i]._id) {
        id = ObjectID.createPk();
        docs[i]._id = id;
      }
      instances[i].id = docs[i]._id.toString();
      delete instances[i]._id;
      i++;
    }
    return this.collection.insert(docs, extend(writeOpts, opts)).then(function(results) {
      return instances;
    }).asCallback(callback);
  };

  Collection.prototype.update = function(query, update, opts, callback) {
    var done;
    if (!opts && !callback) {
      return this.update(query, update, {}, noop);
    }
    if (isFunction(opts)) {
      return this.update(query, update, {}, opts);
    }
    done = function(err, result) {
      if (err) {
        return callback(err);
      }
      return callback(null, result.result);
    };
    return this.collection.update(query, update, extend(writeOpts, opts)).asCallback(done);
  };

  Collection.prototype.save = function(doc, opts, callback) {
    if (opts == null) {
      opts = {};
    }
    if (isFunction(opts)) {
      return this.save(doc, {}, opts);
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
    return this.collection[deleteOperation](query, extend(opts, writeOpts)).then(function(result) {
      return result.result;
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

  Collection.prototype.aggregate = function(pipeline, opts, mapFn, callback) {
    var cursor;
    if (isFunction(mapFn && !isFunction(callback))) {
      return this.aggregate(pipeline, opts, null, callback);
    }
    cursor = new Cursor(this.collection.aggregate(pipeline, opts));
    if (callback) {
      if (mapFn) {
        return cursor.map(mapFn).asCallback(callback);
      } else {
        return cursor.toArray().asCallback(callback);
      }
    }
    return cursor;
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

var Bulk, Collection, Cursor, ObjectID, extend, isBoolean, isFunction, noop, ref, writeOpts;

Cursor = require('./cursor');

Bulk = require('./bulk');

ObjectID = require('mongodb').ObjectID;

ref = require('lodash'), extend = ref.extend, isFunction = ref.isFunction, isBoolean = ref.isBoolean;

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
      return cursor.toArray(callback);
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
    return this.find(query, projection).next(callback);
  };

  Collection.prototype.findAndModify = function(opts, callback) {
    return this.execute('findAndModify', opts, function(err, result) {
      if (err) {
        return callback(err);
      }
      return callback(null, result.value, result.lastErrorObject || {
        n: 0
      });
    });
  };

  Collection.prototype.count = function(query, callback) {
    if (isFunction(query)) {
      return this.count({}, query);
    }
    return this.find(query).count(callback);
  };

  Collection.prototype.distinct = function(field, query, callback) {
    var params;
    params = {
      key: field,
      query: query
    };
    return this.execute('distinct', params, function(err, result) {
      if (err) {
        return callback(err);
      }
      return callback(null, result.values);
    });
  };

  Collection.prototype.insert = function(docOrDocs, opts, callback) {
    var docs, i;
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
    i = 0;
    while (i < docs.length) {
      if (!docs[i]._id) {
        docs[i]._id = ObjectID.createPk();
      }
      i++;
    }
    return this.collection.insert(docs, extend(writeOpts, opts), function(err) {
      if (err) {
        return callback(err);
      }
      return callback(null, docOrDocs);
    });
  };

  Collection.prototype.update = function(query, update, opts, callback) {
    if (!opts && !callback) {
      return this.update(query, update, {}, noop);
    }
    if (isFunction(opts)) {
      return this.update(query, update, {}, opts);
    }
    return this.collection.update(query, update, extend(writeOpts, opts), function(err, result) {
      if (err) {
        return callback(err);
      }
      return callback(null, result.result);
    });
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
      }, opts), callback);
    } else {
      return this.insert(doc, opts, callback);
    }
  };

  Collection.prototype.remove = function(query, opts, callback) {
    var deleteOperation, finish;
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
    finish = function(err, result) {
      if (err) {
        return callback(err);
      }
      return callback(null, result.result);
    };
    return this.collection[deleteOperation](query, extend(opts, writeOpts), finish);
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
    return this.collection.rename(name, opts, callback);
  };

  Collection.prototype.drop = function(callback) {
    return this.execute('drop', callback);
  };

  Collection.prototype.stats = function(callback) {
    return this.execute('collStats', callback);
  };

  Collection.prototype.mapReduce = function(map, reduce, opts, callback) {
    if (isFunction(opts)) {
      return this.mapReduce(map, reduce, {}, opts);
    }
    if (!callback) {
      return this.mapReduce(map, reduce, opts, noop);
    }
    return this.collection.mapReduce(map, reduce, opts, callback);
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
    return this.collection.s.db.command(obj, callback);
  };

  Collection.prototype.toString = function() {
    return this.collection.s.name;
  };

  Collection.prototype.dropIndexes = function(callback) {
    return this.execute('dropIndexes', {
      index: '*'
    }, callback);
  };

  Collection.prototype.dropIndex = function(index, callback) {
    return this.execute('dropIndexes', {
      index: index
    }, callback);
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
    return this.collection.createIndex(index, opts, callback);
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
    return this.collection.ensureIndex(index, opts, callback);
  };

  Collection.prototype.getIndexes = function(callback) {
    return this.collection.indexes(callback);
  };

  Collection.prototype.reIndex = function(callback) {
    return this.execute('reIndex', callback);
  };

  Collection.prototype.isCapped = function(callback) {
    return this.collection.isCapped(callback);
  };

  Collection.prototype.group = function(doc, callback) {
    return this.collection.group(doc.key || doc.keyf, doc.cond, doc.initial, doc.reduce, doc.finalize, callback);
  };

  Collection.prototype.aggregate = function(pipeline, opts, callback) {
    var strm;
    strm = new Cursor(this.collection.aggregate(pipeline, opts));
    if (callback) {
      return strm.toArray(callback);
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

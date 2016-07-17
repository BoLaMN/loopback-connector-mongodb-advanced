var Connector, ORM, Query, debug, normalizeId, normalizeIds, parseUpdateData, ref, rewriteId,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

debug = require('debug')('loopback:connector:mongodb-advanced');

Query = require('./query');

Connector = require('loopback-connector').Connector;

ref = require('./utils'), normalizeIds = ref.normalizeIds, normalizeId = ref.normalizeId, rewriteId = ref.rewriteId, parseUpdateData = ref.parseUpdateData;

ORM = (function(superClass) {
  extend(ORM, superClass);

  function ORM() {
    return ORM.__super__.constructor.apply(this, arguments);
  }


  /**
   * Find matching model instances by the filter
  #
   * @param {String} model The model name
   * @param {Object} filter The filter
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.all = function(modelName, filter, options, callback) {
    var aggregate, collection, cursor, fields, model, ref1, where;
    if (options == null) {
      options = {};
    }
    debug('all', modelName, filter);
    collection = this.collection(modelName);
    model = this.model(modelName);
    ref1 = new Query(filter, model.model), filter = ref1.filter, options = ref1.options;
    debug('all.filter', modelName, filter);
    where = filter.where, aggregate = filter.aggregate, fields = filter.fields;
    if (aggregate) {
      aggregate = [
        {
          '$match': where
        }, {
          '$group': filter.aggregateGroup
        }
      ];
      cursor = collection.aggregate(aggregate, options);
    } else {
      cursor = collection.find(where, fields, options);
    }
    return cursor.mapArray(rewriteId).tap(function(results) {
      return debug('all.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Count the number of instances for the given model
  #
   * @param {String} model The model name
   * @param {Function} [callback] The callback function
   * @param {Object} filter The filter for where
  #
   */

  ORM.prototype.count = function(modelName, filter, options, callback) {
    var collection, model, ref1, where;
    if (options == null) {
      options = {};
    }
    debug('count', modelName, filter);
    if (typeof filter === 'object') {
      delete filter.fields;
    }
    collection = this.collection(modelName);
    model = this.model(modelName);
    ref1 = new Query(filter, model.model), filter = ref1.filter, options = ref1.options;
    where = filter.where;
    return collection.count(where).tap(function(results) {
      return debug('count.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Create a new model instance for the given data
   * @param {String} model The model name
   * @param {Object} data The model data
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.create = function(modelName, data, options, callback) {
    var collection, docs;
    if (options == null) {
      options = {};
    }
    debug('create', modelName, data);
    collection = this.collection(modelName);
    docs = Array.isArray(data) ? data : [data];
    return collection.insert(normalizeIds(docs), {
      safe: true
    }).tap(function(results) {
      return debug('create.callback', modelName, results);
    }).then(function(results) {
      return results.insertedIds[0];
    }).asCallback(callback);
  };


  /**
   * Delete a model instance by id
   * @param {String} model The model name
   * @param {*} id The id value
   * @param [callback] The callback function
   */

  ORM.prototype.destroy = function(modelName, id, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('delete', modelName, id);
    collection = this.collection(modelName);
    return collection.remove({
      _id: id
    }, true).tap(function(results) {
      return debug('delete.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Delete all instances for the given model
   * @param {String} model The model name
   * @param {Object} [where] The filter for where
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.destroyAll = function(modelName, filter, options, callback) {
    var collection, fields, model, ref1, where;
    if (options == null) {
      options = {};
    }
    debug('destroyAll', modelName, filter);
    if (typeof filter === 'object') {
      delete filter.fields;
    }
    collection = this.collection(modelName);
    model = this.model(modelName);
    ref1 = new Query(filter, model.model), filter = ref1.filter, options = ref1.options;
    where = filter.where, fields = filter.fields;
    return collection.remove(where, options).then(function(results) {
      debug('destroyAll.callback', results);
      if (!Array.isArray(results)) {
        results = [results];
      }
      results = results.map(function(result) {
        return {
          id: result
        };
      });
      return results;
    }).tap(function(results) {
      return debug('destroyAll.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Check if a model instance exists by id
   * @param {String} model The model name
   * @param {*} id The id value
   * @param {Function} [callback] The callback function
  #
   */

  ORM.prototype.exists = function(modelName, id, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('exists', modelName, id);
    collection = this.collection(modelName);
    return collection.findOne({
      _id: id
    }, options).tap(function(results) {
      return debug('findOne.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Find a model instance by id
   * @param {String} model The model name
   * @param {*} id The id value
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.find = function(modelName, id, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('find', modelName, id);
    collection = this.collection(modelName);
    return collection.findOne({
      _id: id
    }, options, rewriteId).tap(function(results) {
      return debug('find.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Find a matching model instances by the filter
   * or create a new instance
  #
   * Only supported on mongodb 2.6+
  #
   * @param {String} model The model name
   * @param {Object} data The model instance data
   * @param {Object} filter The filter
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.findOrCreate = function(modelName, filter, data, callback) {
    var collection, fields, model, options, query, ref1, sort, where;
    if (filter == null) {
      filter = {};
    }
    debug('findOrCreate', modelName, filter, data);
    collection = this.collection(modelName);
    model = this.model(modelName);
    ref1 = new Query(filter, model.model), filter = ref1.filter, options = ref1.options;
    where = filter.where, fields = filter.fields, sort = filter.sort;
    query = {
      projection: fields,
      sort: sort,
      upsert: true
    };
    return collection.findOneAndUpdate(where, {
      $setOnInsert: data
    }, query).tap(function(results) {
      return debug('findOrCreate.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Replace properties for the model instance data
   * @param {String} model The name of the model
   * @param {*} id The instance id
   * @param {Object} data The model data
   * @param {Object} options The options object
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.replaceById = function(modelName, id, data, options, callback) {
    if (options == null) {
      options = {};
    }
    debug('replaceById', modelName, id, data);
    return this.replaceWithOptions(model, id, data, {
      upsert: false
    }).tap(function(results) {
      return debug('replaceById.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Replace model instance if it exists or create a new one if it doesn't
  #
   * @param {String} model The name of the model
   * @param {Object} data The model instance data
   * @param {Object} options The options object
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.replaceOrCreate = function(modelName, data, options, callback) {
    if (options == null) {
      options = {};
    }
    debug('replaceOrCreate', modelName, data);
    return this.replaceWithOptions(model, id, data, {
      upsert: true
    }).tap(function(results) {
      return debug('replaceOrCreate.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Update a model instance with id
   * @param {String} model The name of the model
   * @param {Object} id The id of the model instance
   * @param {Object} data The property/value pairs to be
   *                 updated or inserted if {upsert: true}
   *                 is passed as options
   * @param {Object} options The options you want to pass
   *                 for update, e.g, {upsert: true}
   * @callback {Function} [callback] Callback function
   */

  ORM.prototype.replaceWithOptions = function(modelName, id, data, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('updateWithOptions', modelName, id, data);
    collection = this.collection(modelName);
    return collection.update({
      _id: normalizeId(id)
    }, data, options).tap(function(results) {
      return debug('updateWithOptions.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Save the model instance for the given data
   * @param {String} model The model name
   * @param {Object} data The model data
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.save = function(modelName, data, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('save', modelName, data);
    collection = this.collection(modelName);
    return collection.save(normalizeId(data), options).tap(function(results) {
      return debug('save.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Update all matching instances
   * @param {String} model The model name
   * @param {Object} where The search criteria
   * @param {Object} data The property/value pairs to be updated
   * @callback {Function} callback Callback function
   */

  ORM.prototype.update = function(modelName, filter, data, options, callback) {
    var collection, model, ref1, where;
    if (options == null) {
      options = {};
    }
    debug('update', modelName, filter, data);
    if (typeof filter === 'object') {
      delete filter.fields;
    }
    collection = this.collection(modelName);
    model = this.model(modelName);
    ref1 = new Query(filter, model.model), filter = ref1.filter, options = ref1.options;
    where = filter.where;
    return collection.update(where, data, options).tap(function(results) {
      return debug('update.callback', modelName, results);
    }).asCallback(callback, {
      spread: true
    });
  };

  ORM.prototype.updateAll = ORM.update;


  /**
   * Update properties for the model instance data
   * @param {String} model The model name
   * @param {Object} data The model data
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.updateAttributes = function(modelName, id, data, options, callback) {
    var collection, sort;
    if (options == null) {
      options = {};
    }
    debug('updateAttributes', modelName, id, data);
    data = parseUpdateData(data);
    collection = this.collection(modelName);
    id = data[this.idName(modelName)];
    sort = ['_id', 'asc'];
    return collection.findAndModify({
      _id: id
    }, data, sort).tap(function(results) {
      return debug('updateAttributes.callback', modelName, results);
    }).asCallback(callback);
  };


  /**
   * Update if the model instance exists with the same
   * id or create a new instance
  #
   * @param {String} model The model name
   * @param {Object} data The model instance data
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.updateOrCreate = ORM.save;

  return ORM;

})(Connector);

module.exports = ORM;

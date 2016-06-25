var Connector, ORM, Query, debug, rewriteIds,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

debug = require('debug')('loopback:connector:mongodb-advanced');

Query = require('./query');

Connector = require('loopback-connector').Connector;

rewriteIds = require('./utils').rewriteIds;

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
    var aggregate, collection, fields, finish, model, ref, where;
    if (options == null) {
      options = {};
    }
    debug('all', modelName, filter);
    collection = this.collection(modelName);
    model = this.model(modelName);
    ref = new Query(filter, model.model), filter = ref.filter, options = ref.options;
    where = filter.where, aggregate = filter.aggregate, fields = filter.fields;
    debug(filter.lookups);
    finish = function(err, results) {
      debug('all.callback', modelName, results);
      return callback(err, rewriteIds(results));
    };
    if (aggregate) {
      aggregate = [
        {
          '$match': where
        }, {
          '$group': filter.aggregateGroup
        }
      ];
      return collection.aggregate(aggregate, finish);
    } else {
      return collection.find(where, fields, options, finish);
    }
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
    var collection, model, ref, where;
    if (options == null) {
      options = {};
    }
    debug('count', modelName, filter);
    if (typeof filter === 'object') {
      delete filter.fields;
    }
    collection = this.collection(modelName);
    model = this.model(modelName);
    ref = new Query(filter, model), filter = ref.filter, options = ref.options;
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
    var collection;
    if (options == null) {
      options = {};
    }
    debug('create', modelName, data);
    collection = this.collection(modelName);
    return collection.insert(data, {
      safe: true
    }).tap(function(results) {
      return debug('create.callback', modelName, results);
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
    var collection, fields, model, ref, where;
    if (options == null) {
      options = {};
    }
    debug('destroyAll', modelName, filter);
    if (typeof filter === 'object') {
      delete filter.fields;
    }
    collection = this.collection(modelName);
    model = this.model(modelName);
    ref = new Query(filter, model), filter = ref.filter, options = ref.options;
    where = filter.where, fields = filter.fields;
    return collection.remove(where, options, function(err, results) {
      var result, resultsArray;
      if (err) {
        return callback(err);
      }
      resultsArray = [];
      if (!Array.isArray(results)) {
        resultsArray.push({
          id: results
        });
        return callback(null, resultsArray);
      }
      results.forEach(function(result) {
        return resultsArray.push({
          id: result
        });
      });
      result = rewriteIds(resultArray, properties);
      debug('destroyAll.callback', result);
      return callback(null, result);
    });
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
    }, options, callback);
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
    var collection;
    if (filter == null) {
      filter = {};
    }
    debug('findOrCreate', modelName, filter, data);
    collection = this.collection(modelName);
    return callback(null, {});
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
    var collection;
    if (options == null) {
      options = {};
    }
    debug('replaceById', modelName, id, data);
    collection = this.collection(modelName);
    return callback(null, {});
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
    var collection;
    if (options == null) {
      options = {};
    }
    debug('replaceOrCreate', modelName, data);
    collection = this.collection(modelName);
    return callback(null, {});
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
    return callback(null, {});
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
    return collection.save(data, options, callback);
  };


  /**
   * Update all matching instances
   * @param {String} model The model name
   * @param {Object} where The search criteria
   * @param {Object} data The property/value pairs to be updated
   * @callback {Function} callback Callback function
   */

  ORM.prototype.update = function(modelName, filter, data, options, callback) {
    var collection, model, ref, where;
    if (options == null) {
      options = {};
    }
    debug('update', modelName, filter, data);
    if (typeof filter === 'object') {
      delete filter.fields;
    }
    collection = this.collection(modelName);
    model = this.model(modelName);
    ref = new Query(filter, model), filter = ref.filter, options = ref.options;
    where = filter.where;
    return collection.update(where, data, options).tap(function(results) {
      return debug('update.callback', modelName, results);
    }).asCallback(callback);
  };

  ORM.prototype.updateAll = ORM.update;


  /**
   * Update properties for the model instance data
   * @param {String} model The model name
   * @param {Object} data The model data
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.updateAttributes = function(modelName, id, data, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('updateAttributes', modelName, id, data);
    collection = this.collection(modelName);
    return callback(null, {});
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

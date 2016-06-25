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

  ORM.prototype.all = function(model, filter, options, callback) {
    var aggregate, collection, fields, finish, properties, ref, where;
    if (options == null) {
      options = {};
    }
    debug('all', model, filter);
    collection = this.collection(model);
    properties = this.properties(model);
    ref = new Query(filter, properties), filter = ref.filter, options = ref.options;
    where = filter.where, aggregate = filter.aggregate, fields = filter.fields;
    finish = function(err, results) {
      return callback(err, rewriteIds(results, properties));
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

  ORM.prototype.count = function(model, filter, options, callback) {
    var collection, properties, ref, where;
    if (options == null) {
      options = {};
    }
    debug('count', model, filter);
    if (typeof filter === 'object') {
      delete filter.fields;
    }
    collection = this.collection(model);
    properties = this.properties(model);
    ref = new Query(filter, properties), filter = ref.filter, options = ref.options;
    where = filter.where;
    return collection.count(where, callback);
  };


  /**
   * Create a new model instance for the given data
   * @param {String} model The model name
   * @param {Object} data The model data
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.create = function(model, data, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('create', model, data);
    collection = this.collection(model);
    return collection.insert(data, {
      safe: true
    }, callback);
  };


  /**
   * Delete a model instance by id
   * @param {String} model The model name
   * @param {*} id The id value
   * @param [callback] The callback function
   */

  ORM.prototype.destroy = function(model, id, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('delete', model, id);
    collection = this.collection(model);
    return collection.remove({
      _id: id
    }, true, callback);
  };


  /**
   * Delete all instances for the given model
   * @param {String} model The model name
   * @param {Object} [where] The filter for where
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.destroyAll = function(model, filter, options, callback) {
    var collection, fields, properties, ref, where;
    if (options == null) {
      options = {};
    }
    debug('destroyAll', model, filter);
    if (typeof filter === 'object') {
      delete filter.fields;
    }
    collection = this.collection(model);
    properties = this.properties(model);
    ref = new Query(filter, properties), filter = ref.filter, options = ref.options;
    where = filter.where, fields = filter.fields;
    return collection.remove(where, options, function(err, results) {
      var resultsArray;
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
      return callback(null, rewriteIds(resultArray, properties));
    });
  };


  /**
   * Check if a model instance exists by id
   * @param {String} model The model name
   * @param {*} id The id value
   * @param {Function} [callback] The callback function
  #
   */

  ORM.prototype.exists = function(model, id, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('exists', model, id);
    collection = this.collection(model);
    return collection.findOne({
      _id: id
    }, options, callback);
  };


  /**
   * Find a model instance by id
   * @param {String} model The model name
   * @param {*} id The id value
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.find = function(model, id, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('find', model, id);
    collection = this.collection(model);
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

  ORM.prototype.findOrCreate = function(model, filter, data, callback) {
    var collection;
    if (filter == null) {
      filter = {};
    }
    debug('findOrCreate', model, filter, data);
    collection = this.collection(model);
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

  ORM.prototype.replaceById = function(model, id, data, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('replaceById', model, id, data);
    collection = this.collection(model);
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

  ORM.prototype.replaceOrCreate = function(model, data, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('replaceOrCreate', model, data);
    collection = this.collection(model);
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

  ORM.prototype.replaceWithOptions = function(model, id, data, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('updateWithOptions', model, id, data);
    collection = this.collection(model);
    return callback(null, {});
  };


  /**
   * Save the model instance for the given data
   * @param {String} model The model name
   * @param {Object} data The model data
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.save = function(model, data, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('save', model, data);
    collection = this.collection(model);
    return collection.save(data, options, callback);
  };


  /**
   * Update all matching instances
   * @param {String} model The model name
   * @param {Object} where The search criteria
   * @param {Object} data The property/value pairs to be updated
   * @callback {Function} callback Callback function
   */

  ORM.prototype.update = function(model, filter, data, options, callback) {
    var collection, properties, ref, where;
    if (options == null) {
      options = {};
    }
    debug('update', model, filter, data);
    if (typeof filter === 'object') {
      delete filter.fields;
    }
    collection = this.collection(model);
    properties = this.properties(model);
    ref = new Query(filter, properties), filter = ref.filter, options = ref.options;
    where = filter.where;
    return collection.update(where, data, options, callback);
  };

  ORM.prototype.updateAll = ORM.update;


  /**
   * Update properties for the model instance data
   * @param {String} model The model name
   * @param {Object} data The model data
   * @param {Function} [callback] The callback function
   */

  ORM.prototype.updateAttributes = function(model, id, data, options, callback) {
    var collection;
    if (options == null) {
      options = {};
    }
    debug('updateAttributes', model, id, data);
    collection = this.collection(model);
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

'use strict';
var Aggregate, ObjectId, Query, Where, debug, extend, isArray, isFunction, isObject, isPlainObject, isString, ref,
  hasProp = {}.hasOwnProperty;

debug = require('debug')('loopback:connector:mongodb-advanced');

Aggregate = require('./aggregate');

Where = require('./where');

ref = require('lodash'), isObject = ref.isObject, isString = ref.isString, isFunction = ref.isFunction, isArray = ref.isArray, isPlainObject = ref.isPlainObject, extend = ref.extend;

ObjectId = require('mongodb').ObjectId;


/**
* Query
 */

Query = (function() {
  function Query(filter, properties) {
    var key, value;
    this.properties = properties;
    this.filter = {
      fields: {},
      where: {}
    };
    this.options = {
      sort: {}
    };
    for (key in filter) {
      if (!hasProp.call(filter, key)) continue;
      value = filter[key];
      if (isFunction(this[key])) {
        this[key](value);
      } else {
        debug('query filter ' + key + ' not found, value: ', value);
      }
    }
  }


  /**
   * set where query
  #
   * @param {String} key
   * @api public
   */

  Query.prototype.where = function(conditions) {
    var query;
    query = new Where(conditions).query;
    this.filter.where = query;
    return this;
  };


  /**
   * set aggregate query
  #
   * @param {String} key
   * @api public
   */

  Query.prototype.aggregate = function(conditions) {
    var query;
    query = new Aggregate(conditions).query;
    this.filter.aggregate = query;
    return this;
  };


  /**
   * Handle iterating over include/exclude methods
  #
   * @param {String} key
   * @param {Mixed} value
   * @api public
   */

  Query.prototype.fields = function(fields, value) {
    var keys;
    if (value == null) {
      value = 1;
    }
    if (isArray(fields)) {
      fields.forEach((function(_this) {
        return function(key) {
          return _this.fields(key);
        };
      })(this));
    }
    if (isObject(fields)) {
      keys = Object.keys(fields);
      keys.forEach((function(_this) {
        return function(key) {
          return _this.fields(key, fields[key]);
        };
      })(this));
    }
    if (isString(fields)) {
      this.filter.fields[fields] = value;
    }
    return this;
  };


  /**
   * Include fields in a result
  #
   * @param {String} key
   * @api public
   */

  Query.prototype.include = function(includes) {
    debug('todo, move over to $lookup');
    return this;
  };


  /**
   * Exclude fields from result
  #
   * @param {String} key
   * @api public
   */

  Query.prototype.exclude = function(fields) {
    this.fields(fields, 0);
    return this;
  };


  /**
   * Set query limit
  #
   * @param {Number} limit - limit number
   * @api public
   */

  Query.prototype.limit = function(limit) {
    this.options.limit = limit;
    return this;
  };


  /**
   * Set query skip
  #
   * @param {Number} skip - skip number
   * @api public
   */

  Query.prototype.skip = function(skip) {
    this.options.skip = skip;
    return this;
  };


  /**
   * Alias for skip
  #
   * @param {String} offset
   * @api public
   */

  Query.prototype.offset = function(offset) {
    return this.skip(offset);
  };


  /**
   * Search using text index
  #
   * @param {String} text
   * @api public
   */

  Query.prototype.search = function(text) {
    this.where({
      '$text': {
        '$search': text
      }
    });
    return this;
  };


  /**
   * Sort query results
  #
   * @param {Object} sort - sort params
   * @api public
   */

  Query.prototype.sort = function(sorts, value) {
    var matches;
    if (isArray(sorts)) {
      sorts.forEach((function(_this) {
        return function(sort) {
          return _this.sort.apply(_this, sort.split(' '));
        };
      })(this));
    }
    if (isString(value)) {
      matches = sorts.match(/([\w\d]+) (A|DE)SC/gi);
      if (matches) {
        return this.sort(matches);
      }
      if (sorts === 'id') {
        sorts = '_id';
      }
      this.options.sort[sorts] = value === 'DE' ? -1 : 1;
    } else {
      this.options.sort._id = 1;
    }
    return this;
  };

  return Query;

})();

module.exports = Query;

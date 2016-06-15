var Aggregate, ObjectId, Query, isPlainObject, isString, ref,
  indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

Aggregate = require('./aggregate');

ref = require('lodash'), isString = ref.isString, isPlainObject = ref.isPlainObject;

ObjectId = require('mongodb').ObjectId;

Query = (function() {
  function Query(filter, properties) {
    this.properties = properties;
    this.fields = this.parseFields(filter.fields);
    this.checkAggregate(filter);
    delete filter.fields;
    this.filter = this.normalizeFilter(filter) || {};
    this;
  }

  Query.prototype.checkAggregate = function(filter) {
    var aggregateOptions, aggregates;
    aggregateOptions = ['lookup', 'groupBy', 'sum', 'average', 'min', 'max'];
    aggregates = Object.keys(filter).filter(function(filterKey) {
      return indexOf.call(aggregateOptions, filterKey) >= 0;
    });
    if (!aggregates.length) {
      return filter;
    }
    this.aggregate = new Aggregate(filter);
  };

  Query.prototype.normalizeFilter = function(filter) {
    filter.where = this.parseWhere(filter.where);
    filter.sort = this.parseSort(filter.sort);
    return filter;
  };

  Query.prototype.parseWhere = function(where) {
    var query;
    query = {};
    if (where === null || typeof where !== 'object') {
      return query;
    }
    Object.keys(where).forEach(function(propName) {
      var cond, options, spec;
      cond = where[propName];
      if (propName === 'and' || propName === 'or' || propName === 'nor') {
        if (Array.isArray(cond)) {
          cond = cond.map((function(_this) {
            return function(c) {
              return _this.parseWhere(c);
            };
          })(this));
        }
        query['$' + propName] = cond;
        delete query[propName];
      }
      if (propName === 'id') {
        propName = '_id';
      }
      spec = false;
      options = null;
      if (isPlainObject(cond)) {
        options = cond.options;
        spec = Object.keys(cond)[0];
        cond = cond[spec];
      }
      if (spec) {
        query[propName] = (function() {
          switch (spec) {
            case 'between':
              return {
                $gte: cond[0],
                $lte: cond[1]
              };
            case 'inq':
              return {
                $in: cond
              };
            case 'nin':
              return {
                $nin: cond
              };
            case 'like':
              return {
                $regex: new RegExp(cond, options)
              };
            case 'nlike':
              return {
                $not: new RegExp(cond, options)
              };
            case 'neq':
              return {
                $ne: cond
              };
            case 'regexp':
              return {
                $regex: cond
              };
          }
        })();
        if (!query[propName]) {
          query[propName] = {};
          return query[propName]['$' + spec] = cond;
        }
      } else {
        return query[propName] = cond || {
          $type: 10
        };
      }
    });
    return query;
  };

  Query.prototype.parseSort = function(order) {
    var index, input, key, len, m, ref1, sort;
    sort = {};
    if (order) {
      if (isString(order)) {
        order = order.split(',');
      }
      index = 0;
      len = order.length;
      while (index < len) {
        ref1 = /\s+([\w\d]+) (A|DE)SC$/.exec(order[index]), input = ref1[0], key = ref1[1], m = ref1[2];
        if (key === 'id') {
          key = '_id';
        }
        if (m && m[1] === 'DE') {
          sort[key] = -1;
        } else {
          sort[key] = 1;
        }
        index++;
      }
    } else {
      sort = {
        _id: 1
      };
    }
    return sort;
  };

  Query.prototype.parseFields = function(original) {
    var fields;
    if (original == null) {
      original = [];
    }
    fields = {};
    original.forEach(function(field) {
      return fields[field] = 1;
    });
    return fields;
  };

  return Query;

})();

module.exports = Query;

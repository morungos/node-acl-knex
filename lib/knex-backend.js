'use strict';

var knex = require('knex');
var async = require('async');
var _ = require('lodash');

function KnexDBBackend(db){
  this.db = db;
}

var bucketNumbers = {
  'meta': 0,
  'resources': 1,
  'parents': 2,
  'users': 3,
  'permissions': 4
};


KnexDBBackend.prototype = {

  begin: function() {
    return {promise: this.db.raw('savepoint service_transaction')};
  },

  add: function(transaction, bucket, key, values) {
    var _this = this;
    transaction.promise = transaction.promise.then(function() {
      _this.db('acl').first('id', 'key', 'value')
        .where({bucket: bucket, key: key})
        .then(function(found) {
          var data = (found !== undefined) ? JSON.parse(found) : {};
          if (found !== undefined) {
            _this.db('acl').where('id', '=', found.id).update({values: _.union(values, data)});
          } else {
            _this.db('acl').insert({bucket: bucket, key: key, values: _.union(values, data)})
          }
        });
      });
  },

  del: function(transaction, bucket, keys) {
    console.log("del: not yet implemented", bucket, keys);
  },

  remove: function(transaction, bucket, key, values) {
    console.log("remove: not yet implemented", bucket, keys);
  },

  clean: function(callback) {
    var _this = this;
    _this.db.truncate('acl')
      .then(function() {
        return callback(null);
      }).catch(function(err) {
        return callback(err);
      });
  },

  end: function(transaction, callback){
    var _this = this;
    transaction.promise.then(function() {
      _this.db.raw('release savepoint service_transaction');
      callback(null);
    }).catch(function(err) {
      _this.db.raw('rollback to service_transaction').then(function() {
        _this.db.raw('release savepoint service_transaction')
      });
      callback(err);
    });
  },

  get: function(bucket, key, callback) {
    if (bucket.indexOf('allows') != -1) {
      key = bucket;
      bucket = bucketNumbers['permissions'];
    }
    this.db.first('key', 'value')
      .from('acl')
      .where({'bucket': bucket, 'key': key})
      .then(function(value) {
        console.log("Got value", value);
        callback(null, value);
      }).catch(function(err) {
        console.log("Got err", err);
        callback(err, null);
      });
  },

  union: function() {
    console.log("Error: union not yet implemented");
  },

  setup: function(callback) {
    var _this = this;
    _this.db.schema.hasTable('acl').then(function(exists) {
      if (!exists) {
        return _this.db.schema.createTable('acl', function(table) {
          table.increments('id');
          table.integer('bucket').notNullable();
          table.string('key', 24).notNullable();
          table.text('values', 'longtext').notNullable();
          table.unique(['bucket', 'key']);
        });
      }
    }).then(function() {
      return callback(null);
    }).catch(function(err) {
      return callback(err);
    });
  },

  teardown: function(callback) {
    this.db.schema.dropTable('acl')
      .then(function() {
        return callback(null);
      }).catch(function(err) {
        return callback(err);
    });
  }
}

exports = module.exports = KnexDBBackend;
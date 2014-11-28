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
    return [];
  },

  clean: function(callback) {
    this.db.truncate('acl')
      .then(function() {
        return callback(null);
      }).catch(function(err) {
        return callback(err);
      });
  },

  end: function(transaction, callback){
    console.log("End", transaction);
    this.db.transaction(function(trx) {
      Promise.all(transaction.map(function(obj) {
        return obj.promise.transacting(trx);
      })).then(function(values) {
        _length = transaction.length;
        for(var i = 0; i < _length; i++) {
          transaction[i].recipient(null, values[i]);
        }
      }).catch(function(err) {
        _length = transaction.length;
        for(var i = 0; i < _length; i++) {
          transaction[i].recipient(err, null);
        }
      });
    });
  },

  get: function(bucket, key, callback) {
    if (bucket.indexOf('allows') != -1) {
      key = bucket;
      bucket = bucketNumbers['permissions'];
    }
    this.db.select('key', 'value')
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
    console.log("Error: not yet implemented");
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
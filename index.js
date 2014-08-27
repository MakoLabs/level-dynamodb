var util = require('util')
var AWS = require('aws-sdk')
var async = require('async')
var ALD = require('abstract-leveldown').AbstractLevelDOWN

var Iterator = require('./iterator')

AWS.config.apiVersions = { dynamodb: '2012-08-10' }

var DynamoDown = module.exports = function (location) {
  if (!(this instanceof DynamoDown)) {
    return new DynamoDown(location)
  }

  var tableHash = location.split('/')
  this.tableName = tableHash[0]
  this.hashKey = tableHash[1] || '!'

  ALD.call(this, location)
}

util.inherits(DynamoDown, ALD)

DynamoDown.prototype._open = function(options, cb) {
  var self = this

  if (!options.dynamo) {
    var msg = 'DB open requires options argument with "dynamo" key.'
    return cb(new Error(msg))
  }

  var dynOpts = options.dynamo
  dynOpts.tableName = this.tableName

  this.ddb = new AWS.DynamoDB(dynOpts)

  if (options.createIfMissing) {
    this.createTable({
      ProvisionedThroughput: options.dynamo.ProvisionedThroughput
    }, function(err, data) {
      var exists = err && (err.code === 'ResourceInUseException')
      if (options.errorIfExists && exists) return cb(err)
      if (err && !exists) return cb(err)

      return cb(null, self)
    })
  } else {
    setImmediate(function () {
      return cb(null, self)
    })
  }
}

DynamoDown.prototype._close = function(cb) {
  this.ddb = null
  setImmediate(function() {
    return cb(null)
  })
}

DynamoDown.prototype._put = function(key, value, options, cb) {
   if (typeof options == 'function')
    cb = options;
    
  var params = {
    TableName: this.tableName,
    Item: {
      hkey: { S: this.hashKey },
      rkey: { S: key },
      value: {
        S: value.toString()
      }
    }
  }

  this.ddb.putItem(params, cb)
}

DynamoDown.prototype._get = function(key, options, cb) {
  if (typeof options == 'function'){
    cb = options;
    options = null;
  }
   
  var params = {
    TableName: this.tableName,
    Key: {
      hkey: { S: this.hashKey },
      rkey: { S: key }
    }
  }

  this.ddb.getItem(params, function(err, data) {
    if (err) return cb(err)
    if (data && data.Item && data.Item.value) {
      var value = data.Item.value.S
      if (options && options.asBuffer === false) {
        return cb(null, value)
      } else {
        return cb(null, new Buffer(value))
      }
    } else {
      return cb(new Error('NotFound'))
    }
  })
}

DynamoDown.prototype._del = function(key, options, cb) {
  if (typeof options == 'function')
    cb = options;
    
  var params = {
    TableName: this.tableName,
    Key: {
      hkey: { S: this.hashKey },
      rkey: { S: key }
    }
  }

  this.ddb.deleteItem(params, function(err, data) {
    if (err) return cb(err)
    cb(null, data)
  })
}

DynamoDown.prototype._batch = function (array, options, cb) {
  var self = this
  if (typeof options == 'function')
    cb = options;

  async.eachSeries(array, function (item, cb) {
    if (item.type === 'put') {
      self._put(item.key, item.value, options, cb)
    } else if (item.type === 'del') {
      self._del(item.key, options, cb)
    }
  }, cb)
}

DynamoDown.prototype._iterator = function(options) {
  return new Iterator(this, options)
}

DynamoDown.prototype.createTable = function(opts, cb) {
  var params = {
    TableName: this.tableName,
    AttributeDefinitions: [
      {
        AttributeName: "hkey",
        AttributeType: "S"
      },
      {
        AttributeName: "rkey",
        AttributeType: "S"
      }
    ],
    KeySchema: [
      {
        AttributeName: "hkey",
        KeyType: "HASH"
      },
      {
        AttributeName: "rkey",
        KeyType: "RANGE"
      }
    ]
  }

  params.ProvisionedThroughput = opts.ProvisionedThroughput || {
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 1
  }

  this.ddb.createTable(params, cb)
}

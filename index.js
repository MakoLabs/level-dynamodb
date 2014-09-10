var util = require('util');
var AWS = require('aws-sdk');
var async = require('async');
var highland = require('highland');
var Promise = require('bluebird');
var ALD = require('abstract-leveldown').AbstractLevelDOWN;

var Iterator = require('./iterator');

AWS.config.apiVersions = { dynamodb: '2012-08-10' };

var bulkBuffer = [];
var bulkBufferSize = 0;
var flushTimeout = null;

var DynamoDown = module.exports = function (location) {
    if (!(this instanceof DynamoDown)) {
	return new DynamoDown(location)
    }

    var tableHash = location.split('/')
    this.tableName = tableHash[0]
    this.hashKey = tableHash[1] || '!'
    this.maxDelay = 2500;
    this.flushing = null;
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

    if('maxDelay' in options) try{ this.maxDelay = parseInt(options.maxDelay); }catch(err){}
    if('bulkBufferSize' in options){
	bulkBufferSize = options.bulkBufferSize;
    }
    this.ProvisionedThroughput = options.dynamo.ProvisionedThroughput;

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
    var done = function(){
	this.ddb = null
	setImmediate(function() {
	    return cb(null)
	});
    };
    
    if(bulkBuffer.length > 0){
	self.flush(done);
    }else{
	done();
    }
}

DynamoDown.prototype._put = function(key, value, options, cb) {
    if (typeof options == 'function')
	cb = options;
    var hkey = this.hashKey;
    if('hash' in value){
	hkey = hkey + "~"+value.hash;
	delete value['hash'];
    }else if('hash' in options){
    	hkey = hkey + "~"+options.hash;
    }

    var self = this;

    if(bulkBufferSize > 0){
	var process = function(){
	    bulkBuffer.push({ type: 'put', key: key, value:value, params: params, hash: hkey });
	    if(bulkBuffer.length >= bulkBufferSize){
		self.flush(cb);
	    }else{
		// set the flush timeout if need be
		if(self.maxDelay > 0 && !flushTimeout){
		    flushTimeout = setTimeout(function(){
			flushTimeout = null;
			self.flush.bind(self)();
		    }, self.maxDelay);
		}
		setImmediate(cb);
	    }
	};
	if(self.flushing != null){
	    // we wait for the flush to complete
	    self.flushing.then(process);
	}else{
	    process();
	}
    }else{
    	var params = {
	    TableName: this.tableName,
	    Item: {
	        hkey: { S: hkey },
	        rkey: { S: key },
	        value: {
		    S: JSON.stringify(value)
	    	}
	    }
    	};
	this.ddb.putItem(params, cb)
    }
}

DynamoDown.prototype._get = function(key, options, cb) {
    if (typeof options == 'function'){
	cb = options;
	options = null;
    }
    
    var hkey = this.hashKey;
    if('hash' in options){
	hkey = hkey + "~"+options.hash;
    }
    
    var params = {
	TableName: this.tableName,
	Key: {
	    hkey: { S: hkey },
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
    
    var hkey = this.hashKey;
    if('hash' in options){
	hkey = hkey + "~"+options.hash;
    }
    
    var params = {
	TableName: this.tableName,
	Key: {
	    hkey: { S: hkey },
	    rkey: { S: key }
	}
    }

    var self = this;
    if(bulkBufferSize > 0){
	var process = function(){
	    bulkBuffer.push({ type: 'del', key: key, params: params, hash: hkey });
	    if(bulkBuffer.length >= bulkBufferSize){
		self.flush(cb);
	    }else{
		// set the flush timeout if need be
		if(self.maxDelay > 0 && !flushTimeout){
		    flushTimeout = setTimeout(function(){
			flushTimeout = null;
			self.flush.bind(self)();
		    }, self.maxDelay);
		}
		setImmediate(cb);
	    }
	};
	if(self.flushing != null){
	    // we wait for the flush to complete
	    self.flushing.then(process);
	}else{
	    process();
	}
    }else{
	this.ddb.deleteItem(params, function(err, data) {
	    if (err) return cb(err)
	    cb(null, data)
	});
    }
}

DynamoDown.prototype._batch = function (array, options, cb) {
    var self = this
    if (typeof options == 'function')
	cb = options;

    var self = this;
    if(bulkBufferSize > 0){
	var process = function(){
		var hkey = self.hashKey;
	    for(var i=0;i<array.length;i++){
	    	var entry = null;
		if (array[i].type === 'del') {
		    entry = { type: 'del', key: array[i].key, 'hash': hkey };
		}else{
		    entry = { type: 'put', key: array[i].key, value: array[i].value, 'hash': hkey };
		}
		if('hash' in array[i]) entry['hash'] = hkey + "~"+array[i].hash;
		bulkBuffer.push(entry);
		console.log(util.inspect(entry))
	    }
	    
	    if(bulkBuffer.length >= bulkBufferSize){
		self.flush(cb);
	    }else{
		// set the flush timeout if need be
		if(self.maxDelay > 0 && !flushTimeout){
		    flushTimeout = setTimeout(function(){
			flushTimeout = null;
			self.flush.bind(self)();
		    }, self.maxDelay);
		}
		setImmediate(cb);
	    }
	};
	if(self.flushing != null){
	    // we wait for the flush to complete
	    self.flushing.then(process);
	}else{
	    process();
	}
    }else{
	async.eachSeries(array, function (item, cb) {
	    var opts = {};
	    if('hash' in item) opts['hash'] = item.hash;
	    if (item.type === 'put') {
		//self._put(item.key, item.value, options, cb)
		self._put(item.key, item.value, opts, cb)
	    } else if (item.type === 'del') {
		//self._del(item.key, options, cb)
		self._del(item.key, opts, cb)
	    }
	}, cb);
    }
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

DynamoDown.prototype.flush = function(cb)
{
    if(flushTimeout){
	clearTimeout(flushTimeout);
	flushTimeout = null;
    }
    var self = this;
    if(bulkBuffer.length > 0){
	// need a transaction
	if(self.flushing){
    	    if(cb) self.flushing.nodeify(cb);
    	    return;
	}
	self.flushing = new Promise(function(resolve, reject){
	    var stream = highland(bulkBuffer).ratelimit(Math.ceil(0.95*self.ProvisionedThroughput.WriteCapacityUnits), 1000);
	    stream.each(function(entry){
		if(entry.type == 'del'){
		    var params = {
			TableName: self.tableName,
			Key: {
			    hkey: { S: entry.hash },
			    rkey: { S: entry.key }
			}
		    };
		    this.ddb.deleteItem(params, function(err){
			if(err) console.log("[delete error] Error deleting "+util.inspect(entry)+":"+err);
		    });		    
		}else{
		    var params = {
			TableName: self.tableName,
			Key: {
			    hkey: { S: entry.hash },
			    rkey: { S: entry.key }
			},
			AttributeUpdates: {
			    value: {
				Action: 'PUT',
				Value: {
				    S: JSON.stringify(entry.value)
				}
			    }
			}
		    };
		    console.log(util.inspect(params, { depth: 6 }));
		    self.ddb.updateItem(params, function(err){
			if(err) console.log("[update error] Error updating "+util.inspect(entry)+":"+err);			
		    });
		}
	    });
	    stream.on('end', function(){
		var flushing = self.flushing;
		self.flushing = null;
		bulkBuffer = [];
		resolve(true);
		if(cb) setImmediate(cb);
	    });
	    stream.resume();
	});
	if(cb) self.flushing.nodeify(cb);
    }else{
	if(cb) setImmediate(cb);
    }
};

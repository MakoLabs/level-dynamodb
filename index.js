var util = require('util');
var AWS = require('aws-sdk');
var async = require('async');
var highland = require('highland');
var Promise = require('bluebird');
var ALD = require('abstract-leveldown').AbstractLevelDOWN;
var EventEmitter = require('events').EventEmitter;

var Iterator = require('./iterator');

AWS.config.apiVersions = { dynamodb: '2012-08-10' };

var bulkBuffer = [];
var bulkBufferSize = 0;
var bulkStream = null;
var ended = false;
var inflight = 0;
var written = 0;
var pending = 0;
var flushed = false;

var DynamoDown = module.exports = function (location) {
    if (!(this instanceof DynamoDown)) {
	return new DynamoDown(location)
    }

    var tableHash = location.split('/')
    this.tableName = tableHash[0]
    this.hashKey = tableHash[1] || '!'
    this.maxDelay = 2500;
    this.events = new EventEmitter();
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
    this.ProvisionedThroughput = options.dynamo.ProvisionedThroughput;
    if('bulkBufferSize' in options){
	bulkBufferSize = options.bulkBufferSize;
	//var ratelimit = Math.ceil(0.02*this.ProvisionedThroughput.WriteCapacityUnits);
	//console.log("Using ratelimit of "+ratelimit);
	//bulkStream = highland().ratelimit(ratelimit, 1000).batch(20);
	bulkStream = highland().batch(24);
	bulkStream.on('data', self.process_batch.bind(self));
	bulkStream.on('end', function(){
	    ended = true; 
	    self.events.emit('end');
	});
    }
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
    if(options && typeof options == 'object' && 'hash' in options){
    	hkey = hkey + "~"+options.hash;
    }

    var self = this;

    if(bulkBufferSize > 0){
	bulkStream.write({ type: 'put', key: key, value:value, params: params, hash: hkey });
	if(cb) setImmediate(cb);
    }else{
    	var params = {
	    TableName: this.tableName,
	    Item: {
	        hkey: { S: hkey },
	        rkey: { S: key },
	        value: {
		    S: value
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
		return cb(null, value.toString())
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
    if(options && typeof options == 'object' && 'hash' in options){
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
	for(var i=0;i<array.length;i++){
	    var entry = null;
	    if (array[i].type === 'del') {
		entry = { type: 'del', key: array[i].key };
	    }else{
		entry = { type: 'put', key: array[i].key, value: array[i].value };
	    }
	    if('hash' in array[i]) entry['hash'] = self.hashKey + "~"+array[i].hash;
	    else if(options && typeof options == 'object' && 'hash' in options) entry['hash'] = self.hashKey + "~"+options.hash;
	    else entry['hash'] = self.hashKey;
	    bulkStream.write(entry);
	}
	if(cb) setImmediate(cb);
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

DynamoDown.prototype.process_batch = function(args)
{
    var self = this;
    var request;
    var items = [];
    for(var i=0;i<args.length;i++){
	var entry = args[i];
	if(entry.type == 'retry'){
	    items.push(entry.contents);
	}else if(entry.type == 'del'){
	    items.push({ 
		DeleteRequest: {
		    Key: {
			hkey: { S: entry.hash },
			rkey: { S: entry.key }
		    }
		}
	    });
	}else{
	    items.push({
		PutRequest: {
		    Item: {
			hkey: { S: entry.hash },
			rkey: { S: entry.key },
			value: {
			    S: entry.value
			}
		    }
		}
	    });
	}
    }
    request = { RequestItems: {}, ReturnItemCollectionMetrics: 'SIZE' };
    request['RequestItems'][self.tableName] = items;
    self.events.emit('save', request['RequestItems'][self.tableName].length);
    inflight += 1;
    self.ddb.batchWriteItem(request, function(err, data){
	inflight -= 1;
	/*
	  UnprocessedItems:
	  { lbs:
	  [ [Object],
          [Object],
          [Object],
          [Object],
          [Object],
          [Object],
          [Object],
	*/
	if(err) console.log("[batch write error] "+util.inspect(entry)+":"+err);
	if('UnprocessedItems' in data && self.tableName in data['UnprocessedItems']){
	    // retry
	    console.log("Rescheduled "+data['UnprocessedItems'][self.tableName].length+" items.");

	    // schedule the retry for 10 seconds
	    var retry_items = data['UnprocessedItems'][self.tableName];
	    pending += retry_items.length;
	    setTimeout(function(){
		for(var j=0;j<retry_items.length;j++){
		    bulkStream.write({ type: 'retry', contents: retry_items[j] });
		    --pending;
		}
	    }, 10000);
	    
	    // track writes
	    written = written + items.length - data['UnprocessedItems'][self.tableName].length;

	    // emit an event
	    self.events.emit('saved', items.length, data['UnprocessedItems'][self.tableName].length);
	}else{
	    // track writes
	    written += items.length;

	    // emit an event
	    self.events.emit('saved', items.length);

	    // end if we need to
	    if(flushed && pending == 0 && inflight == 0) bulkStream.write(highland.nil);
	}
	self.events.emit('written', written);
    });
};

DynamoDown.prototype.flush = function(cb)
{
    flushed = true;
    this.events.on('end', cb);
    if(pending == 0 && inflight == 0) bulkStream.write(highland.nil);
};

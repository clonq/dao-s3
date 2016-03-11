var s3 = require('./lib/aws/s3');
var uuid = require('uuid');
var _ = require('underscore');

var storage;
var buckets;
var isDirty = true;

function config(opts) {
    opts = opts || {};
    storage = opts.storage;
}

function create(model, cb){
    if(isDirty) {
        fetch(function(err, data){
            if(err) return cb(err);
            put.call(model);
            store(function(err){
                cb(err, model);
            });
        });
    } else {
        put.call(model);
        store(function(err){
            cb(err, model);
        });
    }
    function put() {
        this.$id = uuid.v4();
        this.$created = Date.now();
        var bucket = model.$type || 'unknown';
        if(!buckets[bucket]) buckets[bucket] = [];
        delete this.$type;
        buckets[bucket].push(this);
    }
}

function read(model, cb) {
    if(isDirty) {
        fetch(function(err, data){
            if(err) return cb(err);
            get.call(model, cb);
        });
    } else {
        get.call(model, cb);
    }
    function get() {
        if(this && this.$id) {
            var bucket = this.$type || 'unknown';
            var ret = _.findWhere(buckets[bucket], {$id:this.$id});
            return cb(null, ret);
        } else {
            return cb(new Error('required $id is missing'));
        }
    }
}

function update(model, cb){
    if(isDirty) {
        fetch(function(err, data){
            if(err) return cb(err);
            try {
                var updatedModel = put.call(model);
                store(function(err){
                    cb(err, updatedModel);
                });
            } catch(err) {
                cb(err);
            }
        });
    } else {
        try {
            var updatedModel = put.call(model);
            store(function(err){
                cb(err, updatedModel);
            });
        } catch(err) {
            cb(err);
        }
    }
    function put() {
        if(this && this.$id) {
            var bucket = this.$type || 'unknown';
            var existingModel = _.findWhere(buckets[bucket], { $id: this.$id });
            var updatedModel = _.defaults(model, { $updated: Date.now() });
            updatedModel = _.extend(existingModel, updatedModel);
            delete updatedModel.$type;
            return updatedModel;
        } else {
            throw new Error('required $id is missing');
        }
    }
}

function remove(model, cb) {
    if(isDirty) {
        fetch(function(err, data){
            if(err) return cb(err);
            try {
                del.call(model);
                store(function(err){
                    cb(err);
                });
            } catch(err) {
                cb(err);
            }
        });
    } else {
        try {
            del.call(model);
            store(function(err){
                cb(err);
            });
        } catch(err) {
            cb(err);
        }
    }
    function del() {
        var self = this;
        if(this && this.$id) {
            var bucket = this.$type || 'unknown';
            buckets = _.mapObject(buckets, function(existingRecords, bucketName){
                if(bucketName === bucket) {
                    return _.reject(existingRecords, function(it){
                        return it.$id === self.$id;
                    });
                } else {
                    return existingRecords
                }
            })
        } else {
            throw new Error('required $id is missing');
        }
    }
}

module.exports = {
    config: config,
    create: create,
    read: read,
    update: update,
    delete: remove
}

function fetch(cb){
    if(storage) {
        var s3bucket = storage.split('/')[0];
        var s3key = storage.split('/')[1];
        s3.get(s3bucket, s3key, function(err, data){
            if(err) data = {};
            buckets = data;
            isDirty = false;
            return cb(null, data);
        });
    } else {
        cb(new Error('storage not configured'));
    }
}

function store(cb){
    isDirty = true;
    if(storage) {
        var s3bucket = storage.split('/')[0];
        var s3key = storage.split('/')[1];
        s3.put(s3bucket, s3key, buckets, function(err, data){
            if(err) return cb(err);
            else {
                isDirty = false;
                return cb(null, data);
            }
        });
    } else {
        cb(new Error('storage not configured'));
    }
}

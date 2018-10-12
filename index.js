'use strict';
var _ = require('underscore');
var async = require('async');
var s3 = require('./lib/aws/s3');
var uuid = require('uuid/v1');

var storage;

function config(opts) {
    opts = opts || {};
    storage = opts.storage;
}

function create(model, cb){
    fetch(function(err, data){
        if(err) return cb(err);
        let key = `$${model.$type}`;
        data[key] = data[key] || [];
        function put() {
            this.$id = uuid();
            this.$created = Date.now();
            delete this.$type;
            data[key].push(this);
        }
        put.call(model);
        store(data, function(err){
            return cb(err, model);
        });
    });
}

function read(model, cb) {
    fetch(function(err, data){
        if(err) return cb(err);
        let key = `$${model.$type}`;
        data[key] = data[key] || [];
        function get() {
            if(this && this.$id) {
                var ret = _.findWhere(data[key], {$id:this.$id});
                return cb(null, ret);
            } else {
                return cb(new Error('required $id is missing'));
            }
        }
        return get.call(model, cb);
    });
}

function update(model, cb){
    fetch(function(err, data){
        if(err) return cb(err);
        let key = `$${model.$type}`;
        let modelBucket = data[key] = data[key] || [];
        function put() {
            if(this && this.$id) {
                var existingModel = _.findWhere(modelBucket, { $id: this.$id });
                var updatedModel = _.defaults(model, { $updated: Date.now() });
                updatedModel = _.extend(existingModel, updatedModel);
                delete updatedModel.$type;
                return updatedModel;
            } else {
                throw new Error('required $id is missing');
            }
        }
        try {
            var updatedModel = put.call(model);
            store(data, function(err){
                cb(err, updatedModel);
            });
        } catch(err) {
            cb(err);
        }
    });
}

function remove(model, cb) {
    fetch(function(err, data){
        if(err) return cb(err);
        let key = `$${model.$type}`;
        data[key] = data[key] || [];
        function del() {
            var self = this;
            if(this && this.$id) {
                data[key] = _.reject(data[key], function(it){
                    return it.$id === self.$id;
                });
            } else {
                throw new Error('required $id is missing');
            }
        }
        try {
            del.call(model);
            store(data, function(err){
                cb(err);
            });
        } catch(err) {
            cb(err);
        }
    });
}

function find(model, cb) {
    fetch(function(err, data){
        if(err) return cb(err);
        let key = `$${model.$type}`;
        data[key] = data[key] || [];
        function _find() {
            if(this) {
                delete model.$type;
                var ret = _.where(data[key], model);
                return cb(null, ret);
            }
        }
        _find.call(model, cb);
    });
}

function findOne(model, cb) {
    fetch(function(err, data){
        if(err) return cb(err);
        let key = `$${model.$type}`;
        data[key] = data[key] || [];
        function _find() {
            if(this) {
                delete model.$type;
                var ret = _.findWhere(data[key], model);
                return cb(null, ret);
            }
        }
        _find.call(model, cb);
    });
}

function count(model, cb) {
    fetch(function(err, data){
        if(err) return cb(err);
        let key = `$${model.$type}`;
        data[key] = data[key] || [];
        function _count() {
            if(this) {
                delete model.$type;
                var ret = _.size(_.where(data[key], model));
                return cb(null, ret);
            }
        }
        _count.call(model, cb);
    });
}

function clear(model, cb) {
    fetch(function(err, data){
        function _clear() {
            if(this) {
                data = [];
            }
        }
        if(err) return cb(err);
        try {
            _clear.call(model);
            store(data, function(err){
                cb(err);
            });
        } catch(err) {
            cb(err);
        }
    });
}

module.exports = {
    config: config,
    create: create,
    read: read,
    update: update,
    delete: remove,
    find: find,
    findOne: findOne,
    count: count,
    clear: clear
}

function fetch(cb){
    if(storage) {
        var path = storage.split('/');
        var s3key = path.splice(-1, 1)[0];
        var s3bucket = path.join('/');
        s3.get(s3bucket, s3key, function(err, data){
            if(err) {
                console.log('fetch err:', err.message);
                data = {};
            }
            return cb(null, data);
        });
    } else {
        cb(new Error('storage not configured'));
    }
}

function store(data, cb){
    if(storage) {
        var path = storage.split('/');
        var s3key = path.splice(-1, 1)[0];
        var s3bucket = path.join('/');
        s3.put(s3bucket, s3key, data, function(err, data){
            if(err) {
                console.log('store err:', err.message);
                return cb(err);
            }
            return cb(null, data);
        });
    } else {
        return cb(new Error('storage not configured'));
    }
}

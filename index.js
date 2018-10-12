var s3 = require('./lib/aws/s3');
var uuid = require('uuid/v1');
var _ = require('underscore');

var storage;
var datastore;;
var bucket;
var isDirty = true;

function config(opts) {
    opts = opts || {};
    storage = opts.storage;
}

function create(model, cb){
    bucket = model.$type || 'unknown';
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
        this.$id = uuid();
        this.$created = Date.now();
        var bucket = model.$type || 'unknown';
        if(!datastore) datastore = [];
        delete this.$type;
        datastore.push(this);
    }
}

function read(model, cb) {
    bucket = model.$type || 'unknown';
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
            var ret = _.findWhere(datastore, {$id:this.$id});
            return cb(null, ret);
        } else {
            return cb(new Error('required $id is missing'));
        }
    }
}

function update(model, cb){
    bucket = model.$type || 'unknown';
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
            var existingModel = _.findWhere(datastore, { $id: this.$id });
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
    bucket = model.$type || 'unknown';
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
            datastore = _.reject(datastore, function(it){
                return it.$id === self.$id;
            });
        } else {
            throw new Error('required $id is missing');
        }
    }
}

function find(model, cb) {
    bucket = model.$type || 'unknown';
    if(isDirty) {
        fetch(function(err, data){
            if(err) return cb(err);
            _find.call(model, cb);
        });
    } else {
        _find.call(model, cb);
    }
    function _find() {
        if(this) {
            var bucket = this.$type || 'unknown';
            delete model.$type;
            var ret = _.where(datastore, model);
            return cb(null, ret);
        }
    }
}

function findOne(model, cb) {
    bucket = model.$type || 'unknown';
    if(isDirty) {
        fetch(function(err, data){
            if(err) return cb(err);
            _find.call(model, cb);
        });
    } else {
        _find.call(model, cb);
    }
    function _find() {
        if(this) {
            var bucket = this.$type || 'unknown';
            delete model.$type;
            var ret = _.findWhere(datastore, model);
            return cb(null, ret);
        }
    }
}

function count(model, cb) {
    bucket = model.$type || 'unknown';
    if(isDirty) {
        fetch(function(err, data){
            if(err) return cb(err);
            _count.call(model, cb);
        });
    } else {
        _count.call(model, cb);
    }
    function _count() {
        if(this) {
            var bucket = this.$type || 'unknown';
            delete model.$type;
            var ret = _.size(_.where(datastore, model));
            return cb(null, ret);
        }
    }
}

function clear(model, cb) {
    bucket = model.$type || 'unknown';
    if(isDirty) {
        fetch(function(err, data){
            if(err) return cb(err);
            try {
                _clear.call(model);
                store(function(err){
                    cb(err);
                });
            } catch(err) {
                cb(err);
            }
        });
    } else {
        try {
            _clear.call(model);
            store(function(err){
                cb(err);
            });
        } catch(err) {
            cb(err);
        }
    }
    function _clear() {
        if(this) {
            var bucket = this.$type || 'unknown';
            datastore = [];
        }
    }
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
            if(err) data = [];
            datastore = data;
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
        var path = storage.split('/');
        var s3key = path.splice(-1, 1)[0];
        var s3bucket = path.join('/');
        if(!datastore) datastore = [];
        s3.put(s3bucket, s3key, datastore, function(err, data){
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

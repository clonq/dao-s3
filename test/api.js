var should = require('chai').should();
var dao = require('daoi');
var s3DaoAdapter = require('../index');
var async = require('async');

var TEST_MODEL = {
    $type: 'user',
    name: 'joe',
    age: 23
};
var ALT_TEST_MODEL = {
    $type: 'location',
    city: 'toronto',
    country: 'canada'
};

var S3_STORAGE_FILENAME = 'dao-s3-test-bucket/test.json';
var TEST_ID;
var ALT_TEST_ID;

describe.skip("DAO-S3 direct tests", function() {
    before(function(done){
        s3DaoAdapter.config({storage: S3_STORAGE_FILENAME});
        // todo: create record without using the adapter
        dao
        .use(s3DaoAdapter)
        .on('create', function(model){
            should.exist(model);
            model.should.have.property('$id');
            model.should.have.property('name');
            model.name.should.equal(TEST_MODEL.name);
            TEST_ID = model.$id;
            done();
        })
        .on('error', function(err){
            done(err);
        })
        .create(TEST_MODEL);
    });
    after(function(done){
        require('../lib/aws/s3').del(S3_STORAGE_FILENAME.split('/')[0], S3_STORAGE_FILENAME.split('/')[1], function(err){
            done(err);
        });
    });
    it('should find all available models', function(done){
        s3DaoAdapter.find({$type: 'user'}, function(err, users){
            should.exist(users);
            users.should.be.an('array');
            users[0].should.be.an('object');
            users[0].should.have.property('$id');
            done(err);
        });
    });
    it('should find one model', function(done){
        s3DaoAdapter.findOne({$type: 'user', name: 'joe'}, function(err, user){
            should.exist(user);
            user.should.be.an('object');
            user.should.have.property('$id');
            user.should.have.property('name');
            user.name.should.equal(TEST_MODEL.name);
            done(err);
        });
    });
    it('should count existing models', function(done){
        s3DaoAdapter.count({$type: 'user', age: 23}, function(err, count){
            count.should.equal(1);
            done(err);
        });
    });
    it('should clear the bucket', function(done){
        s3DaoAdapter.clear({$type: 'user'}, function(err){
            s3DaoAdapter.count({$type: 'user'}, function(err, count){
                count.should.equal(0);
                done(err);
            });
        });
    });
});

describe("DAO-S3 API v0 compliance tests", function() {
    before(function(){
        s3DaoAdapter.config({storage: S3_STORAGE_FILENAME});
    });
    after(function(done){
        require('../lib/aws/s3').del(S3_STORAGE_FILENAME.split('/')[0], S3_STORAGE_FILENAME.split('/')[1], function(err){
            done(err);
        });
    });
    it('should create a model', function(done){
        dao
        .use(s3DaoAdapter)
        .on('create', function(model){
            should.exist(model);
            model.should.have.property('$id');
            /[0-9a-z\-]{36}/.test(model.$id).should.be.ok;
            model.should.have.property('name');
            model.name.should.equal(TEST_MODEL.name);
            TEST_ID = model.$id;
            done();
        })
        .on('error', function(err){
            done(err);
        })
        .create(TEST_MODEL);
    });
    it('should read a model', function(done){
        TEST_MODEL.$id = TEST_ID;
        TEST_MODEL.$type = 'user';
        dao
        .use(s3DaoAdapter)
        .on('read', function(model){
            should.exist(model);
            model.should.have.property('$id');
            model.should.have.property('name');
            model.name.should.equal(TEST_MODEL.name);
            done();
        })
        .on('error', function(err){
            done(err);
        })
        .read(TEST_MODEL);
    });
    it('should update a model', function(done){
        TEST_MODEL.$id = TEST_ID;
        TEST_MODEL.$type = 'user';
        TEST_MODEL.newField = 'new-value';
        dao
        .use(s3DaoAdapter)
        .on('update', function(model){
            should.exist(model);
            model.should.have.property('$id');
            model.should.have.property('newField');
            model.newField.should.equal('new-value');
            done();
        })
        .on('error', function(err){
            done(err);
        })
        .update(TEST_MODEL);
    });
    it('should delete a model', function(done){
        TEST_MODEL.$id = TEST_ID;
        TEST_MODEL.$type = 'user';
        dao
        .use(s3DaoAdapter)
        .on('delete', function(model){
            require('../lib/aws/s3').get(S3_STORAGE_FILENAME.split('/')[0], S3_STORAGE_FILENAME.split('/')[1], function(err, datastore){
                should.exist(datastore);
                datastore.should.be.an('object');
                datastore.should.have.property('$user');
                datastore.$user.should.be.an('array');
                datastore.$user.length.should.equal(0);
                done(err);
            });
        })
        .on('error', function(err){
            done(err);
        })
        .delete(TEST_MODEL);
    });
});

describe("DAO-S3 API v1 compliance tests", function() {
    before(function(done){
        s3DaoAdapter.config({storage: S3_STORAGE_FILENAME});
        // todo: create record without using the adapter
        dao
        .use(s3DaoAdapter)
        .on('create', function(model){
            should.exist(model);
            model.should.have.property('$id');
            model.should.have.property('name');
            model.name.should.equal(TEST_MODEL.name);
            TEST_ID = model.$id;
            done();
        })
        .on('error', function(err){
            done(err);
        })
        .create(TEST_MODEL);
    });
    after(function(done){
        require('../lib/aws/s3').del(S3_STORAGE_FILENAME.split('/')[0], S3_STORAGE_FILENAME.split('/')[1], function(err){
            done(err);
        });
    });
    it('should find all models', function(done){
        dao
        .use(s3DaoAdapter)
        .on('find', function(users){
            should.exist(users);
            users.should.be.an('array');
            users[0].should.be.an('object');
            users[0].should.have.property('$id');
            done();
        })
        .on('error', function(err){
            done(err);
        })
        .find({$type: 'user'});
    });
    it('should find one model', function(done){
        dao
        .use(s3DaoAdapter)
        .on('findOne', function(user){
            should.exist(user);
            user.should.be.an('object');
            user.should.have.property('$id');
            user.should.have.property('name');
            user.name.should.equal(TEST_MODEL.name);
            done();
        })
        .on('error', function(err){
            done(err);
        })
        .findOne({$type: 'user', age: 23});
    });
    it('should count existing models', function(done){
        dao
        .use(s3DaoAdapter)
        .on('count', function(count){
            count.should.equal(1);
            done();
        })
        .on('error', function(err){
            done(err);
        })
        .count({$type: 'user'});
    });
    it('should clear the storage', function(done){
        dao
        .use(s3DaoAdapter)
        .on('clear', function(count){
            s3DaoAdapter.count({$type: 'user'}, function(err, count){
                count.should.equal(0);
                done(err);
            });
        })
        .on('error', function(err){
            done(err);
        })
        .clear({$type: 'user'});
    });
});

describe.skip("DAO-S3 API mixed models tests", function() {
    before(function(){
        s3DaoAdapter.config({storage: S3_STORAGE_FILENAME});
    });
    after(function(done){
        require('../lib/aws/s3').del(S3_STORAGE_FILENAME.split('/')[0], S3_STORAGE_FILENAME.split('/')[1], done);
    });
    it('should create two different models', function(done){
        async.series({
            main: cb => {
                dao
                .use(s3DaoAdapter)
                .on('create', function(model){
                    should.exist(model);
                    model.should.have.property('$id');
                    /[0-9a-z\-]{36}/.test(model.$id).should.be.ok;
                    model.should.have.property('name');
                    model.name.should.equal(TEST_MODEL.name);
                    TEST_ID = model.$id;
                    return cb(null, model);
                })
                .on('error', function(err){
                    return cb(err);
                })
                .create(TEST_MODEL);
            },
            alt: cb => {
                dao
                .use(s3DaoAdapter)
                .on('create', function(model){
                    should.exist(model);
                    model.should.have.property('$id');
                    /[0-9a-z\-]{36}/.test(model.$id).should.be.ok;
                    model.should.have.property('city');
                    model.city.should.equal(ALT_TEST_MODEL.city);
                    ALT_TEST_ID = model.$id;
                    return cb(null, model);
                })
                .on('error', function(err){
                    return cb(err);
                })
                .create(ALT_TEST_MODEL);
            }
        }, (err, results) => {
            return done(err);
        });
    });
    it('should read multiple models in parallel', function(done){
        async.parallel({
            user1: cb => {
                TEST_MODEL.$id = TEST_ID;
                TEST_MODEL.$type = 'user';
                dao
                .use(s3DaoAdapter)
                .on('read', function(model){
                    should.exist(model);
                    model.should.have.property('$id');
                    model.should.have.property('name');
                    model.name.should.equal(TEST_MODEL.name);
                    cb(null, model);
                })
                .on('error', function(err){
                    cb(err);
                })
                .read(TEST_MODEL);
            },
            location1: cb => {
                ALT_TEST_MODEL.$id = ALT_TEST_ID;
                ALT_TEST_MODEL.$type = 'location';
                dao
                .use(s3DaoAdapter)
                .on('read', function(model){
                    should.exist(model);
                    model.should.have.property('$id');
                    model.should.have.property('city');
                    model.city.should.equal(ALT_TEST_MODEL.city);
                    cb(null, model);
                })
                .on('error', function(err){
                    cb(err);
                })
                .read(ALT_TEST_MODEL);
            },
            location2: cb => {
                ALT_TEST_MODEL.$id = ALT_TEST_ID;
                ALT_TEST_MODEL.$type = 'location';
                dao
                .use(s3DaoAdapter)
                .on('read', function(model){
                    should.exist(model);
                    model.should.have.property('$id');
                    model.should.have.property('city');
                    model.city.should.equal(ALT_TEST_MODEL.city);
                    cb(null, model);
                })
                .on('error', function(err){
                    cb(err);
                })
                .read(ALT_TEST_MODEL);
            },
            user2: cb => {
                TEST_MODEL.$id = TEST_ID;
                TEST_MODEL.$type = 'user';
                dao
                .use(s3DaoAdapter)
                .on('read', function(model){
                    should.exist(model);
                    model.should.have.property('$id');
                    model.should.have.property('name');
                    model.name.should.equal(TEST_MODEL.name);
                    cb(null, model);
                })
                .on('error', function(err){
                    cb(err);
                })
                .read(TEST_MODEL);
            }
        }, (err, results) => {
            return done(err);
        });
    });
});

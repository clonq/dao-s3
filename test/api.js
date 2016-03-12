var should = require('chai').should();
var dao = require('daoi');
var s3DaoAdapter = require('../index');

var TEST_MODEL = {
    $type: 'user',
    name: 'joe',
    age: 23
};

var S3_STORAGE_FILENAME = 'dao-s3-test-bucket/users.json';
var TEST_ID;

describe.only("DAO-S3 direct tests", function() {
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
            users.should.be.an.array;
            users[0].should.be.an.object;
            users[0].should.have.property('$id');
            done(err);
        });
    });
    it('should find one model', function(done){
        s3DaoAdapter.findOne({$type: 'user', name: 'joe'}, function(err, user){
            should.exist(user);
            user.should.be.an.object;
            user.should.have.property('$id');
            user.should.have.property('name');
            user.name.should.equal(TEST_MODEL.name);
            done(err);
        });
    });
});

describe("DAO-S3 API v1 compliance tests", function() {
    before(function(){
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
    it('should find all models without registering a model', function(done){
        dao
        .use(s3DaoAdapter)
        .on('find', function(model){
            should.exist(model);
            model.should.have.property('$id');
            model.should.have.property('name');
            model.name.should.equal(TEST_MODEL.name);
            done();
        })
        .on('error', function(err){
            done(err);
        })
        .find({$type: 'user'});
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
            require('../lib/aws/s3').get(S3_STORAGE_FILENAME.split('/')[0], S3_STORAGE_FILENAME.split('/')[1], function(err, buckets){
                should.exist(buckets);
                buckets.should.have.property('user');
                buckets.user.should.be.an.array;
                buckets.user.length.should.equal(0);
                done(err);
            });
        })
        .on('error', function(err){
            done(err);
        })
        .delete(TEST_MODEL);
    });
});

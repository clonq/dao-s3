var should = require('chai').should();
var dao = require('daoi');
var s3DaoAdapter = require('../index');

var TEST_MODEL = {
    $type: 'user',
    name: 'joe',
    age: 23
};
var TEST_ID;
var S3_STORAGE_FILENAME = 'dao-s3-test-bucket/users.json';

describe("DAO-S3 API tests", function() {
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
        .config({storage:S3_STORAGE_FILENAME})
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
        .config({storage:S3_STORAGE_FILENAME})
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
        .config({storage:S3_STORAGE_FILENAME})
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
        .config({storage:S3_STORAGE_FILENAME})
        .delete(TEST_MODEL);
    });
});

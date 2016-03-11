var AWS = require('aws-sdk');
var commonAwsConfig = {apiVersion: 'latest', region: 'us-east-1'};
var s3 = new AWS.S3(commonAwsConfig);

function put(bucket, key, data, cb) {
    var s3Params = {
        Bucket: bucket,
        Key: key,
        Body: JSON.stringify(data)
    };
    s3.putObject(s3Params, cb);
}

function get(bucket, key, cb) {
    var s3Params = {
        Bucket: bucket,
        Key: key
    };
    s3.getObject(s3Params, function(err, data){
        if(err) return cb(err);
        else {
            if(data && data.Body) return cb(null, JSON.parse(Buffer(data.Body).toString()));
            else return cb(null, {});
        }
    });
}

function del(bucket, key, cb) {
    var s3Params = {
        Bucket: bucket,
        Key: key
    };
    s3.deleteObject(s3Params, cb);
}

module.exports = {
    put: put,
    get: get,
    del: del
}

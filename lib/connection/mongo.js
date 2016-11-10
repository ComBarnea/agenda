var MongoClient = require('mongodb').MongoClient,
    Db = require('mongodb').Db;



var MongoConnection = module.exports = function (url, collection, options, cb){
    MongoClient.connect(url, options, function ( error, db ){
        if (error) {
            if (cb) {
                cb(error, null);
            } else {
                throw error;
            }

            return;
        }
        cb(null, db)
    });
};

MongoConnection.prototype.db_init = function (mdb, cb) {
    var collection = mdb.collection(collection || 'agendaJobs');

    collection.createIndexes([{
            "key": {"name" : 1, "priority" : -1, "lockedAt" : 1, "nextRunAt" : 1, "disabled" : 1},
            "name": "findAndLockNextJobIndex1"
        }, {
            "key": {"name" : 1, "lockedAt" : 1, "priority" : -1, "nextRunAt" : 1, "disabled" : 1},
            "name": "findAndLockNextJobIndex2"
        }],
        function( err, result ){
            handleLegacyCreateIndex(collection, err, result, cb)
        });
};

function handleLegacyCreateIndex(collection, err, result, cb){
    if(err && err.message !== 'no such cmd: createIndexes'){
        cb(err);
    } else {
        // Looks like a mongo.version < 2.4.x
        err = null;
        collection.ensureIndex(
            {"name": 1, "priority": -1, "lockedAt": 1, "nextRunAt": 1, "disabled": 1},
            {name: "findAndLockNextJobIndex1"}
        );
        collection.ensureIndex(
            {"name": 1, "lockedAt": 1, "priority": -1, "nextRunAt": 1, "disabled": 1},
            {name: "findAndLockNextJobIndex2"}
        );
    }
    if (cb){
        cb(null, collection);
    } else {
        throw new Error('Error while ensuring index, connection error?');
    }
}
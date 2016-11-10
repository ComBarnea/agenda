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
/**
 * Initiate mongo collection
 * @param {Object} mdb
 * @param {String} [collectionName]
 * @param {String} [collection]
 * @param cb
 */
MongoConnection.prototype.db_init = function (mdb, collectionName, collection, cb) {
    var collection = collection || mdb.collection(collectionName);

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

/**
 * get all jobs by query
 * @param collection
 * @param query
 * @param cb
 */
MongoConnection.prototype.getJobs = function (collection, query, cb) {
    collection.find(query).toArray( function(error, result) {
        cb(error, result)
    });
};


MongoConnection.prototype.cancel = function(collection, query, cb) {
    collection.deleteMany( query, function( error, result ){
        if (cb) {
            cb( error, result && result.result ? result.result.n : undefined );
        }
    });
};


/**
 *
 * @param collection
 * @param query
 * @param sort
 * @param update
 * @param options
 * @param cb
 */
MongoConnection.prototype.update = function(collection, query, sort, update, cb) {
    collection.findAndModify(query, sort, update, options, function( error, result ){
        if (cb) {
            cb( error, result && result.result ? result.result.n : undefined );
        }
    });
};

/**
 *
 * @param collection
 * @param query
 * @param sort
 * @param update
 * @param options
 * @param cb
 */
MongoConnection.prototype.updateMany = function(collection, query, sort, update, cb) {
    collection.updateMany(query, update, function( error, result ){
        if (cb) {
            cb( error, result && result.result ? result.result.n : undefined );
        }
    });
};

/**
 *
 * @param collection
 * @param data
 * @param cb
 */
MongoConnection.prototype.insertOne = function(collection, data, cb) {
    collection.insertOne(data, function( error, result ){
        if (cb) {
            cb( error, result && result.result ? result.result.n : undefined );
        }
    });
};

/**
 *
 * @param mdb
 * @param collection
 * @param jobName
 * @param definition
 * @param _nextScanAt
 * @param createJob
 * @param agenda
 * @param cb
 */
MongoConnection.prototype.findAndLockNextJob = function(mdb, collection, jobName, definition, _nextScanAt, createJob, agenda, cb) {
    var now = new Date();
    var lockDeadline = new Date(Date.now().valueOf() - definition.lockLifetime);

    // Don't try and access Mongo Db if we've lost connection to it. Also see clibu_automation.js db.on.close code. NF 29/04/2015
    // Trying to resolve crash on Dev PC when it resumes from sleep.
    var s = mdb.s || mdb.db.s;
    if (s.topology.connections().length === 0) {
        cb(new Error( 'No MongoDB Connection'));
    } else {
        this._collection.findAndModify(
            {
                $or: [
                    {name: jobName, lockedAt: null, nextRunAt: {$lte: _nextScanAt}, disabled: { $ne: true }},
                    {name: jobName, lockedAt: {$exists: false}, nextRunAt: {$lte: _nextScanAt}, disabled: { $ne: true }},
                    {name: jobName, lockedAt: {$lte: lockDeadline}, disabled: { $ne: true }}
                ]
            },
            {'priority': -1},  // sort
            {$set: {lockedAt: now}},  // Doc
            {'new': true},  // options
            function (err, result) {
                var job;
                if (!err && result.value) {
                    job = createJob(agenda, result.value);
                }
                cb(err, job);
            }
        );
    }
};


/**
 *
 * @param collection
 * @param err
 * @param result
 * @param cb
 */
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



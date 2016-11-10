/**
 *
 *
 * in this file collection can reference a mongoDB collection or SQL table.
 */

var mongoConnection = require('./mongo');
// var sqlConnection   = require('./sql');

var ConnectionDriver = module.exports = function () {
    if(!(this instanceof ConnectionDriver)) return new ConnectionDriver();
    return this;
};

/**
 * load basic data for this instance
 * @param {String} type - type of db can be mongoDB or sqlDB
 * @param {String} [url] - url to connect to
 * @param {String} [collectionName='agendaJobs'] - collection/table name defaults to agendaJobs
 * @param {Object} [options] - options to pass to db connection
 * @param {Object} [db] - instance of db to use
 * @param {Object} [collection] - instance of collection/table to use
 */
ConnectionDriver.prototype.init = function (type, url, collectionName, options, db, collection) {
    var self = this;
    self._url = null;
    self._collectionName = null;
    self._options = null;
    self._type = null;
    self._connection = null;



    if(type === 'mongoDB') {
        self._type = type;
        self._connection = new mongoConnection();

        if(db && collection) {
            self._db = db;

            self._collection = collection;
            return;
        }

        if (!url.match(/^mongodb:\/\/.*/)) {
            url = 'mongodb://' + url;
        }

        self._url = url;
    } else {
        throw new Error('Wrong Database type.');
    }

    self._collectionName = collectionName || 'agendaJobs';
    self._options = options || {};
};

/**
 * connect to the db
 * @param cb
 */
ConnectionDriver.prototype.connect = function (cb) {
    var self = this;

    self._connection.connect(self._url, self._collectionName, self._options, function (err, db) {
        if(err) return cb(err);

        self._db = db;
        cb(null, db);
    });
};

/**
 * initiate db collection/tabe
 * @param cb
 */
ConnectionDriver.prototype.db_init = function (cb) {
    var self = this;

    if(!self._db) throw new Error('No db found, did you connect?')

    self._connection.db_init(self._db, self._collectionName, self._collection, function (err, collection) {
        if(err) return cb(err);

        self._collection = collection;
        cb(null, collection);
    });
};


/**
 * single interface for agenda to work with
 * abstraction for agenda queries
 */

/**
 * get all jobs by query
 * @param query
 * @param cb
 */
ConnectionDriver.prototype.getJobs = function (query, cb) {
    var self = this;
    self._connection.getJobs(self._collection ,query, function (err, jobs) {
        if(err) return cb(err);

        cb(null, jobs);
    });
};
/**
 *
 * @param query
 * @param cb
 */
ConnectionDriver.prototype.cancel = function(query, cb) {
    var self = this;
    self._connection.cancel(self._collection, query, function (err, jobs) {
        if(err) return cb(err);

        cb(null, jobs);
    });
};
/**
 *
 * @param query
 * @param sort
 * @param update
 * @param options
 * @param cb
 */
ConnectionDriver.prototype.update = function(query, sort, update, options, cb) {
    var self = this;
    self._connection.update(self._collection, query, sort, update, options, function (err, jobs) {
        if(err) return cb(err);

        cb(null, jobs);
    });
};

/**
 *
 * @param query
 * @param sort
 * @param update
 * @param options
 * @param cb
 */
ConnectionDriver.prototype.updateMany = function(query, update, cb) {
    var self = this;
    self._connection.updateMany(self._collection, query, update, function (err, jobs) {
        if(err) return cb(err);

        cb(null, jobs);
    });
};


/**
 *
 * @param data
 * @param cb
 */
ConnectionDriver.prototype.insertOne = function(data, cb) {
    var self = this;
    self._connection.update(self._collection, data, function (err, jobs) {
        if(err) return cb(err);

        cb(null, jobs);
    });
};

/**
 *
 * @param jobName
 * @param definition
 * @param _nextScanAt
 * @param createJob
 * @param agenda
 * @param cb
 */
ConnectionDriver.prototype.findAndLockNextJob = function(jobName, definition, _nextScanAt, createJob, agenda, cb) {
    var self = this;
    self._connection.findAndLockNextJob(self._db, self._collection, jobName, definition, _nextScanAt, createJob, agenda, function (err, job) {
        if(err) return cb(err);

        cb(null, job);
    });
};

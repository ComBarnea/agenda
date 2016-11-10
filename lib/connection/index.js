/**
 *
 *
 * in this file collection can reference a mongoDB collection or SQL table.
 */

var mongoConnection = require('./mongo');
// var sqlConnection   = require('./sql');

var ConnectionDriver = module.exports = function (type, url, collectionName, options, db) {
    var self = this;
    self._url = null;
    self._collectionName = null;
    self._options = null;
    self._type = null;
    self._connection = null;



    if(type === 'mongoDB') {
        self._type = type;
        self._connection = mongoConnection;

        if(db) {
            self._db = db;

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


ConnectionDriver.prototype.connect = function (cb) {
    var self = this;

    self._connection(self._url, self._collectionName, self._options, function (err, db) {
        if(err) return cb(err);

        self._db = db;
        cb(null, db);
    });
};

ConnectionDriver.prototype.db_init = function (cb) {
    var self = this;
    if(!self._db) throw new Error('No db found, did you connect?')

    self._connection.db_init(self._db, function (err, collection) {
        if(err) return cb(err);

        self._collection = collection;
        cb(null, collection);
    });
};


ConnectionDriver.prototype.db = function () {

};
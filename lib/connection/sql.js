var Sequelize = require('sequelize');



var SqlConnection = module.exports = function () {
    if (!(this instanceof SqlConnection)) return new SqlConnection();

    return this;
};

/**
 *
 * @param url
 * @param collection
 * @param options
 * @param cb
 */
SqlConnection.prototype.connect = function (url, db, collection, options, cb) {
    var sequelize;
    // connect based on url
    // check options is empty object
    if(url && !db && !(Object.getOwnPropertyNames(options).length > 0)) {
        sequelize = new Sequelize(url, {logging: false});
    } else {
        sequelize = new Sequelize(db, options.username, options.password, {
            host: url,
            dialect: options.dialect,
            logging: false
        });
    }
    cb(null, sequelize);
};

/**
 * Initiate mongo collection
 * @param {Object} mdb
 * @param {String} [collectionName]
 * @param {String} [collection]
 * @param cb
 */
SqlConnection.prototype.db_init = function (sequelize, collectionName, collection, cb) {
    // capitalize collection name
    var capCollection = collectionName.replace(/\b\w/g, function(l){ return l.toUpperCase() });

    var db = { models: {}};
    db.models[capCollection] = sequelize.define(collectionName, {
        _id: {
            type: Sequelize.DataTypes.INTEGER,
            primaryKey: true,
            autoIncrement: true
        },
        name: Sequelize.DataTypes.STRING,
        type: Sequelize.DataTypes.STRING,
        data: Sequelize.DataTypes.STRING,
        priority: Sequelize.DataTypes.INTEGER,
        repeatInterval: Sequelize.DataTypes.STRING,
        repeatTimezone: Sequelize.DataTypes.STRING,
        lastModifiedBy: Sequelize.DataTypes.STRING,
        nextRunAt: Sequelize.DataTypes.DATE,
        lockedAt: Sequelize.DataTypes.DATE,
        lastRunAt: Sequelize.DataTypes.DATE,
        lastFinishedAt: Sequelize.DataTypes.DATE,
    }, {
        timestamps: false
    });

    db.models[capCollection].sync({force: true})
    .then(function () {
        cb(null, db.models[capCollection]);
    })
    .catch(function (err) {
        cb(err);
    });
};


/**
 * get all jobs by query
 * @param collection
 * @param query
 * @param cb
 */
SqlConnection.prototype.getJobs = function (collection, query, cb) {
    collection.findAll(query)
    .then(function(result) {
        cb(null, buildReturnObject(result));
    })
    .catch(function(err) {
        cb(err);
    });
};


SqlConnection.prototype.cancel = function(collection, query, cb) {
    collection.destroy(query)
    .then(function(result) {
        cb(null, result);
    })
    .catch(function(err) {
        cb(err);
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
SqlConnection.prototype.update = function(collection, query, sort, update, options, cb) {
    var updateData = update ? updateBuilder(update.update, update.set, update.setOnInsert) : {};
    var updateOptions = options || {};

    if(query) delete query.disabled;
    updateOptions.where = query;

    collection.update(updateData, updateOptions)
    .then(function(result) {
        if(result[0] === 0){ // need to create
            return collection.create(updateData, options)
                .then(function(newItem) {
                    cb(null, buildReturnObject(newItem || undefined));
                });
        } else { // fetch by query... not very good, TODO: find better solution
            var newQuery = fixQueryAfterUpdate(query, updateData); // passing object with values reduced
            return collection.findOne({where: newQuery})
                .then(function(item) {
                    cb(null, buildReturnObject(item || undefined));
                });
        }
    })
    .catch(function(err) {
        cb(err);
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
SqlConnection.prototype.updateMany = function(collection, query, sort, update, cb) {
    var updateData = update ? updateBuilder(update.update, update.set, update.setOnInsert) : {};
    var updateOptions =  {};

    updateOptions.where = query;

    collection.update(updateData, updateOptions)
    .then(function(result) {
        return collection.findAll({where: query})
            .then(function (items) {
                cb(null, buildReturnObject(items));
            })
    })
    .catch(function(err) {
        cb(err);
    });
};

/**
 *
 * @param collection
 * @param data
 * @param cb
 */
SqlConnection.prototype.insertOne = function(collection, data, cb) {
    collection.create(data)
    .then(function(item) {
        cb( null, buildReturnObject(item || undefined));
    }).catch(function (err) {
        cb(err);
    })
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
SqlConnection.prototype.findAndLockNextJob = function(mdb, collection, jobName, definition, _nextScanAt, createJob, agenda, cb) {
    var now = new Date();
    var lockDeadline = new Date(Date.now().valueOf() - definition.lockLifetime);

    collection.update({
            lockedAt: now
        },
        {
            where: {
                $or: [
                    {name: jobName, lockedAt: null, nextRunAt: {$lte: _nextScanAt}},
                    {name: jobName, lockedAt: {$lte: lockDeadline}}
                ]
            }
    }).then(function(updated) {
        return collection.findOne({
            where: {
                $or: [
                    {name: jobName, lockedAt: null, nextRunAt: {$lte: _nextScanAt}},
                    {name: jobName, lockedAt: {$lte: lockDeadline}}
                ]
            }
        }).then(function(item) {
            var job;
            if(item) {
                job = createJob(agenda, item);
            }

            cb(null, buildReturnObject(job));
        });
    }).catch(function(err) {
        cb(err);
    });
};

/**
 *
 * @param update
 * @param set
 * @param setOnInsert
 */
function updateBuilder(update, set, setOnInsert) {
    var newUpdate = {};
    Object.assign(newUpdate, update,set, setOnInsert)

    return newUpdate;
}

/**
 * fix issue with the presence of dataValues
 * @param dataToReturn
 */
function buildReturnObject(dataToReturn) {
    if(!dataToReturn) return undefined;
    var fixData =[];

    if(Array.isArray(dataToReturn)) {
        for (var i = 0; i < dataToReturn.length; i++) {
            _fix(dataToReturn[i]);
        }

        return fixData;
    } else {
        _fix(dataToReturn);

        return fixData[0];
    }

    function _fix(data) {
        var dataObj = {};
        if(data && data.attrs) {
            dataObj = data.get();
        } else if(data) {
            dataObj = data.get();
        }
        fixData.push(dataObj)
    }
}

/**
 * get the original find query and update function
 * check if any of original query params where updated and replace with these values
 * @param query
 * @param update
 */
function fixQueryAfterUpdate(query, update) {
    var reg$ = /\$/g;

    // iterate over update keys
    Object.keys(update).forEach(function(key) {
        // skip key with $ in the name
        if(!reg$.test(key)) {
            if(key in query) {
                query[key] = update[key];
            }
        }
    });

    return query;
}
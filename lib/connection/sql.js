var Sequelize = require('sequelize');
var _         = require('lodash');

var util = require('util');

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
        data: Sequelize.DataTypes.JSONB,
        priority: Sequelize.DataTypes.INTEGER,
        repeatInterval: Sequelize.DataTypes.STRING,
        repeatTimezone: Sequelize.DataTypes.STRING,
        lastModifiedBy: Sequelize.DataTypes.STRING,
        nextRunAt: Sequelize.DataTypes.DATE,
        lockedAt: Sequelize.DataTypes.DATE,
        lastRunAt: Sequelize.DataTypes.DATE,
        lastFinishedAt: Sequelize.DataTypes.DATE,
    }, {
        timestamps: false,
        indexes: [
            {
                fields: ['data'],
                using: 'gin'
            }
        ]
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
    collection.findAll({where: fixQueryObj(query)})
    .then(function(result) {
        if(cb) cb(null, buildReturnObject(result));
    })
    .catch(function(err) {
        if(cb) cb(err);
    });
};

/**
 *
 * @param collection
 * @param query
 * @param cb
 */
SqlConnection.prototype.cancel = function(collection, query, cb) {
    // console.log('query', query);
    console.log('query', util.inspect(fixQueryObj(query), false, null));

//    console.log(fixQueryObj(query));
    collection.destroy({where: fixQueryObj(query)})
    .then(function(result) {
        if(cb) cb(null, result[0]);
    })
    .catch(function(err) {
        console.log(err);
        if(cb) cb(err);
    });
};



SqlConnection.prototype.purge = function(collection, names, cb) {
    var query = {name: {$notIn: names}};
    this.cancel(collection, query, cb);
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
    var updateData = fixEmptyUpdate(update ? updateBuilder(update.update, update.set) : {}, query);
    var updateOptions = options || {};

    delete updateOptions.new;
    var doUpsert = updateOptions.upsert;
    delete updateOptions.upsert;

    updateOptions.where = fixQueryObj(query);
    // console.log('options', updateOptions);
    // console.log('data', updateData);
    // console.log('Odata', update);
    // console.log('upsert', doUpsert);

    collection.update(updateData, updateOptions)
    .then(function(result) {
        var isNotUpdated = checkIfUpdated(result);
        
        // console.log('isNotUpdated', isNotUpdated);
        if(isNotUpdated && doUpsert) { // need to create
            // console.log('lets create');
            // set new updateData without arbitrary query key
            updateData = update ? updateBuilder(update.update, update.set) : {};
            if(update.setOnInsert) Object.assign(updateData, update.setOnInsert);
            return collection.create(updateData, options)
                .then(function(newItem) {
                    if(cb) cb(null, buildReturnObject(newItem || undefined));
                });
        } else {
            var newQuery = !isNotUpdated ? fixQueryAfterUpdate(query, updateData , false) : query; // passing object with values reduced
            // console.log('newQuery', newQuery);
            return collection.findOne({where: newQuery})
                .then(function(item) {
                    if(cb) cb(null, buildReturnObject(item || undefined));
                });
        }
    })
    .catch(function(err) {
        if(cb) cb(err);
    });
};

/**
 *
 * @param collection
 * @param query
 * @param update
 * @param cb
 */
SqlConnection.prototype.updateMany = function(collection, query, update, cb) {
    var updateData = update ? updateBuilder(update.update, update.set) : {};
    var updateOptions =  {};

    updateOptions.where = fixQueryObj(query);

    collection.update(updateData, updateOptions)
    .then(function(result) {
        return collection.findAll({where: fixQueryAfterUpdate(query, updateData)})
            .then(function (items) {
                if(cb) cb(null, items[0] || undefined);
            })
    })
    .catch(function(err) {
        if(cb) cb(err);
    });
};

/**
 *
 * @param collection
 * @param data
 * @param cb
 */
SqlConnection.prototype.insertOne = function(collection, data, cb) {
    var updateData = data ? updateBuilder(data, null, null) : {};

    collection.create(updateData)
    .then(function(item) {
        if(cb) cb( null, buildReturnObject(item || undefined));
    }).catch(function (err) {
        if(cb) cb(err);
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
                job = createJob(agenda, buildReturnObject(item));
            }

            if(cb) cb(null, job);
        });
    }).catch(function(err) {
        if(cb) cb(err);
    });
};

/**
 * build an update / data object based on our keywords
 * @param {Object} [update={}]
 * @param {Object} [set={}]
 */
function updateBuilder(update, set) {
    update = update ? update : {};
    set = set ? set : {};

    var newUpdate = {};

    Object.assign(newUpdate, update, set);
    newUpdate = removeFix(newUpdate);

    return newUpdate;

    function removeFix(data) {

        return data;
    }
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
function fixQueryAfterUpdate(query, update, applyFixQueryOnj) {
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

    if(applyFixQueryOnj) {
        return fixQueryObj(query);
    }
    return query;
}

/**
 * add $like where data.keys are to enable these options again
 * @param originalQuery
 */
function fixQueryObj(originalQuery) {
    if(!originalQuery) return {};
    var regTest = /^data/;

    var newQuery = {};

    Object.keys(originalQuery).forEach(function(key) {
       if(regTest.test(key)) { // starts with data

           _.set(newQuery, key, originalQuery[key]);
       } else {
           newQuery[key] = originalQuery[key];

       }
    });

    if('data' in newQuery) {
        newQuery.data = {
            $contains: JSON.stringify(newQuery.data)
        }
    }

    delete newQuery.disabled;

    return newQuery;
}

/**
 * 
 * @param result
 * @return {boolean}
 */
function checkIfUpdated(result) {
    if(result[0] === 0) {
        return true;
    } else if(isNaN(result[0])) {
        return true;
    }
    
    return false;
}
/**
 *
 * @param update
 * @param query
 * @return {*}
 */
function fixEmptyUpdate(update, query) {
    if(!_.isEmpty(update)) return update;
    var newUpdate = {};

    if('_id' in query) {
        newUpdate._id = query._id;
    } else if('name' in query) {
        newUpdate.name = query.name;
    } else if('type' in query) {
        newUpdate.type = query.type;
    } else if('priority' in query) {
        newUpdate.priority = query.priority;
    } else if('repeatInterval' in query) {
        newUpdate.repeatInterval = query.repeatInterval;
    } else if('repeatTimezone' in query) {
        newUpdate.repeatTimezone = query.repeatTimezone;
    } else if('data' in query) {
        newUpdate.data = query.data;
    } else {
        return {};
    }

    return newUpdate;
}
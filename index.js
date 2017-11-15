/*
 *   js-data-bigtable
 */

'use strict';

var jsData        = require('js-data');
var jsDataAdapter = require('js-data-adapter');

var defineProperty = function (obj, key, value) {
    if (key in obj) {
        Object.defineProperty(obj, key, {
            value: value,
            enumerable: true,
            configurable: true,
            writable: true
        });
    } else {
        obj[key] = value;
    }

    return obj;
};


var DEFAULTS = {

    /**
     * Convert ObjectIDs to strings when pulling records out of the database.
     *
     * @name BigTableAdapter#translateId
     * @type {boolean}
     * @default true
     */
    translateId: true,

    /**
     * Convert fields of record from database that are ObjectIDs to strings
     *
     * @name BigTableAdapter#translateObjectIDs
     * @type {Boolean}
     * @default false
     */
    translateObjectIDs: false
};

var COUNT_OPTS_DEFAULTS = {};
var FIND_OPTS_DEFAULTS = {};




/**
 * BigTableAdapter class.
 *
 * @example
 * // Use Container instead of DataStore on the server
 * import { Container } from 'js-data';
 * import BigTableAdapter from 'js-data-BigTable';
 *
 * // Create a store to hold your Mappers
 * const store = new Container({
 *   mapperDefaults: {
 *     // BigTable uses "_id" as the primary key
 *     idAttribute: '_id'
 *   }
 * });
 *
 * // Create an instance of BigTableAdapter with default settings
 * const adapter = new BigTableAdapter();
 *
 * // Mappers in "store" will use the BigTable adapter by default
 * store.registerAdapter('BigTable', adapter, { default: true });
 *
 * // Create a Mapper that maps to a "user" collection
 * store.defineMapper('user');
 *
 * @class BigTableAdapter
 * @extends Adapter
 * @param {object} [options] Configuration options.
 *
 *     options = {
 *                  promise     : promise,
 *                  projectId   : project id,
 *                  keyFilename : json file,
 *                  instance    : instance name,
 *              }
 *
 * @param {boolean} [opts.debug=false] See {@link Adapter#debug}.
 * @param {object} [opts.countOpts] See {@link BigTableAdapter#countOpts}.
 * @param {object} [opts.findOpts] See {@link BigTableAdapter#findOpts}.
 * @param {object} [opts.findOneOpts] See {@link BigTableAdapter#findOneOpts}.
 * @param {object} [opts.insertOpts] See {@link BigTableAdapter#insertOpts}.
 * @param {object} [opts.insertManyOpts] See {@link BigTableAdapter#insertManyOpts}.
 * @param {boolean} [opts.raw=false] See {@link Adapter#raw}.
 * @param {object} [opts.removeOpts] See {@link BigTableAdapter#removeOpts}.
 * @param {boolean} [opts.translateId=true] See {@link BigTableAdapter#translateId}.
 * @param {boolean} [opts.translateObjectIDs=false] See {@link BigTableAdapter#translateObjectIDs}.
 * @param {object} [opts.updateOpts] See {@link BigTableAdapter#updateOpts}.
 */
function BigTableAdapter(options) {

    if('undefined' === typeof options.promise) {
        return false;
    }

    var _this = this;

    jsData.utils.classCallCheck(this, BigTableAdapter);

    jsData.utils.fillIn(options, DEFAULTS);

    // Setup non-enumerable properties
    Object.defineProperties(this, {
        /**
         * A Promise that resolves to a reference to the BigTable client being used by
         * this adapter.
         *
         * @name BigTableAdapter#client
         * @type {Promise}
         */
        client: {
            writable: true,
            value: undefined
        },

        _db: {
            writable: true,
            value: undefined
        }
    });

    if('undefined' === typeof options.instance) {
        // TODO emit error
        return false;
    }

    var BigTable = require('@google-cloud/bigtable')(options);

    jsDataAdapter.Adapter.call(this, options);

    /**
     * Default options to pass to collection#count.
     *
     * @name BigTableAdapter#countOpts
     * @type {object}
     * @default {}
     */
    this.countOpts || (this.countOpts = {});
    jsData.utils.fillIn(this.countOpts, COUNT_OPTS_DEFAULTS);

    /**
     * Default options to pass to collection#find.
     *
     * @name BigTableAdapter#findOpts
     * @type {object}
     * @default {}
     */
    this.findOpts || (this.findOpts = {});
    jsData.utils.fillIn(this.findOpts, FIND_OPTS_DEFAULTS);

    this.client =  new jsData.utils.Promise(
        function (resolve, reject) {
            _this._db = BigTable.instance(options.instance);
            resolve(_this._db);
            reject('ERROR WHILE INITIALIZING BIGTABLE');
        });
}

jsDataAdapter.Adapter.extend({

    constructor: BigTableAdapter,

    /**
     * Return a Promise that resolves to a reference to the BigTable client being
     * used by this adapter.
     *
     * Useful when you need to do anything custom with the BigTable client library.
     *
     * @method BigTableAdapter#getClient
     * @return {object} BigTable client.
     */
    getClient: function getClient() {
        return this.client;
    },


    /**
     * Map filtering params in a selection query to BigTable a filtering object.
     *
     * Handles the following:
     *
     * - where
     *   - and bunch of filtering operators
     *
     * @method BigTableAdapter#getQuery
     * @return {object}
     */
    getQuery: function getQuery(mapper, query) {

        query = jsData.utils.plainCopy(query || {});

        query.where || (query.where = {});

        jsData.utils.forOwn(query, function (config, keyword) {
            if (jsDataAdapter.reserved.indexOf(keyword) === -1) {
                if (jsData.utils.isObject(config)) {
                    query.where[keyword] = config;
                } else {
                    query.where[keyword] = {
                        '==': config
                    };
                }
                delete query[keyword];
            }
        });

        var BigTableQuery = {};

        if (Object.keys(query.where).length !== 0) {

            jsData.utils.forOwn(query.where, function (criteria, field) {
                if (!jsData.utils.isObject(criteria)) {
                    query.where[field] = {
                        '==': criteria
                    };
                }

                jsData.utils.forOwn(criteria, function (v, op) {
                    if (op === '==' || op === '===' || op === 'contains') {
                        BigTableQuery[field] = v;
                    } else if (op === '!=' || op === '!==' || op === 'notContains') {
                        BigTableQuery[field] = BigTableQuery[field] || {};
                        BigTableQuery[field].$ne = v;
                    } else if (op === '>') {
                        BigTableQuery[field] = BigTableQuery[field] || {};
                        BigTableQuery[field].$gt = v;
                    } else if (op === '>=') {
                        BigTableQuery[field] = BigTableQuery[field] || {};
                        BigTableQuery[field].$gte = v;
                    } else if (op === '<') {
                        BigTableQuery[field] = BigTableQuery[field] || {};
                        BigTableQuery[field].$lt = v;
                    } else if (op === '<=') {
                        BigTableQuery[field] = BigTableQuery[field] || {};
                        BigTableQuery[field].$lte = v;
                    } else if (op === 'in') {
                        BigTableQuery[field] = BigTableQuery[field] || {};
                        BigTableQuery[field].$in = v;
                    } else if (op === 'notIn') {
                        BigTableQuery[field] = BigTableQuery[field] || {};
                        BigTableQuery[field].$nin = v;
                    } else if (op === '|==' || op === '|===' || op === '|contains') {
                        BigTableQuery.$or = BigTableQuery.$or || [];
                        var orEqQuery = {};
                        orEqQuery[field] = v;
                        BigTableQuery.$or.push(orEqQuery);
                    } else if (op === '|!=' || op === '|!==' || op === '|notContains') {
                        BigTableQuery.$or = BigTableQuery.$or || [];
                        var orNeQuery = {};
                        orNeQuery[field] = {
                            '$ne': v
                        };
                        BigTableQuery.$or.push(orNeQuery);
                    } else if (op === '|>') {
                        BigTableQuery.$or = BigTableQuery.$or || [];
                        var orGtQuery = {};
                        orGtQuery[field] = {
                            '$gt': v
                        };
                        BigTableQuery.$or.push(orGtQuery);
                    } else if (op === '|>=') {
                        BigTableQuery.$or = BigTableQuery.$or || [];
                        var orGteQuery = {};
                        orGteQuery[field] = {
                            '$gte': v
                        };
                        BigTableQuery.$or.push(orGteQuery);
                    } else if (op === '|<') {
                        BigTableQuery.$or = BigTableQuery.$or || [];
                        var orLtQuery = {};
                        orLtQuery[field] = {
                            '$lt': v
                        };
                        BigTableQuery.$or.push(orLtQuery);
                    } else if (op === '|<=') {
                        BigTableQuery.$or = BigTableQuery.$or || [];
                        var orLteQuery = {};
                        orLteQuery[field] = {
                            '$lte': v
                        };
                        BigTableQuery.$or.push(orLteQuery);
                    } else if (op === '|in') {
                        BigTableQuery.$or = BigTableQuery.$or || [];
                        var orInQuery = {};
                        orInQuery[field] = {
                            '$in': v
                        };
                        BigTableQuery.$or.push(orInQuery);
                    } else if (op === '|notIn') {
                        BigTableQuery.$or = BigTableQuery.$or || [];
                        var orNinQuery = {};
                        orNinQuery[field] = {
                            '$nin': v
                        };
                        BigTableQuery.$or.push(orNinQuery);
                    }
                });


            });
        }

        return BigTableQuery;
    },


    /**
     * Map non-filtering params in a selection query to BigTable query options.
     *
     * Handles the following:
     *
     * - limit
     * - skip/offset
     * - orderBy/sort
     *
     * @method BigTableAdapter#getQueryOptions
     * @return {object}
     */
    getQueryOptions: function getQueryOptions(mapper, query) {
        query = jsData.utils.plainCopy(query || {});
        query.orderBy = query.orderBy || query.sort;
        query.skip = query.skip || query.offset;

        var queryOptions = {};

        if (query.orderBy) {
            if (jsData.utils.isString(query.orderBy)) {
                query.orderBy = [[query.orderBy, 'asc']];
            }
            for (var i = 0; i < query.orderBy.length; i++) {
                if (jsData.utils.isString(query.orderBy[i])) {
                    query.orderBy[i] = [query.orderBy[i], 'asc'];
                }
            }
            queryOptions.sort = query.orderBy;
        }

        if (query.skip) {
            queryOptions.skip = +query.skip;
        }

        if (query.limit) {
            queryOptions.limit = +query.limit;
        }

        return queryOptions;
    },


    /**
     * Retrieve the number of records that match the selection query.
     *
     * @method BigTableAdapter#count
     * @param {object} mapper The mapper.
     * @param {object} query Selection query.
     * @param {object} [opts] Configuration options.
     * @param {object} [opts.countOpts] Options to pass to collection#count.
     * @param {boolean} [opts.raw=false] Whether to return a more detailed
     * response object.
     * @param {string[]} [opts.with=[]] Relations to eager load.
     * @return {Promise}
     */

    /**
     * Retrieve the records that match the selection query. Internal method used
     * by Adapter#count.
     *
     * @method BigTableAdapter#_count
     * @private
     * @param {object} mapper The mapper.
     * @param {object} query Selection query.
     * @param {object} [opts] Configuration options.
     * @return {Promise}
     */
    _count: function _count(mapper, query, opts) {
        var _this2 = this;

        opts || (opts = {});

        return this._run(function (client, success, failure) {
            var collectionId = _this2._getCollectionId(mapper, opts);
            var countOpts = _this2.getOpt('countOpts', opts);
            jsData.utils.fillIn(countOpts, _this2.getQueryOptions(mapper, query));

            /*
            var mongoQuery = _this2.getQuery(mapper, query);

            client.collection(collectionId).count(mongoQuery, countOpts, function (err, count) {
                return err ? failure(err) : success([count, {}]);
            });
            */
            return success([count, {count: 123}]);
        });
    },

    
    /**
     * Retrieve the record with the given primary key.
     *
     * @method BigTableAdapter#find
     * @param {object} mapper The mapper.
     * @param {(string|number)} id Primary key of the record to retrieve.
     * @param {object} [opts] Configuration options.
     * @param {string|string[]|object} [opts.fields] Select a subset of fields to be returned.
     * @param {object} [opts.findOneOpts] Options to pass to collection#findOne.
     * @param {boolean} [opts.raw=false] Whether to return a more detailed
     * response object.
     * @param {string[]} [opts.with=[]] Relations to eager load.
     * @return {Promise}
     */

    /**
     * Retrieve the record with the given primary key. Internal method used by
     * Adapter#find.
     *
     * @method BigTableAdapter#_find
     * @private
     * @param {object} mapper The mapper.
     * @param {(string|number)} id Primary key of the record to retrieve.
     * @param {object} [opts] Configuration options.
     * @param {string|string[]|object} [opts.fields] Select a subset of fields to be returned.
     * @return {Promise}
     */
    _find: function _find(mapper, id, opts) {

        console.log('[MAPPER]',mapper);
        console.log('[ID]', id);

        var _this7 = this;

        opts || (opts = {});
        opts.with || (opts.with = []);

        return this.
                _run(function (client, success, failure) {

                    var collectionId = _this7._getCollectionId(mapper, opts);

                    try {
                        var row = client
                                    .table(collectionId)
                                    .row(id);

                        row.get(function(err) {
                            if(err) {
                                failure(err);
                            } else {
                                success(row.data);
                            }
                        });

                    } catch (error) {
                        console.error(error);
                    }
                })
                .then(function (record) {
                    if (record) {
                        _this7._translateObjectIDs(record, opts);
                    } else {
                        record = undefined;
                    }
                    return [record, {}];
                });
    },


    /**
     * Retrieve the records that match the selection query.
     *
     * @method BigTableAdapter#findAll
     * @param {object} mapper The mapper.
     * @param {object} query Selection query.
     * @param {object} [opts] Configuration options.
     * @param {string|string[]|object} [opts.fields] Select a subset of fields to be returned.
     * @param {object} [opts.findOpts] Options to pass to collection#find.
     * @param {boolean} [opts.raw=false] Whether to return a more detailed
     * response object.
     * @param {string[]} [opts.with=[]] Relations to eager load.
     * @return {Promise}
     */

    /**
     *
     * This method is not recommended for large datasets as it will buffer all rows
     * before returning the results.
     *
     * Retrieve the records that match the selection query. Internal method used
     * by Adapter#findAll.
     *
     * @method BigTableAdapter#_findAll
     * @private
     * @param {object} mapper The mapper.
     * @param {object} query Selection query.
     * @param {object} [opts] Configuration options.
     * @param {string|string[]|object} [opts.fields] Select a subset of fields to be returned.
     * @return {Promise}
     */
    _findAll: function _findAll(mapper, query, opts) {

        console.log('[opts]', opts);

        var _this8 = this;

        opts || (opts = {});

        return this
            ._run(function (client, success, failure) {

                var collectionId = _this8._getCollectionId(mapper, opts);

                var findOpts = _this8.getOpt('findOpts', opts);
                jsData.utils.fillIn(findOpts, _this8.getQueryOptions(mapper, query));
                findOpts.fields = _this8._getFields(mapper, opts);

                console.log('[QUERY]', query, Object.keys(query).length);
                var filter = Object.keys(query).length ? { filter: [ _this8.getQuery(mapper, query) ] } : {};
                console.log('[FILTER]',filter);

                client
                    .table(collectionId)
                    .getRows(filter)
                    .then(
                        function(data) {
                            success(Promise.all( data[0].map(function(object) {
                                return { "id":object["id"], "data": object["data"] }
                            })))
                        },
                        function(error) {
                            failure(error);
                        }
                    )
            })
            .then(function (records) {
                _this8._translateObjectIDs(records, opts);
                return [records, {}];
            });
    },

    _run: function _run(cb) {
        var _this9 = this;
        if (this._db) {
            // Use the cached db object
            return new jsData.utils.Promise(function (resolve, reject) {
                cb(_this9._db, resolve, reject);
            });
        }
        return this
                .getClient()
                .then(function (client) {
                    return new jsData.utils.Promise(function (resolve, reject) {
                        cb(client, resolve, reject);
                    });
                });
    },

    _translateObjectIDs: function _translateObjectIDs(r, opts) {
        opts || (opts = {});
        if (this.getOpt('translateObjectIDs', opts)) {
            this._translateFieldObjectIDs(r);
        } else if (this.getOpt('translateId', opts)) {
            this._translateId(r);
        }
        return r;
    },

    /**
     * Translate ObjectIDs to strings.
     *
     * @method BigTableAdapter#_translateId
     * @return {*}
     */
    _translateId: function _translateId(r) {
        if (jsData.utils.isArray(r)) {
            r.forEach(function (_r) {
                var __id = _r._id ? _r._id.toString() : _r._id;
                _r._id = typeof __id === 'string' ? __id : _r._id;
            });
        } else if (jsData.utils.isObject(r)) {
            var __id = r._id ? r._id.toString() : r._id;
            r._id = typeof __id === 'string' ? __id : r._id;
        }
        return r;
    },

    /**
     * Translate ObjectIDs to strings.
     *
     * @method BigTableAdapter#_translateFieldObjectIDs
     * @return {*}
     */
    _translateFieldObjectIDs: function _translateFieldObjectIDs(r) {
        var _checkFields = function _checkFields(r) {
            for (var field in r) {
                if (r[field]._bsontype === 'ObjectID') {
                    r[field] = typeof r[field].toString() === 'string' ? r[field].toString() : r[field];
                }
            }
        };
        if (jsData.utils.isArray(r)) {
            r.forEach(function (_r) {
                _checkFields(_r);
            });
        } else if (jsData.utils.isObject(r)) {
            _checkFields(r);
        }
        return r;
    },

    _getCollectionId: function _getCollectionId(mapper, opts) {
        opts || (opts = {});
        return opts.table || opts.collection || mapper.table || mapper.collection || '';
    },

    _getFields: function _getFields(mapper, opts) {
        opts || (opts = {});
        if (jsData.utils.isString(opts.fields)) {
            opts.fields = defineProperty({}, opts.fields, 1);
        } else if (jsData.utils.isArray(opts.fields)) {
            var fields = {};
            opts.fields.forEach(function (field) {
                fields[field] = 1;
            });
            return fields;
        }
        return opts.fields;
    },
});




var version = {
    full: '1.0.1',
    major: 1,
    minor: 0,
    patch: 1
};

exports.BigTableAdapter = BigTableAdapter;
exports.version = version;




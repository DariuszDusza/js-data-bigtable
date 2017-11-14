/*
 *   js-data-bigtable
 */

'use strict';

var jsData        = require('js-data');
var jsDataAdapter = require('js-data-adapter');

var DEFAULTS = {};

/*
 *  options
 *          promise     : promise,
 *          projectId   : project id,
 *          keyFilename : json file,
 *          instance    : instance name,
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

        var _this8 = this;

        opts || (opts = {});

        return this
            ._run(function (client, success, failure) {

                var collectionId = _this8._getCollectionId(mapper, opts);

                var findOpts = _this8.getOpt('findOpts', opts) || {};
                jsData.utils.fillIn(findOpts, _this8.getQueryOptions(mapper, query));
                findOpts.fields = _this8._getFields(mapper, opts);

                var BigTableQuery = _this8.getQuery(mapper, query);

                var filter = [BigTableQuery];

                console.log('[FILTER]',filter);

                client
                    .table(collectionId)
                    .getRows({
                        filter: filter
                    })
                    .then(
                        function(data) {
                            //console.log(data[0]);
                            success(
                                data[0]
                                    .forEach(function (element, index) {
                                        console.log('[element][id]', element.id);
                                        console.log('[element][data]', element.data);
                                        //return {id: element['id'], data: element['data']};
                                    })
                            );
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
    full: '1.0.0',
    major: 1,
    minor: 0,
    patch: 0
};

exports.BigTableAdapter = BigTableAdapter;
exports.version = version;




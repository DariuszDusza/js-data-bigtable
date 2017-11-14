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
 *          projectId   : 'trackthis-179709',
 *          keyFilename : 'TrackThis-bigtable-360451317527.json',
 *          instance    : 'dev-test',
 *          table       : 'stats-payments'
 */
function BigTableAdapter(options) {

    var _this = this;

    jsData.utils.classCallCheck(this, BigTableAdapter);

    jsData.utils.fillIn(options, DEFAULTS);

    // Setup non-enumerable properties
    Object.defineProperties(this, {
        /**
         * A Promise that resolves to a reference to the MongoDB client being used by
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

    var BigTable = require('@google-cloud/bigtable')(options);

    // init instance if given
    if('undefined' !== typeof options.instance) {
        this.instance = new BigTable.instance(options.instance);
    }

    // init table if given
    if('undefined' !== typeof options.table) {
        this.table = this.instance.table(options.table);
    }

    jsDataAdapter.Adapter.call(this, options);

    this.client = new jsData.utils.Promise(function (resolve, reject) {


        /*
        mongodb.MongoClient.connect(opts.uri, opts.mongoDriverOpts, function (err, db) {
            if (err) {
                return reject(err);
            }
            _this._db = db;
            resolve(db);
        });
        */
    });
}

jsDataAdapter.Adapter.extend({

    constructor: BigTableAdapter,





    /**
     * Retrieve the records that match the selection query.
     *
     * @method MongoDBAdapter#findAll
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
     * Retrieve the records that match the selection query. Internal method used
     * by Adapter#findAll.
     *
     * @method MongoDBAdapter#_findAll
     * @private
     * @param {object} mapper The mapper.
     * @param {object} query Selection query.
     * @param {object} [opts] Configuration options.
     * @param {string|string[]|object} [opts.fields] Select a subset of fields to be returned.
     * @return {Promise}
     */
    _findAll: function _findAll(mapper, query, opts) {

        console.log('[FIND ALL]','[MAPPER]',mapper,'[QUERY]',query,'[OPTS]',opts);
        var _this8 = this;

        opts || (opts = {});

        console.log(BigTableAdapter);

        /*return new Promise(function (resolve, reject) {
            _table.getRows().then(
                function(data) {
                    resolve(data[0]);
                },
                function(error) {
                    reject();
                }
            );
        });*/
    }
});

























var version = {
    full: '1.0.0',
    major: 1,
    minor: 0,
    patch: 0
};

exports.BigTableAdapter = BigTableAdapter;
exports.version = version;



var s = {

    BigTable : null,
    table    : null,

    BigTableAdapter: (function(options) {

        /*
         *  options
         *          promise     : promise,
         *          projectId   : 'trackthis-179709',
         *          keyFilename : 'TrackThis-bigtable-360451317527.json',
         *          instance    : 'dev-test',
         *          table       : 'stats-payments'
         */
        this.BigTable = require('@google-cloud/bigtable')(options);

        // init instance if given
        if('undefined' !== typeof options.instance) {
            this.BigTable.instance(options.instance);
        }

        // init tabke if given
        if('undefined' !== typeof options.table) {
            this.table = options.table;
        }

        return this.BigTable;
    }),

    // set instance
    setBigTableInstance: (function(instance) {
        this.BigTable.instance(instance);
    }),

    findAll: (function(t) {
        console.log('findAll');
        var table = this.BigTable.instance.table(t || this.table);
        return new Promise(function (resolve, reject) {
            table.getRows().then(
                function(data) {
                    resolve(data[0]);
                },
                function(error) {
                    reject();
                }
            );
        });
    }),
};

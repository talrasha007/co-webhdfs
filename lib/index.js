var _ = require('codash'),
    request = require('co-request').defaults({
        agentOptions: { keepAlive: true }
    }),
    rawRequest = require('request').defaults({
        agentOptions: { keepAlive: true }
    });

var strStandbyException = 'StandbyException';

var WebHdfsClient = module.exports = function (options) {
    this.options = _.defaults(options || {}, {
        user: 'hadoop',
        namenode_port: 50070,
        namenode_host: 'localhost',
        path_prefix: '/webhdfs/v1'
    });

    var port = this.options.namenode_port,
        prefix = this.options.path_prefix;

    var hosts = this.options.namenode_hosts || this.options.namenode_host;
    if (!_.isArray(hosts)) hosts = [hosts];

    this.base_urls = _.map(hosts, function (h) {
        return 'http://' + h + ':' + port + prefix;
    });
};

_.extend(WebHdfsClient.prototype, {
    del: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('delete', path, hdfsoptions, requestoptions);
        return (yield* this._request('del', args)).boolean;
    },

    listStatus: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('liststatus', path, hdfsoptions, requestoptions);
        return (yield* this._request('get', args)).FileStatuses.FileStatus;
    },

    getFileStatus: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('getfilestatus', path, hdfsoptions, requestoptions);
        return (yield* this._request('get', args)).FileStatus;
    },

    getContentSummary: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('getcontentsummary', path, hdfsoptions, requestoptions);
        return (yield* this._request('get', args)).ContentSummary;
    },

    getFileChecksum: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('getfilechecksum', path, hdfsoptions, requestoptions);
        return (yield* this._request('get', args)).FileChecksum;
    },

    getHomeDirectory: function *(hdfsoptions, requestoptions) {
        var args = this._makeReqArg('gethomedirectory', '', hdfsoptions, requestoptions);
        return (yield* this._request('get', args)).Path;
    },

    rename: function *(path, destination, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('rename', path, _.defauts(hdfsoptions || {}, { destination: destination }), requestoptions);
        return (yield* this._request('put', args)).boolean;
    },

    mkdirs: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('mkdirs', path, hdfsoptions, requestoptions);
        return (yield* this._request('put', args)).Path;
    },

    open: function *(path, hdfsoptions, requestoptions) {
        return yield* this._open(path, hdfsoptions, requestoptions, false);
    },

    create: function *(path, data, hdfsoptions, requestoptions) {
        if (data === true) data = 'true';
        return yield* this._create(path, hdfsoptions, requestoptions, data);
    },

    append: function *(path, data, hdfsoptions, requestoptions) {
        if (data === true) data = 'true';
        return yield* this._append(path, hdfsoptions, requestoptions, data);
    },

    createReadStream: function *(path, hdfsoptions, requestoptions) {
        return yield* this._open(path, hdfsoptions, requestoptions, true);
    },

    createAppendStream: function *(path, hdfsoptions, requestoptions) {
        return yield* this._append(path, hdfsoptions, requestoptions, true);
    },

    createWriteStream: function *(path, hdfsoptions, requestoptions) {
        return yield* this._create(path, hdfsoptions, requestoptions, true);
    },

    _open: function *(path, hdfsoptions, requestoptions, streamMode) {
        try { yield* this.listStatus('/__a_not_exists_path____'); } // Set active name nodes.
        catch (e) { }

        var args = this._makeReqArg('open', path, hdfsoptions, _.defaults(requestoptions || {}, { followRedirect: false }));

        var res = yield request.get(args[0]);
        if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
        if (res.statusCode  !== 307) throw new Error('Status 307 expected');

        args = args[0];
        args.uri = res.headers.location;
        delete args.qs;
        delete args.json;

        return streamMode ? rawRequest.get(args) : yield* this._request('get', [args]);
    },

    _append: function *(path, hdfsoptions, requestoptions, streamMode) {
        try { yield* this.listStatus('/__a_not_exists_path____'); } // Set active name nodes.
        catch (e) { }

        var args = this._makeReqArg('append', path, hdfsoptions, _.defaults(requestoptions || {}, { followRedirect: false }));

        var res = yield request.post(args[0]);
        if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
        if (res.statusCode  !== 307) throw new Error('Status 307 expected');

        args = args[0];
        args.uri = res.headers.location;
        delete args.qs;
        delete args.json;

        if (streamMode === true) {
            return rawRequest.post(args);
        } else {
            args.body = streamMode;
            return yield* this._request('post', [args]);
        }
    },

    _create: function *(path, hdfsoptions, requestoptions, streamMode) {
        try { yield* this.listStatus('/__a_not_exists_path____'); } // Set active name nodes.
        catch (e) { }

        var args = this._makeReqArg('create', path, hdfsoptions, _.defaults(requestoptions || {}, { followRedirect: false }));

        var res = yield request.put(args[0]);
        if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
        if (res.statusCode  !== 307) throw new Error('Status 307 expected');

        args = args[0];
        args.uri = res.headers.location;
        delete args.qs;
        delete args.json;

        if (streamMode === true) {
            return rawRequest.put(args);
        } else {
            args.body = streamMode;
            return yield* this._request('put', [args]);
        }
    },

    _request: function *(op, args, expectStatus) {
        var lastErr;

        for (var i = 0; i < args.length; i++) {
            try {
                var res = yield request[op](args[i]);
            } catch (e) {
                lastErr = new Error(e);
                continue ;
            }

            if (res.body && res.body.RemoteException) {
                if (res.body.RemoteException.exception !== strStandbyException) {
                    this._setActive(i);
                    throw new Error(res.body.RemoteException.message);
                }
            } else if (expectStatus && expectStatus !== res.statusCode) {
                throw new Error('WebHdfs: Status code ' + expectStatus + ' expected.');
            } else if (res.statusCode !== 200 && res.statusCode !== 201) {
                var err;
                try {
                    err = JSON.parse(res.body).RemoteException.message;
                } catch (e) {
                    err = new Error('WebHdfs: Invalid status code.');
                }

                throw err;
            } else {
                this._setActive(i);
                return res.body;
            }
        }

        throw lastErr || new Error('WebHdfs: All namenodes are standby.');
    },

    _setActive: function (i) {
        if (i > 0) {
            var u0 = this.base_urls[0];
            this.base_urls[0] = this.base_urls[i];
            this.base_urls[i] = u0;
        }
    },

    _makeReqArg: function (op, path, hdfsoptions, requestoptions) {
        var me = this;

        return _.map(me.base_urls, function (baseurl) {
            return _.defaults(requestoptions || {}, {
                json: true,
                uri: baseurl + path,
                qs: _.defaults(hdfsoptions || {}, {
                    op: op,
                    'user.name': me.options.user
                })
            });
        });
    }
});

var _ = require('codash'),
    request = require('co-request').defaults({
        agentOptions: { keepAlive: true }
    }),
    rawRequest = require('request').defaults({
        agentOptions: { keepAlive: true }
    });

var WebHdfsClient = module.exports = function (options) {
    this.options = _.defaults(options || {}, {
        user: 'hadoop',
        namenode_port: 50070,
        namenode_host: 'localhost',
        path_prefix: '/webhdfs/v1'
    });

    this.base_url = 'http://' + this.options.namenode_host + ':' + this.options.namenode_port + this.options.path_prefix;
};

function checkResponse(res) {
    if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
    if (res.statusCode !== 200 && res.statusCode !== 201) {
        var err;
        try {
            err = JSON.parse(res.body).RemoteException.message;
        } catch (e) {
            err = new Error('Invalid status code.');
        }
        throw err;
    }

    return res.body;
}

_.extend(WebHdfsClient.prototype, {
    del: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('delete', path, hdfsoptions, requestoptions);
        return checkResponse(yield request.del(args)).boolean;
    },

    listStatus: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('liststatus', path, hdfsoptions, requestoptions);
        return checkResponse(yield request.get(args)).FileStatuses.FileStatus;
    },

    getFileStatus: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('getfilestatus', path, hdfsoptions, requestoptions);
        return checkResponse(yield request.get(args)).FileStatus;
    },

    getContentSummary: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('getcontentsummary', path, hdfsoptions, requestoptions);
        return checkResponse(yield request.get(args)).ContentSummary;
    },

    getFileChecksum: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('getfilechecksum', path, hdfsoptions, requestoptions);
        return checkResponse(yield request.get(args)).FileChecksum;
    },

    getHomeDirectory: function *(hdfsoptions, requestoptions) {
        var args = this._makeReqArg('gethomedirectory', '', hdfsoptions, requestoptions);
        return checkResponse(yield request.get(args)).Path;
    },

    rename: function *(path, destination, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('rename', path, _.defauts(hdfsoptions || {}, { destination: destination }), requestoptions);
        return checkResponse(yield request.put(args)).boolean;
    },

    mkdirs: function *(path, hdfsoptions, requestoptions) {
        var args = this._makeReqArg('mkdirs', path, hdfsoptions, requestoptions);
        return checkResponse(yield request.put(args)).Path;
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
        var args = this._makeReqArg('open', path, hdfsoptions, _.defaults(requestoptions || {}, { followRedirect: false }));

        var res = yield request.get(args);
        if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
        if (res.statusCode  !== 307) throw new Error('Status 307 expected');
        args.uri = res.headers.location;
        delete args.qs;
        delete args.json;

        return streamMode ? rawRequest.get(args) : checkResponse(yield request.get(args));
    },

    _append: function *(path, hdfsoptions, requestoptions, streamMode) {
        var args = this._makeReqArg('append', path, hdfsoptions, _.defaults(requestoptions || {}, { followRedirect: false }));

        var res = yield request.post(args);
        if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
        if (res.statusCode  !== 307) throw new Error('Status 307 expected');
        args.uri = res.headers.location;
        delete args.qs;
        delete args.json;

        if (streamMode === true) {
            return rawRequest.post(args);
        } else {
            args.body = streamMode;
            return checkResponse(yield request.post(args));
        }
    },

    _create: function *(path, hdfsoptions, requestoptions, streamMode) {
        var args = this._makeReqArg('create', path, hdfsoptions, _.defaults(requestoptions || {}, { followRedirect: false }));

        var res = yield request.put(args);
        if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
        if (res.statusCode  !== 307) throw new Error('Status 307 expected');
        args.uri = res.headers.location;
        delete args.qs;
        delete args.json;

        if (streamMode === true) {
            return rawRequest.put(args);
        } else {
            args.body = streamMode;
            return checkResponse(yield request.put(args));
        }
    },

    _makeReqArg: function (op, path, hdfsoptions, requestoptions) {
        return _.defaults(requestoptions || {}, {
            json: true,
            uri: this.base_url + path,
            qs: _.defaults(hdfsoptions || {}, {
                op: op,
                'user.name': this.options.user
            })
        });
    }
});

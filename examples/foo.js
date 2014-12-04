var _ = require('codash'),
    co = require('co'),
    LineReader = require('co-stream').LineReader,
    Writer = require('co-stream').Writer,
    WebHdfsClient = require('../');

co(function *() {
    var client = new WebHdfsClient({ namenode_hosts: ['10.10.0.141', '10.10.0.140'] });

    var home = yield* client.getHomeDirectory();
    //var files = yield* client.listStatus(home);
    //console.log(_.map(files, function (f) { return f.pathSuffix }));

    yield* client.mkdirs(home + '/tmp');

    var filename = home + '/tmp/foo.txt';

    // Data style.
    yield* client.del(filename);
    yield* client.create(filename, 'foooooo\n', { overwrite: true });
    yield* client.append(filename, 'barr\n');
    console.log(yield* client.open(filename));

    // Stream style.
    var stream = new Writer(yield* client.createWriteStream(filename, { overwrite: true }));
    yield stream.writeline('xxxxxxx');
    yield _.sleep(100);
    yield stream.writeline('gggggg');
    yield stream.end();
    yield _.sleep(1000);

    var reader = new LineReader(yield* client.createReadStream(filename));
    for (var line; line = yield* reader.read();) {
        console.log('line: ', line);
    }
}).then(function () {
    console.log('finished...');
}, function (err) {
    console.log(err);
    console.log(err.stack);
});

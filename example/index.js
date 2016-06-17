var bunyan = require('bunyan'),
	path = require('path'),
	PrettyStream = require('bunyan-pretty-stream');

var BunyanDynamo = require(path.resolve(__dirname, '../lib/index.js'));

var bunyanDynamoOptions = {
	"aws": {
        "profile": "livio"
    },
    "tableName": "myAppLogs",
    "tableHashKey": "id",
    "tableHashType": BunyanDynamo.TYPE_STRING
};

var bunyanOptions = {
	name: "MyAppLogger",
	serializers: bunyan.stdSerializers,
    streams: [{
    	level: 'info',
    	stream: new BunyanDynamo(bunyanDynamoOptions),
    	type: 'raw'
    }, {
    	level: 'info',
    	stream: new PrettyStream()
    }]
}

var log = bunyan.createLogger(bunyanOptions);

log.info("It worked!");
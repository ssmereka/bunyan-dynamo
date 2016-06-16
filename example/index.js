var bunyan = require('bunyan'),
	DynamoStream = require('./dynamoStream.js');

var bunyanDynamoOptions = {
	"aws": {
        "profile": "livio"
    },
    "tableName": "myAppLogs",
    "tableHashKey": "id",
    "tableHashType": DynamoStream.TYPE_STRING
};

var bunyanOptions = {
	name: "MyAppLogger",
	serializers: bunyan.stdSerializers,
    stream: {
    	level: 'info',
    	stream: new DynamoStream(options),
    	type: 'raw'
    }
}

var log = bunyan.createLogger(bunyanOptions);

log.info("It worked!");
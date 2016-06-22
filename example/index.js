var bunyan = require('bunyan'),
	path = require('path'),
	PrettyStream = require('bunyan-pretty-stream');

var BunyanDynamo = require(path.resolve(__dirname, '../lib/index.js'));

var bunyanDynamoOptions = {
	"aws": {
    //"accessKeyId": "MyAccessKeyId",
    //"apiVersion": "2012-08-10",
    //"maxRetries": 15,
    //"profile": "default",
    //"region": "us-east-1",
    //"secretAccessKey": "MySecretAccessKey"
  },
  //"batchSize": 25,
  //"debug": false,
  //"sendInterval": 5000, // 5 Seconds
  //"tableName": "MyTableName",
  //"tableHashKey": "id",
  //"tableHashType": BunyanDynamo.DYNAMO_TYPE_STRING,
  //"tableRangeKey": "time",
  //"tableRangeType": BunyanDynamo.DYNAMO_TYPE_NUMBER,
  //"tableReadCapacity": 5,
  //"tableWriteCapacity": 5,
  //"trace": false
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
};

var log = bunyan.createLogger(bunyanOptions);

log.info("Logging to DynamoDB and to console.");
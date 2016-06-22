# Bunyan-dynamo
A <a href="https://github.com/trentm/node-bunyan#streams-introduction" target="_blank">Bunyan stream</a> to Amazon's <a href="https://aws.amazon.com/dynamodb/" target="_blank">DynamoDB</a>, allowing you to store your application's logs in Amazon's document storage.

<a href="https://nodei.co/npm/bunyan-dynamo/" target="_blank"><img src="https://nodei.co/npm/bunyan-dynamo.png?downloads=true&downloadRank=true" /></a>

<a href="https://david-dm.org/ssmereka/bunyan-dynamo" target="_blank"><img src="https://david-dm.org/ssmereka/bunyan-dynamo.svg" /></a> <a href="https://gratipay.com/ScottSmereka/" target="_blank"><img src="http://img.shields.io/gratipay/ScottSmereka.svg" /></a>

## Getting Started

Install bunyan-dynamo using npm and save it as a dependency.

```javascript
npm install bunyan-dynamo --save
```

Use bunyan-dynamo like any other Bunyan stream.

```javascript
var bunyan = require('bunyan'),
	BunyanDynamo = require('bunyan-dynamo');

var bunyanDynamoOptions = {
	"aws": {										// Accepts all AWS SDK options.
		"accessKeyId": "MyAccessKeyId",				// AWS access key ID.
        "secretAccessKey": "MySecretAccessKey"		// AWS secret access key.
    },
    "tableName": "myAppLogs",						// Name of the database table.
    "tableHashKey": "id",							// Partial key for database.
    "tableHashType": BunyanDynamo.TYPE_STRING 		// Hash key data type.
}

var bunyanOptions = {
	name: "MyAppLogger",							// Name of the bunyan logger.
	serializers: bunyan.stdSerializers,				// Bunyan serializers.
	level: 'info',									// Bunyan log level.
	type: 'raw',									// Bunyan type.
    stream: new BunyanDynamo(bunyanDynamoOptions) 	// Bunyan-Dynamo stream.
}

var log = bunyan.createLogger(bunyanOptions);

log.info("Logging to DynamoDB.");
```

Here is what the record created in DynamoDB looks like.
<img src="http://i.imgur.com/XvqwlDI.png"></img>

### Time
Time is stored as a number in milliseconds since January 1, 1970, 00:00:00 UTC (zulu time).  Amazon's SDK may return this value as a string, so use the following to convert time into an object.

```javascript
var time = "1466190023000";           // This is the time value returned from DynamoDB using amazon's SDK.

console.log(new Date(Number(time)));  // Fri Jun 17 2016 15:00:23 GMT-0400 (EDT)
```

## Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **aws** | Object | ```{}``` | Options passed directly into the aws-sdk for DynamoDB. |
| **aws.accessKeyId** | String | ```undefined``` | Your AWS access key ID. |
| **aws.apiVersion** | String | ```2012-08-10``` | The <a href="http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html" target="_blank">AWS API version of DynamoDB</a> to use. |
| **aws.maxRetries** | Number | ```15``` | Maximum amount of retries to attempt with a request to AWS. |
| **aws.profile** | String | ```default``` | <a href="http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html#Using_Profiles_with_the_SDK" target="_blank">AWS credential profile</a> to use. |
| **aws.region** | Number | ```us-east-1``` | AWS region to send service requests to. |
| **aws.secretAccessKey** | String | ```undefined``` | Your AWS secret access key. |
| **batchSize** | Number | ```25``` | Number of log messages to send at a time to the DynamoDB service. |
| **debug** | Boolean | ```false``` | When enabled, additional log messages will be displayed and configurations used to help debug the module. |
| **enableHostname** | Boolean | ```true``` | When enabled, the hostname will be included as an attribute in each item saved to the DynamoDB table. |
| **sendInterval** | Number | ```5000``` | How often, in milliseconds, to send log messages to the DynamoDB service.  Default send interval in debug and trace mode is ```1000```. |
| **tableName** | String | ```APP_NAME,HOSTNAME,PORT``` | Name of the database table.  Must be unique. |
| **tableHashKey** | String | ```id``` | Name of the partial key for the database table. |
| **tableHashType** | String | ```S``` | Data type of the hash key.  (```S``` stands for String) |
| **tableRangeKey** | String | ```time``` | Name of the sort key for the database table. |
| **tableRangeType** | String | ```N``` | Data type of the range key. (```N``` stands for Number) |
| **tableReadCapacity** | Number | ```5``` | AWS read capacity for the table. |
| **tableWriteCapacity** | Number | ```5``` | AWS write capacity for the table. |
| **trace** | Boolean | ```false``` | When enabled, debug mode and trace messages will be displayed and additional configurations used to help debug the module. |

## Environment Variables
The module can take advantage of some optional environment variables.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **APP_NAME** | String | ```undefined``` | Name of the application.  Used to uniquely generate a table name, if a table name is not already defined in the options object. |
| **PORT** | String | ```undefined``` | Port the application is listening on.  Used to uniquely generate a table name, if a table name is not already defined in the options object. |

## AWS Credentials
Credentials can either be passed in as options or configured any other way allowed by the [amazon sdk](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html)
"use strict";

var AWS = require('aws-sdk'),
  moment = require('moment'),
  os = require('os'),
  uuid = require('uuid');

var DEBUG = false,
  TRACE = false;

var buildTableName = function () {
  var tableName = "";

  if(process.env.APP_NAME) {
    tableName += process.env.APP_NAME + "_";
  }

  if(os.hostname()) {
    tableName += os.hostname();
  } else {
    tableName += uuid.v4();
  }

  if(process.env.PORT) {
    tableName += "_" + process.env.PORT;
  }

  return tableName;
};

const DYNAMO_TYPE_ARRAY_BINARY = "BS",
  DYNAMO_TYPE_ARRAY_MAP = "L",
  DYNAMO_TYPE_ARRAY_NUMBER = "NS",
  DYNAMO_TYPE_ARRAY_STRING = "SS",
  DYNAMO_TYPE_BINARY = "B",
  DYNAMO_TYPE_BOOLEAN = "BOOL",
  DYNAMO_TYPE_MAP = "M",
  DYNAMO_TYPE_NULL = "NULL",
  DYNAMO_TYPE_NUMBER = "N",
  DYNAMO_TYPE_STRING = "S";

const DEFAULT_AWS_API_VERSION = "2012-08-10",
  DEFAULT_AWS_MAX_RETRIES = 15,
  //DEFAULT_AWS_PROFILE = "default",
  DEFAULT_AWS_REGION = "us-east-1",
  DEFAULT_HOSTNAME_ENABLE = true,
  //DEFAULT_LOG_LEVEL = "30",                       // Default to the "Info" level.  https://github.com/trentm/node-bunyan#levels
  DEFAULT_TABLE_HASH_KEY = "id",
  DEFAULT_TABLE_HASH_TYPE = DYNAMO_TYPE_STRING,
  DEFAULT_TABLE_RANGE_KEY = "time",
  DEFAULT_TABLE_RANGE_TYPE = DYNAMO_TYPE_NUMBER,
  DEFAULT_TABLE_READ_CAPACITY = 5,
  DEFAULT_TABLE_WRITE_CAPACITY = 5,
  DEFAULT_TABLE_NAME = buildTableName(),
  //DEFAULT_HOST_NAME = os.hostname(),
  DEFAULT_BATCH_SIZE = 25,
  DEFAULT_SEND_INTERVAL = (DEBUG) ? 1000 : 5000;  // 1 second if debug, otherwise 5 seconds.


var createDynamoValueObject = function (key, value) {
  var obj = {};
  obj[key] = value.toString();  // All types must be converted to a string.
  return obj;
};

class DynamoStream {
  constructor(options) {
    var self = this;

    self.writeBuffer = [];
    self.isTableCreated = false;
    self.isTableBeingCreated = false;

    self.setConfig(options);

    (function sendPeriodically() {
      setTimeout(sendPeriodically, self.config.sendInterval);
      self.processWriteQueue(true, function (err, items) {
        // TODO: Send error event instead.
        if(err) {
          console.log(err);
        }
      });
    })();
  }

  setConfig(options) {
    var self = this;

    if (!options) {
      options = {};
    }

    self.config = {
      "aws": options.aws || {},
      "enableHostname": (options.enableHostname === true || options.enableHostname === false) ? options.enableHostname : DEFAULT_HOSTNAME_ENABLE,
      "batchSize": options.batchSize || DEFAULT_BATCH_SIZE,
      "sendInterval": options.sendInterval || DEFAULT_SEND_INTERVAL,
      "tableName": options.tableName || DEFAULT_TABLE_NAME,
      "tableHashKey": options.hashKey || DEFAULT_TABLE_HASH_KEY,
      "tableHashType": options.hashType || DEFAULT_TABLE_HASH_TYPE,
      "tableRangeKey": options.rangeKey || DEFAULT_TABLE_RANGE_KEY,
      "tableRangeType": options.rangeType || DEFAULT_TABLE_RANGE_TYPE,
      "tableReadCapacity": options.tableReadCapacity || DEFAULT_TABLE_READ_CAPACITY,
      "tableWriteCapacity": options.tableWriteCapacity || DEFAULT_TABLE_WRITE_CAPACITY
    };

    if(options["debug"] === true) {
      DEBUG = true;
    }

    if(options["trace"] === true) {
      DEBUG = TRACE = true;
    }

    if (options.aws["apiVersion"] === undefined) {
      self.config.aws.apiVersion = DEFAULT_AWS_API_VERSION;
    }

    if (options.aws["maxRetries"] === undefined) {
      self.config.aws.maxRetries = DEFAULT_AWS_MAX_RETRIES;
    }

    if (options.aws["region"] === undefined) {
      self.config.aws.region = DEFAULT_AWS_REGION;
    }

    if (self.config.aws.endpoint) {
      self.config.aws.endpoint = new AWS.Endpoint(self.config.aws.endpoint);
    }

    if (!self.db) {
      self.db = new AWS.DynamoDB(self.config.aws);
    }
  }

  write(record, encoding, cb = function (err) {if(err) {console.log(err);}}) {
    var item = {},
      self = this;

    if ( ! record) {
      return cb();
    }

    // If the record is a string, convert it to a JSON object.
    if (typeof record === 'string') {
      record = JSON.parse(record);
    }

    // The hash key/value pair must be defined and part of a unique pair.
    if ( ! record[self.config.tableHashKey]) {
      switch (self.config.tableHashType) {
        case DYNAMO_TYPE_STRING:
          record[self.config.tableHashKey] = uuid.v4();
          break;

        case DYNAMO_TYPE_NUMBER:
          record[self.config.tableHashKey] = process.hrtime(1);
          break;

        default:
          var error = new Error("DynamoStream:  The hash field " + self.config.tableHashKey + " must be defined.");
          error.status = 500;
          return cb(error);
      }
    }

    item[self.config.tableHashKey] = createDynamoValueObject(self.config.tableHashType, record[self.config.tableHashKey]);
    item["time"] = createDynamoValueObject(DYNAMO_TYPE_NUMBER, Date.parse(record.time));

    if (record.msg && ! item.msg) {
      item.msg = createDynamoValueObject(DYNAMO_TYPE_STRING, record.msg);
    }
    if (record.level && ! item.level) {
      item.level = createDynamoValueObject(DYNAMO_TYPE_NUMBER, record.level);
    }
    if (self.config.enableHostname && record.hostname && ! item.hostname) {
      item.hostname = createDynamoValueObject(DYNAMO_TYPE_STRING, record.hostname);
    }

    if (record.pid && ! item.pid) {
      item.pid = createDynamoValueObject(DYNAMO_TYPE_NUMBER, record.pid);
    }

    if (record.v && ! item.v) {
      item.v = createDynamoValueObject(DYNAMO_TYPE_NUMBER, record.v);
    }

    this.writeBuffer.push({
      PutRequest: {
        Item: item
      }
    });

    this.processWriteQueue(false, cb);
  }

  processWriteQueue(force, cb) {
    var self = this;

    if( ! self.writeBuffer.length) {
      return cb();
    }

    if( ! (force || self.writeBuffer.length >= self.config.batchSize)) {
      if(DEBUG) { console.log("processWriteQueue(%s/%s): Waiting for send interval or buffer to fill.", self.writeBuffer.length, self.config.batchSize); }
      return cb();
    }

    self.createTable(function (err) {
      if (err) {
        cb(err);
      } else {
        var requestItems = self.writeBuffer.slice(0, self.config.batchSize);

        // build request
        var batchRequest = {
          RequestItems: {}
        };

        batchRequest.RequestItems[self.config.tableName] = requestItems;

        // remove batched items from buffer
        self.writeBuffer = self.writeBuffer.slice(self.config.batchSize);

        self.db.batchWriteItem(batchRequest, function (err, response) {
          if (err) {
            // If an error occurred, the unprocessed items should be returned to the buffer.
            self.writeBuffer = requestItems.concat(self.writeBuffer);

            if(err.code === "ResourceNotFoundException" && self.isTableBeingCreated) {
              if(TRACE) { console.log("processWriteQueue(%s/%s): Can't write, still waiting for table to be created.", self.writeBuffer.length, self.config.batchSize); }
              cb();
            } else {
              cb(err);
            }
          } else {
            var numItemsSent = requestItems.length;
            // If AWS didn't process all of the items, return the unprocessed items to the buffer.
            if (response && response.UnprocessedItems && response.UnprocessedItems[self.config.tableName]) {
              numItemsSent -= response.UnprocessedItems[self.config.tableName].length;
              self.writeBuffer = response.UnprocessedItems[self.config.tableName].concat(self.writeBuffer);
            }
            if(DEBUG) { console.log("processWriteQueue(%s/%s): Sent %s item%s.", self.writeBuffer.length, self.config.batchSize, numItemsSent, (numItemsSent > 1) ? "s" : ""); }
            
            // After writing items to a table, the creation should be complete.
            self.isTableBeingCreated = false;
            cb()
          }
        });
      }
    });
  }

  createTable(cb) {
    var self = this;
    if ( ! self.isTableCreated) {
      self.db.listTables(function (err, result) {
        if (err) {
          cb(err);
        } else if (result && result.TableNames && result.TableNames.indexOf(self.config.tableName) != -1) {
          if(DEBUG) { console.log("createTable(%s):  Table is already created.", self.config.tableName); }
          self.isTableCreated = true;
          cb();
        } else {
          self.db.createTable({
            TableName: self.config.tableName,
            AttributeDefinitions: [
              {
                AttributeName: self.config.tableHashKey,
                AttributeType: self.config.tableHashType
              },
              {
                AttributeName: self.config.tableRangeKey,
                AttributeType: self.config.tableRangeType
              }
            ],
            KeySchema: [
              {
                AttributeName: self.config.tableHashKey,
                KeyType: 'HASH'
              },
              {
                AttributeName: self.config.tableRangeKey,
                KeyType: 'RANGE'
              }
            ],
            ProvisionedThroughput: {
              ReadCapacityUnits: self.config.tableReadCapacity,
              WriteCapacityUnits: self.config.tableWriteCapacity
            }
          }, function (err, result) {
            if (err && err.code !== 'ResourceInUseException') {
              cb(err);
            } else {
              if(DEBUG) { console.log("createTable(%s):  DynamoDB table is being created...", self.config.tableName); }
              
              // TODO: Handle write not available.
              if(err && err.code === 'ResourceInUseException') { console.log("createTable(%s):  Resource in use exception thrown.", self.config.tableName); }
              self.isTableCreated = true;
              self.isTableBeingCreated = true;
              cb();
            }
          });
        }
      });
    } else {
      cb();
    }
  }

  static get DYNAMO_TYPE_ARRAY_BINARY() {
    return DYNAMO_TYPE_ARRAY_BINARY;
  }
  static get DYNAMO_TYPE_ARRAY_MAP() {
    return DYNAMO_TYPE_ARRAY_MAP;
  }
  static get DYNAMO_TYPE_ARRAY_NUMBER() {
    return DYNAMO_TYPE_ARRAY_NUMBER;
  }
  static get DYNAMO_TYPE_ARRAY_STRING() {
    return DYNAMO_TYPE_ARRAY_STRING;
  }
  static get DYNAMO_TYPE_BINARY() {
    return DYNAMO_TYPE_BINARY;
  }
  static get DYNAMO_TYPE_BOOLEAN() {
    return DYNAMO_TYPE_BOOLEAN;
  }
  static get DYNAMO_TYPE_ARRAY_NUMBER() {
    return DYNAMO_TYPE_ARRAY_NUMBER;
  }
  static get DYNAMO_TYPE_MAP() {
    return DYNAMO_TYPE_MAP;
  }
  static get DYNAMO_TYPE_NULL() {
    return DYNAMO_TYPE_NULL;
  }
  static get DYNAMO_TYPE_NUMBER() {
    return DYNAMO_TYPE_NUMBER;
  }
  static get DYNAMO_TYPE_STRING() {
    return DYNAMO_TYPE_STRING;
  }
}

module.exports = DynamoStream;
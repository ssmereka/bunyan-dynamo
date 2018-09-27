"use strict";

var AWS = require('aws-sdk'),
  EventEmitter = require('events'),
  moment = require('moment'),
  os = require('os'),
  util = require('util'),
  uuid = require('uuid');

var DEBUG = false,
  TRACE = false;


/* ************************************************** *
 * ******************** Constants
 * ************************************************** */

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
  DEFAULT_AWS_REGION = "us-east-1",
  DEFAULT_HOSTNAME_ENABLE = true,
  DEFAULT_TABLE_HASH_KEY = "id",
  DEFAULT_TABLE_HASH_TYPE = DYNAMO_TYPE_STRING,
  DEFAULT_TABLE_RANGE_KEY = "time",
  DEFAULT_TABLE_RANGE_TYPE = DYNAMO_TYPE_NUMBER,
  DEFAULT_TABLE_READ_CAPACITY = 5,
  DEFAULT_TABLE_WRITE_CAPACITY = 5,
  DEFAULT_BATCH_SIZE = 25,
  DEFAULT_SEND_INTERVAL = (DEBUG) ? 1000 : 5000;  // 1 second if debug, otherwise 5 seconds.


/* ************************************************** *
 * ******************** Global Private Methods
 * ************************************************** */

var createDynamoValueObject = function (key, value) {
  var obj = {};
  obj[key] = (typeof value === 'object') ? value : value.toString();  // All types must be converted to a string.
  return obj;
};

// detect the DynamoDB type of a value based on its javascript type
var detectDynamoType  = function (value) {
    // handle non-object cases
    if (value === null || value === undefined) {
        return DYNAMO_TYPE_NULL;
    } else if (typeof value === 'number') {
        return DYNAMO_TYPE_NUMBER;
    } else if (typeof value === 'boolean') {
        return DYNAMO_TYPE_BOOLEAN;
    } else if (typeof value !== 'object') {
        return DYNAMO_TYPE_STRING;
    }

    // handle arrays
    if (Array.isArray(value)) {
        var types = value.map(detectDynamoType);
        // ignore arrays with multiple child data types
        if ((new Set(value)).size > 1) { return DYNAMO_TYPE_STRING; }
        // handle arrays of uniform simple child data types
        if (types[0] === DYNAMO_TYPE_NUMBER) { return DYNAMO_TYPE_ARRAY_NUMBER; }
        if (types[0] === DYNAMO_TYPE_STRING) { return DYNAMO_TYPE_ARRAY_STRING; }
        if (types[0] === DYNAMO_TYPE_MAP) { return DYNAMO_TYPE_ARRAY_MAP; }
        return DYNAMO_TYPE_STRING;
    }

    // handle maps (string -> string)
    if (Object.keys(value).every(key => (typeof value[key] === 'string'))) { return DYNAMO_TYPE_MAP; }

    return DYNAMO_TYPE_STRING;
}

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


/* ************************************************** *
 * ******************** Dynamo Stream Class
 * ************************************************** */

class DynamoStream {
  constructor(options) {
    var self = this;

    self.writeBuffer = [];
    self.disableBatchSending = false;
    self.configLock = false;

    self.setConfig(options);
  }

  setDefaultConfig() {
    DEBUG = TRACE = false;
    this.config = {
      "aws": {},
      "enableHostname": DEFAULT_HOSTNAME_ENABLE,
      "batchSize": DEFAULT_BATCH_SIZE,
      "sendInterval": DEFAULT_SEND_INTERVAL,
      "tableName": buildTableName(),
      "tableHashKey": DEFAULT_TABLE_HASH_KEY,
      "tableHashType": DEFAULT_TABLE_HASH_TYPE,
      "tableRangeKey": DEFAULT_TABLE_RANGE_KEY,
      "tableRangeType": DEFAULT_TABLE_RANGE_TYPE,
      "tableReadCapacity": DEFAULT_TABLE_READ_CAPACITY,
      "tableWriteCapacity": DEFAULT_TABLE_WRITE_CAPACITY
    };
  }

  validateLockedOption(options, propertyName) {
    if(options[propertyName] !== undefined && options[propertyName] !== this.config[propertyName]) {
      console.log("Error:  bunyan-dynamo does not support changing the %s property after write() has been called.  Instead create a new bunyan-dynamo object.", propertyName);
      delete options[propertyName];
    }
  };

  setConfig(options) {
    var self = this;

    if ( ! options) {
      options = {};
    }

    if(self.config === undefined) {
      self.setDefaultConfig();
    } else {
      self.validateLockedOption(options, 'tableName');
      self.validateLockedOption(options, 'tableHashKey');
      self.validateLockedOption(options, 'tableHashType');
      self.validateLockedOption(options, 'tableRangeKey');
      self.validateLockedOption(options, 'tableRangeType');
      self.validateLockedOption(options, 'tableReadCapacity');
      self.validateLockedOption(options, 'tableWriteCapacity');
      if(options["aws"] !== undefined) {
        // TODO: Throw error in event instead.
        console.log("Error:  bunyan-dynamo does not support changing the aws settings after a write() has been called.  Instead create a new bunyan-dynamo object.");
        delete options.aws;
      }
    }

    self.stopTimer();

    self.config = {
      "aws": options.aws || self.config.aws,
      "enableHostname": (options.enableHostname === true || options.enableHostname === false) ? options.enableHostname : self.config.enableHostname,
      "batchSize": options.batchSize || self.config.batchSize,
      "sendInterval": options.sendInterval || self.config.sendInterval,
      "tableName": options.tableName || self.config.tableName,
      "tableHashKey": options.hashKey || self.config.tableHashKey,
      "tableHashType": options.hashType || self.config.tableHashType,
      "tableRangeKey": options.rangeKey || self.config.tableRangeKey,
      "tableRangeType": options.rangeType || self.config.tableRangeType,
      "tableReadCapacity": options.tableReadCapacity || self.config.tableReadCapacity,
      "tableWriteCapacity": options.tableWriteCapacity || self.config.tableWriteCapacity
    };

    if(options["debug"] === true) {
      DEBUG = true;
    }

    if(options["trace"] === true) {
      DEBUG = TRACE = true;
    }

    if ((options.aws === undefined || options.aws["apiVersion"] === undefined) && self.config.aws.apiVersion === undefined) {
      self.config.aws.apiVersion = DEFAULT_AWS_API_VERSION;
    }

    if ((options.aws === undefined || options.aws["maxRetries"] === undefined) && self.config.aws.maxRetries === undefined) {
      self.config.aws.maxRetries = DEFAULT_AWS_MAX_RETRIES;
    }

    if ((options.aws === undefined || options.aws["region"] === undefined) && self.config.aws.region === undefined) {
      self.config.aws.region = DEFAULT_AWS_REGION;
    }

    //if (self.config.aws.endpoint) {
    //  self.config.aws.endpoint = new AWS.Endpoint(self.config.aws.endpoint);
    //}

    self.db = new AWS.DynamoDB(self.config.aws);

    self.startTimer();
  }

  startTimer() {
    var self = this;

    self.timer = setInterval(function sendPeriodically() {
      if(TRACE) { console.log("sendPeriodically(%s): Send interval expired.", self.config.sendInterval); }
      self.processWriteQueue(true, function (err, items) {
        // TODO: Send error event instead.
        if (err) {
          console.log(err);
        }
      });
    }, self.config.sendInterval);
  }

  stopTimer() {
    if(this.timer) {
      clearInterval(this.timer);
    }
  }

  getConfig() {
    return this.config;
  }

  write(record, encoding, cb = function (err) {if(err) {console.log(err);} }) {
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

    // store all other object properties in the dynamo item as strings
    var globalKeys = new Set([self.config.tableHashKey, 'time', 'msg', 'level', 'hostname', 'pid', 'v']);
    var data = {};
    Object.keys(record).filter(key => !globalKeys.has(key)).map(key => (data[key] = record[key]));
    item.data = createDynamoValueObject(DYNAMO_TYPE_MAP, AWS.DynamoDB.Converter.marshall(data));


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

    if( ! force) {
      if(self.disableBatchSending) {
        if(DEBUG) { console.log("processWriteQueue(%s/%s): Waiting for Dynamo DB to create the table.", self.writeBuffer.length, self.config.batchSize); }
        return cb();
      } else if(self.writeBuffer.length < self.config.batchSize) {
        if(DEBUG) { console.log("processWriteQueue(%s/%s): Waiting for send interval or buffer to fill.", self.writeBuffer.length, self.config.batchSize); }
        return cb();
      }
    }

    /*self.createTable(function (err) {
      if (err) {
        cb(err);
      } else {*/
        console.log('slicing buffer of length', self.writeBuffer.length);
        var requestItems = self.writeBuffer.slice(0, self.config.batchSize);

        // build request
        var batchRequest = {
          RequestItems: {}
        };

        batchRequest.RequestItems[self.config.tableName] = requestItems;

        // remove batched items from buffer
        self.writeBuffer = self.writeBuffer.slice(self.config.batchSize);

          console.log('batchWriteItem', JSON.stringify(batchRequest, null, 4));
        self.db.batchWriteItem(batchRequest, function (err, response) {
          if (err) {
            if(TRACE) { console.log("processWriteQueue(%s/%s): batchWriteItem failed with an error.", self.writeBuffer.length, self.config.batchSize); }

            // If an error occurred, the unprocessed items should be returned to the buffer.
            self.writeBuffer = requestItems.concat(self.writeBuffer);
            cb(err);
          } else {
            var numItemsSent = requestItems.length;
            // If AWS didn't process all of the items, return the unprocessed items to the buffer.
            if (response && response.UnprocessedItems && response.UnprocessedItems[self.config.tableName]) {
              numItemsSent -= response.UnprocessedItems[self.config.tableName].length;
              self.writeBuffer = response.UnprocessedItems[self.config.tableName].concat(self.writeBuffer);
            }
            if(DEBUG) { console.log("processWriteQueue(%s/%s): Sent %s item%s.", self.writeBuffer.length, self.config.batchSize, numItemsSent, (numItemsSent > 1) ? "s" : ""); }
            cb()
          }
        });
      //}
    // });
  }

  createTable(cb) {
    var self = this;

    if ( ! self.isTableCreated) {
      // Disable changes to the config options once a table needs to be selected or created.
      self.configLock = true;

      self.db.listTables(function (err, result) {
        if (err) {
          if(TRACE) { console.log("createTable(%s): listTables failed with an error.", self.config.tableName); }
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
            if (err) {
              if(TRACE) { console.log("createTable(%s): createTable failed with an error.", self.config.tableName); }
              cb(err);
            } else {
              if(DEBUG) { console.log("createTable(%s):  DynamoDB table is being created...", self.config.tableName); }

              // Disable sending via the batch trigger.
              self.disableBatchSending = true;

              // Disable sending via the sendInterval trigger.
              self.stopTimer();

              // Wait for the database table to be written.
              self.db.waitFor('tableExists', { TableName: self.config.tableName }, function (err, data) {
                if(err) {
                  if(TRACE) { console.log("createTable(%s): waitFor table to exist failed with an error.", self.config.tableName); }
                  cb(err);
                } else {
                  // Enable batch sending.
                  self.disableBatchSending = false;

                  // Enable sendInterval sending.
                  self.startTimer();

                  // Mark the table as created.
                  self.isTableCreated = true;
                  cb();
                }
              });
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

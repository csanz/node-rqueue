/**
 * Requires.
 */
var redis  = require('./deps/node-redis'),
    events = require('events');

/**
 * Handle an error appropiatly.
 *
 * @param {Error} error The error object in question.
 * @param {Function} callback Optional callback to pass error to.
 */
var handleError = function (error, callback) {
  if (callback) callback(error);
  else {
    throw error;
  }
};

/**
 * Remove a prefix from a string.
 *
 * @param {String} prefix The prefix.
 * @param {String} string The string.
 * @returns {string} The un-prefixed string.
 */
var removePrefix = function (prefix, string) {
  return string.slice(prefix.length);
};

/**
 * The Queue prototype used by the server to add jobs to a queue.
 *
 * @constructor
 * @param {String} name The queue name.
 * @param {String} host The host name for the Redis client.
 * @param {Number} port The port number for the Redis client.
 */
var Queue = function (name, host, port) {
  this.name   = name;
  this.client = redis.createClient(host, port);
  this.prefix = 'queue:';
};

/**
 * Creates a new Queue object.
 *
 * @param {String} name The queue name.
 * @param {String} host The host name for the Redis client.
 * @param {Number} port The port number for the Redis client.
 * @returns {Queue}
 */
exports.createQueue = function (name, host, port) {
  return new Queue(name, host, port);
};

exports.Queue = Queue;

/**
 * Adds a new job to the queue.
 *
 * @param The data payload to enqueue.
 */
Queue.prototype.push = function (payload, callback) {
  var self = this;

  // Get an ID from redis
  this.client.incr('id:' + this.name, function (error, id) {
    if (error) {
      return handleError(error, callback);
    }

    // Push the job.
    self.client.rpush(self.prefix + self.name, JSON.stringify({
      id: id,
      payload: payload,
      error_count: 0,
      errors: []
    }), function (error, length) {
      if (error) {
        return handleError(error, callback);
      }

      if (callback) callback(null, id);
    });
  });
};


/**
 * Worker prototype used by the workers to listen for jobs.
 * Inherits from EventEmitter.
 *
 * @constructor
 */
var Worker = function (name, host, port) {
  var self = this;

  this.host      = host;
  this.port      = port;
  this.prefix    = 'queue:';
  this.name      = name;
  this.queues    = {};
  // TODO: Rename?
  this.continual = false;

  // Client for use with child jobs.
  this.child_client = redis.createClient(host, port);

  /*
   * Callback for blpop responses.
   *
   * @private
   * @param {Error} error Possible error from Redis
   * @param data The data from redis.
   */
  this._onPop = function (error, data) {
    if (error) {
      return self.emit('error', error);
    }

    var data = JSON.parse(Buffer.isBuffer(data[1]) ? data[1].toString() : data),
        job  = new Job(self, data);

    self.emit('message', job);

    if (!self.client.quitting && self.continual) {
      // Listen for more jobs.
      self.client.blpop(self.prefix + self.name, 0, self._onPop);
    }
  };
};

// Inherits from EventEmitter.
Worker.prototype = Object.create(events.EventEmitter.prototype);
Worker.prototype.constructor = Worker;

/**
 * Creates a new Worker object.
 *
 * @param {String} host The host name for the Redis client.
 * @param {Number} port The port number for the Redis client.
 * @returns {Worker}
 */
exports.createWorker = function (host, port) {
  return new Worker(host, port);
};

exports.Worker = Worker;

/**
 * Listen for the next job. Only has to be called by user if `continual` is false.
 */
Worker.prototype.next = function () {
  this.client.blpop(this.prefix + this.name, 0, this._onPop); 
};

/**
 * Start the worker
 */
Worker.prototype.start = function () {
  this.client = redis.createClient(this.host, this.port);
  this.next();
};

/**
 * Stop the worker
 */
Worker.prototype.stop = function () {
  this.client.destroy();
};


/**
 * Job prototype used by the workers.
 *
 * @constructor
 * @param {Worker} parent Parent prototype
 * @param payload The data to set as the payload.
 */
var Job = function (parent, data, queue) {
  this.payload     = data.payload;
  this.id          = data.id;
  this.error_count = data.error_count;
  this.errors      = data.errors;
  this.queue       = queue;
  this.prefix      = parent.prefix;

  // Associate with a redis client.
  this.client = parent.child_client;
};

exports.Job = Job;

/**
 * Add an error the the job.
 *
 * @param {Error} error The error object to add.
 */
Job.prototype.reportError = function (error) {
  ++this.error_count;
  this.errors.push(error.message ? error.message : error.toString());
};

/**
 * Re-process the job by adding back to the queue.
 *
 * @param {Function} callback The optional callback
 */
Job.prototype.retry = function (callback) {
  var self = this;

  this.client.rpush(this.prefix + this.queue, JSON.stringify({
    id:          this.id,
    payload:     this.payload,
    error_count: this.error_count,
    errors:      this.errors
  }), function (error) {
    if (error)    return handleError(error, callback);
    if (callback) callback(null, self.id);
  });
};

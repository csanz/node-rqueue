/**
 * Requires.
 */
var redis  = require('./deps/node-redis'),
    events = require('events'),
    util   = require('util');

/**
 * Handle an error appropiatly.
 *
 * @param {Error} error: The error object in question.
 * @param {Function} callback: Optional callback to pass error to.
 */
var handleError = function (error, callback) {
  if (callback) callback(error);
  else {
    throw error;
  }
};

/**
 * The Queue prototype used by the server to add jobs to a queue.
 *
 * @constructor
 * @param {Object} options: A hash that can contain name, host, port, auth, prefix
 */
var Queue = function (options) {
  var self = this;

  this.name   = options.name;
  this.client = redis.createClient(options.port, options.host, options.auth);
  this.prefix = options.prefix || '';

  this.client.on('error', function (error) {
    self.emit('error', error);
  });
};

// Inherits from EventEmitter.
util.inherits(Queue, events.EventEmitter);

/**
 * Creates a new Queue object.
 *
 * @param {Object} options: A hash that can contain name, host, port, auth, prefix
 * @returns {Queue}
 */
exports.createQueue = function (options) {
  return new Queue(options);
};

exports.Queue = Queue;

/**
 * Adds a new job to the queue.
 *
 * @param {Object} payload: The data payload to enqueue.
 * @param {Function} callback: The data payload to enqueue.
 */
Queue.prototype.push = function (payload, callback) {
  var self = this;

  // Get an ID from redis
  this.client.incr(this.prefix + 'id:' + this.name, function (error, id) {
    if (error) {
      return handleError(error, callback);
    }

    // Push the job.
    self.client.rpush(self.prefix + 'queue:' + self.name, JSON.stringify({
      id: id,
      payload: payload,
      error_count: 0,
      errors: [],
      modified: Date.now()
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
 * @param {Object} options: A hash that can contain name, host, port, auth, prefix
 */
var Worker = function (options) {
  var self = this;

  // Call parent
  events.EventEmitter.call(this);

  this.host      = options.host;
  this.port      = options.port;
  this.auth      = options.auth;
  this.prefix    = options.prefix || '';
  this.name      = options.name;
  this.queues    = {};
  // TODO: Rename?
  this.continual = false;

  // Client for use with child jobs.
  this._child_client = redis.createClient(this.port, this.host, this.auth);

  this._child_client.on('error', function (error) {
    self.emit('error', error);
  });

  /**
   * Callback for blpop responses.
   *
   * @private
   * @param {Error} error: Possible error from Redis
   * @param {Object} data: The data from redis.
   */
  this._onPop = function (error, data) {
    if (error) {
      return self.emit('error', error);
    }

    try {
      data = JSON.parse(Buffer.isBuffer(data[1]) ? data[1].toString() : data[1]);
      var job  = new Job(self, data);

      self.emit('message', job);
    } catch (json_error) {}

    if (!self.client.quitting && self.continual) {
      // Listen for more jobs.
      self.client.blpop(self.prefix + 'queue:' + self.name, 0, self._onPop);
    }
  };
};

// Inherits from EventEmitter.
util.inherits(Worker, events.EventEmitter);

/**
 * Creates a new Worker object.
 *
 * @param {Object} options: A hash that can contain name, host, port, auth, prefix
 * @returns {Worker}
 */
exports.createWorker = function (options) {
  return new Worker(options);
};

exports.Worker = Worker;

/**
 * Listen for the next job. Only has to be called by user if `continual` is false.
 */
Worker.prototype.next = function () {
  this.client.blpop(this.prefix + 'queue:' + this.name, 0, this._onPop); 
};

/**
 * Start the worker
 */
Worker.prototype.start = function () {
  var self = this;

  this.client = redis.createClient(this.port, this.host, this.auth);

  this.client.on('error', function (error) {
    self.emit('error', error);
  });

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
 * @param {Worker} worker: Parent prototype
 * @param {Object} payload: The data to set as the payload.
 */
var Job = function (worker, data) {
  this.payload     = data.payload;
  this.id          = data.id;
  this.error_count = data.error_count;
  this.errors      = data.errors;
  this.modified    = data.modified;
  this.queue       = worker.name;
  this.prefix      = worker.prefix;

  // Associate with a redis client.
  this.parent = worker;
};

exports.Job = Job;

/**
 * Add an error the the job.
 *
 * @param {Error} error: The error object to add.
 */
Job.prototype.reportError = function (error) {
  ++this.error_count;
  this.errors.push(error.message ? error.message : error.toString());
};

/**
 * Re-process the job by adding back to the queue.
 *
 * @param {Function} callback: The optional callback
 */
Job.prototype.retry = function (callback) {
  var self = this;

  this.parent._child_client.rpush(this.prefix + 'queue:' + this.queue, JSON.stringify({
    id:          this.id,
    payload:     this.payload,
    error_count: this.error_count,
    errors:      this.errors,
    modified:    Date.now()
  }), function (error) {
    if (error)    return handleError(error, callback);
    if (callback) callback(null, self.id);
  });
};

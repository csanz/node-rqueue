var client = require('../'),
    assert = require('assert');

var q = client.createQueue('worker.test'),
    w = client.createWorker('worker.test'),
    job;

w.continual = true;

job = new client.Job(w, {
  payload:     'testing',
  id:          2,
  error_count: 0,
  errors:      []
}, 'worker.test');

q.push({
  testing: 'worker',
  time: 'lunch'
});

module.exports = {
  "test Worker events": function (done) {
    w.once('message', function (job) {
      assert.ok(job);
      assert.equal(typeof job.reportError, 'function');
      assert.equal(typeof job.retry, 'function');

      assert.equal(job.id, 1);
      assert.equal(job.error_count, 0);
      assert.equal(job.errors.length, 0);
      assert.equal(job.payload.testing, 'worker');
      assert.equal(job.payload.time, 'lunch');

      done();
    });

    w.start();
  },
  "test Worker#stop": function () {
    w.stop();
    assert.ok(w.client.quitting);
  },
  "test Job#reportError": function () {
    job.reportError(new Error('Bacon was not tasty enough.'));

    assert.equal(job.errors.length, 1);
    assert.equal(job.error_count, 1);
  },
  "test Job#retry": function (done) {
    w.on('message', function (job) {
      assert.ok(job);

      assert.equal(job.id, 2);
      assert.equal(job.error_count, 1);
      assert.equal(job.errors.length, 1);
      assert.equal(job.errors[0], 'Bacon was not tasty enough.');
      assert.equal(job.payload, 'testing');

      done();
    });

    w.start();

    job.retry(function (error, id) {
      assert.ok(!error);
      assert.equal(id, job.id);
    });
  },
  after: function () {
    q.client.del('queue:worker.test');
    q.client.del('id:worker.test');
    q.client.quit();

    w.child_client.quit();
    w.stop();
  }
}

var q      = require('../').createQueue({ name: 'queue.test' }),
    assert = require('assert');

module.exports = {
  'test queue exports': function () {
    assert.ok(q.client);
    assert.equal('queue.test', q.name);

    // Queue methods
    assert.equal(typeof q.push, 'function');
  },
  'test job creation': function (done) {
    q.push({
      name: 'Tim',
      server: 'node'
    }, function (error, id) {
      assert.ok(!error);
      assert.equal(id, 1);

      q.client.lpop('queue:queue.test', function (error, data) {
        assert.ok(!error);

        data = JSON.parse(data.toString());

        assert.equal(data.id, 1);
        assert.equal(data.errors.length, 0);
        assert.equal(data.error_count, 0);
        assert.equal(data.payload.name, 'Tim');
        assert.equal(data.payload.server, 'node');

        done();
      });
    });
  },
  after: function () {
    q.client.del('queue:queue.test');
    q.client.del('id:queue.test');
    q.client.quit();
  }
};

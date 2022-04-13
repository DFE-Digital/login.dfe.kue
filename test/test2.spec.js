var _, async, kue, should, util;

_ = require('lodash');

async = require('async');

should = require('should');

kue = require('../');

util = require('util');

describe('Kue Tests', function() {
  var Job, jobs;
  jobs = null;
  Job = null;
  beforeEach(function() {
    jobs = kue.createQueue({
      promotion: {
        interval: 50
      }
    });
    return Job = kue.Job;
  });
  afterEach(function(done) {
    return jobs.shutdown(50, done);
  });
  //  before (done) ->
  //    jobs = kue.createQueue({promotion:{interval:100}})
  //    jobs.client.flushdb done

  //  after (done) ->
  //    jobs = kue.createQueue({promotion:{interval:100}})
  //    jobs.client.flushdb done
  describe('Job Producer', function() {
    it('should save jobs having a new id', function(done) {
      var job, job_data;
      job_data = {
        title: 'Test Email Job',
        to: 'tj@learnboost.com'
      };
      job = jobs.create('email-to-be-saved', job_data);
      jobs.process('email-to-be-saved', _.noop);
      return job.save(function(err) {
        job.id.should.be.an.instanceOf(Number);
        return done(err);
      });
    });
    it('should set worker id on job hash', function(done) {
      var job, job_data;
      job_data = {
        title: 'Test workerId Job',
        to: 'tj@learnboost.com'
      };
      job = jobs.create('worker-id-test', job_data);
      jobs.process('worker-id-test', function(job, jdone) {
        jdone();
        return Job.get(job.id, function(err, j) {
          j.toJSON().workerId.should.be.not.null;
          return done();
        });
      });
      return job.save();
    });
    it('should receive job complete event', function(done) {
      var job_data;
      jobs.process('email-to-be-completed', function(job, done) {
        return done();
      });
      job_data = {
        title: 'Test Email Job',
        to: 'tj@learnboost.com'
      };
      return jobs.create('email-to-be-completed', job_data).on('complete', function() {
        return done();
      }).save();
    });
    it('should receive job result in complete event', function(done) {
      var job_data;
      jobs.process('email-with-results', function(job, done) {
        return done(null, {
          finalResult: 123
        });
      });
      job_data = {
        title: 'Test Email Job With Results',
        to: 'tj@learnboost.com'
      };
      return jobs.create('email-with-results', job_data).on('complete', function(result) {
        result.finalResult.should.be.equal(123);
        return done();
      }).save();
    });
    it('should receive job progress event', function(done) {
      var job_data;
      jobs.process('email-to-be-progressed', function(job, done) {
        job.progress(1, 2);
        return done();
      });
      job_data = {
        title: 'Test Email Job',
        to: 'tj@learnboost.com'
      };
      return jobs.create('email-to-be-progressed', job_data).on('progress', function(progress) {
        progress.should.be.equal(50);
        return done();
      }).save();
    });
    it('should receive job progress event with extra data', function(done) {
      var job_data;
      jobs.process('email-to-be-progressed', function(job, done) {
        job.progress(1, 2, {
          notifyTime: "2014-11-22"
        });
        return done();
      });
      job_data = {
        title: 'Test Email Job',
        to: 'tj@learnboost.com'
      };
      return jobs.create('email-to-be-progressed', job_data).on('progress', function(progress, extraData) {
        progress.should.be.equal(50);
        extraData.notifyTime.should.be.equal("2014-11-22");
        return done();
      }).save();
    });
    it('should receive job failed attempt events', function(done) {
      var errorMsg, job_data, total;
      total = 2;
      errorMsg = 'myError';
      jobs.process('email-to-be-failed', function(job, jdone) {
        return jdone(errorMsg);
      });
      job_data = {
        title: 'Test Email Job',
        to: 'tj@learnboost.com'
      };
      return jobs.create('email-to-be-failed', job_data).attempts(2).on('failed attempt', function(errMsg, doneAttempts) {
        errMsg.should.be.equal(errorMsg);
        doneAttempts.should.be.equal(1);
        return total--;
      }).on('failed', function(errMsg) {
        errMsg.should.be.equal(errorMsg);
        (--total).should.be.equal(0);
        return done();
      }).save();
    });
    it('should receive queue level complete event', function(done) {
      var job_data, testJob;
      jobs.process('email-to-be-completed', function(job, jdone) {
        return jdone(null, {
          prop: 'val'
        });
      });
      jobs.on('job complete', function(id, result) {
        id.should.be.equal(testJob.id);
        result.prop.should.be.equal('val');
        return done();
      });
      job_data = {
        title: 'Test Email Job',
        to: 'tj@learnboost.com'
      };
      return testJob = jobs.create('email-to-be-completed', job_data).save();
    });
    return it('should receive queue level failed attempt events', function(done) {
      var errorMsg, job_data, newJob, total;
      total = 2;
      errorMsg = 'myError';
      jobs.process('email-to-be-failed', function(job, jdone) {
        return jdone(errorMsg);
      });
      job_data = {
        title: 'Test Email Job',
        to: 'tj@learnboost.com'
      };
      jobs.on('job failed attempt', function(id, errMsg, doneAttempts) {
        id.should.be.equal(newJob.id);
        errMsg.should.be.equal(errorMsg);
        doneAttempts.should.be.equal(1);
        return total--;
      }).on('job failed', function(id, errMsg) {
        id.should.be.equal(newJob.id);
        errMsg.should.be.equal(errorMsg);
        (--total).should.be.equal(0);
        return done();
      });
      return newJob = jobs.create('email-to-be-failed', job_data).attempts(2).save();
    });
  });
  describe('Job', function() {
    it('should be processed after delay', function(done) {
      var now;
      now = Date.now();
      jobs.create('simple-delay-job', {
        title: 'simple delay job'
      }).delay(300).save();
      return jobs.process('simple-delay-job', function(job, jdone) {
        var processed;
        processed = Date.now();
        (processed - now).should.be.approximately(300, 100);
        jdone();
        return done();
      });
    });
    it('should have promote_at timestamp', function(done) {
      var job, now;
      now = Date.now();
      job = jobs.create('simple-delayed-job', {
        title: 'simple delay job'
      }).delay(300).save();
      return jobs.process('simple-delayed-job', function(job, jdone) {
        job.promote_at.should.be.approximately(now + 300, 100);
        jdone();
        return done();
      });
    });
    it('should update promote_at after delay change', function(done) {
      var job, now;
      now = Date.now();
      job = jobs.create('simple-delayed-job-1', {
        title: 'simple delay job'
      }).delay(300).save();
      job.delay(100).save();
      return jobs.process('simple-delayed-job-1', function(job, jdone) {
        job.promote_at.should.be.approximately(now + 100, 100);
        jdone();
        return done();
      });
    });
    it('should update promote_at after failure with backoff', function(done) {
      var calls, job, now;
      now = Date.now();
      job = jobs.create('simple-delayed-job-2', {
        title: 'simple delay job'
      }).delay(100).attempts(2).backoff({
        delay: 100,
        type: 'fixed'
      }).save();
      calls = 0;
      return jobs.process('simple-delayed-job-2', function(job, jdone) {
        var processed;
        processed = Date.now();
        if (calls === 1) {
          (processed - now).should.be.approximately(300, 100);
          jdone();
          done();
        } else {
          (processed - now).should.be.approximately(100, 100);
          jdone('error');
        }
        return calls++;
      });
    });
    it('should be processed at a future date', function(done) {
      var now;
      now = Date.now();
      jobs.create('future-job', {
        title: 'future job'
      }).delay(new Date(now + 200)).save();
      return jobs.process('future-job', function(job, jdone) {
        var processed;
        processed = Date.now();
        (processed - now).should.be.approximately(200, 100);
        jdone();
        return done();
      });
    });
    it('should receive promotion event', function(done) {
      var job_data;
      job_data = {
        title: 'Test Email Job',
        to: 'tj@learnboost.com'
      };
      jobs.process('email-to-be-promoted', function(job, done) {});
      return jobs.create('email-to-be-promoted', job_data).delay(200).on('promotion', function() {
        return done();
      }).save();
    });
    it('should be re tried after failed attempts', function(done) {
      var remaining, total;
      [total, remaining] = [2, 2];
      jobs.create('simple-multi-attempts-job', {
        title: 'simple-multi-attempts-job'
      }).attempts(total).save();
      return jobs.process('simple-multi-attempts-job', function(job, jdone) {
        job.toJSON().attempts.remaining.should.be.equal(remaining);
        (job.toJSON().attempts.made + job.toJSON().attempts.remaining).should.be.equal(total);
        if (!--remaining) {
          jdone();
          return done();
        } else {
          return jdone(new Error('reaattempt'));
        }
      });
    });
    it('should honor original delay at fixed backoff', function(done) {
      var remaining, start, total;
      [total, remaining] = [2, 2];
      start = Date.now();
      jobs.create('backoff-fixed-job', {
        title: 'backoff-fixed-job'
      }).delay(200).attempts(total).backoff(true).save();
      return jobs.process('backoff-fixed-job', function(job, jdone) {
        var now;
        if (!--remaining) {
          now = Date.now();
          (now - start).should.be.approximately(400, 120);
          jdone();
          return done();
        } else {
          return jdone(new Error('reaattempt'));
        }
      });
    });
    it('should honor original delay at exponential backoff', function(done) {
      var remaining, start, total;
      [total, remaining] = [3, 3];
      start = Date.now();
      jobs.create('backoff-exponential-job', {
        title: 'backoff-exponential-job'
      }).delay(50).attempts(total).backoff({
        type: 'exponential',
        delay: 100
      }).save();
      return jobs.process('backoff-exponential-job', function(job, jdone) {
        var now;
        job._backoff.type.should.be.equal("exponential");
        job._backoff.delay.should.be.equal(100);
        now = Date.now();
        if (!--remaining) {
          (now - start).should.be.approximately(350, 100);
          jdone();
          return done();
        } else {
          return jdone(new Error('reaattempt'));
        }
      });
    });
    it('should honor max delay at exponential backoff', function(done) {
      var last, remaining, total;
      [total, remaining] = [10, 10];
      last = Date.now();
      jobs.create('backoff-exponential-job', {
        title: 'backoff-exponential-job'
      }).attempts(total).backoff({
        type: 'exponential',
        delay: 50,
        maxDelay: 100
      }).save();
      return jobs.process('backoff-exponential-job', function(job, jdone) {
        var now;
        job._backoff.type.should.be.equal("exponential");
        job._backoff.delay.should.be.equal(50);
        job._backoff.maxDelay.should.be.equal(100);
        now = Date.now();
        (now - last).should.be.lessThan(120);
        if (!--remaining) {
          jdone();
          return done();
        } else {
          last = now;
          return jdone(new Error('reaattempt'));
        }
      });
    });
    it('should honor users backoff function', function(done) {
      var remaining, start, total;
      [total, remaining] = [2, 2];
      start = Date.now();
      jobs.create('backoff-user-job', {
        title: 'backoff-user-job'
      }).delay(50).attempts(total).backoff(function(attempts, delay) {
        return 250;
      }).save();
      return jobs.process('backoff-user-job', function(job, jdone) {
        var now;
        now = Date.now();
        if (!--remaining) {
          (now - start).should.be.approximately(350, 100);
          jdone();
          return done();
        } else {
          return jdone(new Error('reaattempt'));
        }
      });
    });
    it('should log with a sprintf-style string', function(done) {
      jobs.create('log-job', {
        title: 'simple job'
      }).save();
      return jobs.process('log-job', function(job, jdone) {
        job.log('this is %s number %d', 'test', 1);
        Job.log(job.id, function(err, logs) {
          logs[0].should.be.equal('this is test number 1');
          return done();
        });
        return jdone();
      });
    });
    return it('should log objects, errors, arrays, numbers, etc', function(done) {
      jobs.create('log-job', {
        title: 'simple job'
      }).save();
      return jobs.process('log-job', function(job, jdone) {
        var testErr;
        testErr = new Error('test error'); // to compare the same stack
        job.log();
        job.log(void 0);
        job.log(null);
        job.log({
          test: 'some text'
        });
        job.log(testErr);
        job.log([1, 2, 3]);
        job.log(123);
        job.log(1.23);
        job.log(0);
        job.log(0/0);
        job.log(true);
        job.log(false);
        Job.log(job.id, function(err, logs) {
          logs[0].should.be.equal(util.format(void 0));
          logs[1].should.be.equal(util.format(void 0));
          logs[2].should.be.equal(util.format(null));
          logs[3].should.be.equal(util.format({
            test: 'some text'
          }));
          logs[4].should.be.equal(util.format(testErr));
          logs[5].should.be.equal(util.format([1, 2, 3]));
          logs[6].should.be.equal(util.format(123));
          logs[7].should.be.equal(util.format(1.23));
          logs[8].should.be.equal(util.format(0));
          logs[9].should.be.equal(util.format(0/0));
          logs[10].should.be.equal(util.format(true));
          logs[11].should.be.equal(util.format(false));
          return done();
        });
        return jdone();
      });
    });
  });
  describe('Kue Core', function() {
    it('should receive a "job enqueue" event', function(done) {
      var job;
      jobs.on('job enqueue', function(id, type) {
        if (type === 'email-to-be-enqueued') {
          id.should.be.equal(job.id);
          return done();
        }
      });
      jobs.process('email-to-be-enqueued', function(job, jdone) {
        return jdone();
      });
      return job = jobs.create('email-to-be-enqueued').save();
    });
    it('should receive a "job remove" event', function(done) {
      var job;
      jobs.on('job remove', function(id, type) {
        if (type === 'removable-job') {
          id.should.be.equal(job.id);
          return done();
        }
      });
      jobs.process('removable-job', function(job, jdone) {
        return jdone();
      });
      return job = jobs.create('removable-job').save().remove();
    });
    return it('should fail a job with TTL is exceeded', function(done) {
      jobs.process('test-job-with-ttl', function(job, jdone) {});
      // do nothing to sample a stuck worker
      return jobs.create('test-job-with-ttl', {
        title: 'a ttl job'
      }).ttl(500).on('failed', function(err) {
        err.should.be.equal('TTL exceeded');
        return done();
      }).save();
    });
  });
  describe('Kue Job Concurrency', function() {
    it('should process 2 concurrent jobs at the same time', function(done) {
      var jobStartTimes, now;
      now = Date.now();
      jobStartTimes = [];
      jobs.process('test-job-parallel', 2, function(job, jdone) {
        jobStartTimes.push(Date.now());
        if (jobStartTimes.length === 2) {
          (jobStartTimes[0] - now).should.be.approximately(0, 100);
          (jobStartTimes[1] - now).should.be.approximately(0, 100);
          done();
        }
        return setTimeout(jdone, 500);
      });
      jobs.create('test-job-parallel', {
        title: 'concurrent job 1'
      }).save();
      return jobs.create('test-job-parallel', {
        title: 'concurrent job 2'
      }).save();
    });
    it('should process non concurrent jobs serially', function(done) {
      var jobStartTimes, now;
      now = Date.now();
      jobStartTimes = [];
      jobs.process('test-job-serial', 1, function(job, jdone) {
        jobStartTimes.push(Date.now());
        if (jobStartTimes.length === 2) {
          (jobStartTimes[0] - now).should.be.approximately(0, 100);
          (jobStartTimes[1] - now).should.be.approximately(500, 100);
          done();
        }
        return setTimeout(jdone, 500);
      });
      jobs.create('test-job-serial', {
        title: 'non concurrent job 1'
      }).save();
      return jobs.create('test-job-serial', {
        title: 'non concurrent job 2'
      }).save();
    });
    it('should process a new job after a previous one fails with TTL is exceeded', function(done) {
      var failures, jobStartTimes, now;
      failures = 0;
      now = Date.now();
      jobStartTimes = [];
      jobs.process('test-job-serial-failed', 1, function(job, jdone) {
        jobStartTimes.push(Date.now());
        if (jobStartTimes.length === 2) {
          (jobStartTimes[0] - now).should.be.approximately(0, 100);
          (jobStartTimes[1] - now).should.be.approximately(500, 100);
          failures.should.be.equal(1);
          return done();
        }
      });
      // do not call jdone to simulate a stuck worker
      jobs.create('test-job-serial-failed', {
        title: 'a ttl job 1'
      }).ttl(500).on('failed', function() {
        return ++failures;
      }).save();
      return jobs.create('test-job-serial-failed', {
        title: 'a ttl job 2'
      }).ttl(500).on('failed', function() {
        return ++failures;
      }).save();
    });
    return it('should not stuck in inactive mode if one of the workers failed because of ttl', function(done) {
      jobs.create('jobsA', {
        title: 'titleA',
        metadata: {}
      }).delay(1000).attempts(3).backoff({
        delay: 1 * 1000,
        type: 'exponential'
      }).removeOnComplete(true).ttl(1 * 1000).save();
      jobs.create('jobsB', {
        title: 'titleB',
        metadata: {}
      }).delay(1500).attempts(3).backoff({
        delay: 1 * 1000,
        type: 'exponential'
      }).removeOnComplete(true).ttl(1 * 1000).save();
      jobs.process('jobsA', 1, function(job, jdone) {
        if (job._attempts === '2') {
          done();
        }
      });
      return jobs.process('jobsB', 1, function(job, jdone) {
        done();
      });
    });
  });
  return describe('Kue Job Removal', function() {
    beforeEach(function(done) {
      jobs.process('sample-job-to-be-cleaned', function(job, jdone) {
        return jdone();
      });
      return async.each([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], function(id, next) {
        return jobs.create('sample-job-to-be-cleaned', {
          id: id
        }).save(next);
      }, done);
    });
    it('should be able to remove completed jobs', function(done) {
      return jobs.complete(function(err, ids) {
        should.not.exist(err);
        return async.each(ids, function(id, next) {
          return Job.remove(id, next);
        }, done);
      });
    });
    return it('should be able to remove failed jobs', function(done) {
      return jobs.failed(function(err, ids) {
        should.not.exist(err);
        return async.each(ids, function(id, next) {
          return Job.remove(id, next);
        }, done);
      });
    });
  });
});

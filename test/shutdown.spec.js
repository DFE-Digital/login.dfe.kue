var kue, should;

should = require('should');

kue = require('../');

describe('Kue', function() {
  before(function(done) {
    var jobs;
    jobs = kue.createQueue();
    return jobs.client.flushdb(done);
  });
  after(function(done) {
    var jobs;
    jobs = kue.createQueue();
    return jobs.client.flushdb(done);
  });
  return describe('Shutdown', function() {
    it('should return singleton from createQueue', function(done) {
      var jobs, jobsToo;
      jobs = kue.createQueue();
      jobsToo = kue.createQueue();
      jobs.should.equal(jobsToo);
      return jobs.shutdown(done);
    });
    it('should destroy singleton on shutdown', function(done) {
      var jobs;
      jobs = kue.createQueue();
      return jobs.shutdown(function(err) {
        var newJobs;
        // test that new jobs object is a different reference
        newJobs = kue.createQueue();
        newJobs.should.not.equal(jobs);
        return newJobs.shutdown(done);
      });
    });
    it('should clear properties on shutdown', function(done) {
      var jobs;
      jobs = kue.createQueue({
        promotion: {
          interval: 200
        }
      });
      return jobs.shutdown(function(err) {
        should(jobs.workers).be.empty;
        should(jobs.client).be.empty;
        should(jobs.promoter).be.empty;
        return done();
      });
    });
    it('should be able to pause/resume the worker', function(done) {
      var i, j, job_data, jobs, ref, total_jobs;
      jobs = kue.createQueue();
      job_data = {
        title: 'resumable jobs',
        to: 'tj@learnboost.com'
      };
      total_jobs = 3;
      for (i = j = 0, ref = total_jobs; (0 <= ref ? j < ref : j > ref); i = 0 <= ref ? ++j : --j) {
        jobs.create('resumable-jobs', job_data).save();
      }
      return jobs.process('resumable-jobs', 1, function(job, ctx, job_done) {
        job_done();
        if (!--total_jobs) {
          return jobs.shutdown(1000, done);
        } else {
          ctx.pause();
          return setTimeout(ctx.resume, 100);
        }
      });
    });
    it('should not clear properties on single type shutdown', function(testDone) {
      var fn, jobs;
      jobs = kue.createQueue();
      fn = function(err) {
        jobs.client.should.not.be.empty;
        return jobs.shutdown(10, testDone);
      };
      return jobs.shutdown(10, 'fooJob', fn);
    });
    it('should shutdown one worker type on single type shutdown', function(testDone) {
      var fn, jobs;
      jobs = kue.createQueue();
      // set up two worker types
      jobs.process('runningTask', function(job, done) {
        return done();
      });
      jobs.workers.should.have.length(1);
      jobs.process('shutdownTask', function(job, done) {
        return done();
      });
      jobs.workers.should.have.length(2);
      fn = function(err) {
        var j, len, ref, worker;
        ref = jobs.workers;
        // verify shutdownTask is not running but runningTask is
        for (j = 0, len = ref.length; j < len; j++) {
          worker = ref[j];
          switch (worker.type) {
            case 'shutdownTask':
              worker.should.have.property('running', false);
              break;
            case 'runningTask':
              worker.should.have.property('running', true);
          }
        }
        // kue should still be running
        jobs.promoter.should.not.be.empty;
        jobs.client.should.not.be.empty;
        return jobs.shutdown(10, testDone);
      };
      return jobs.shutdown(10, 'shutdownTask', fn);
    });
    it('should fail active job when shutdown timer expires', function(testDone) {
      var jobId, jobs, waitForJobToRun;
      jobs = kue.createQueue();
      jobId = null;
      jobs.process('long-task', function(job, done) {
        var fn;
        jobId = job.id;
        fn = function() {
          return done();
        };
        return setTimeout(fn, 10000);
      });
      jobs.create('long-task', {}).save();
      // need to make sure long-task has had enough time to get into active state
      waitForJobToRun = function() {
        var fn;
        fn = function(err) {
          return kue.Job.get(jobId, function(err, job) {
            job.should.have.property('_state', "failed");
            job.should.have.property('_error', "Shutdown");
            return testDone();
          });
        };
        // shutdown timer is shorter than job length
        return jobs.shutdown(10, fn);
      };
      return setTimeout(waitForJobToRun, 50);
    });
    it('should not call graceful shutdown twice on subsequent calls', function(testDone) {
      var jobs;
      jobs = kue.createQueue();
      jobs.process('test-subsequent-shutdowns', function(job, done) {
        done();
        setTimeout(function() {
          return jobs.shutdown(100, function(err) {
            return should.not.exist(err);
          });
        }, 50);
        return setTimeout(function() {
          return jobs.shutdown(100, function(err) {
            should.exist(err, 'expected `err` to exist');
            err.should.be.an.instanceOf(Error).with.property('message', 'Shutdown already in progress');
            return testDone();
          });
        }, 60);
      });
      return jobs.create('test-subsequent-shutdowns', {}).save();
    });
    return it('should fail active re-attemptable job when shutdown timer expires', function(testDone) {
      var jobId, jobs, waitForJobToRun;
      jobs = kue.createQueue();
      jobId = null;
      jobs.process('shutdown-reattemptable-jobs', function(job, done) {
        jobId = job.id;
        return setTimeout(done, 500);
      });
      jobs.create('shutdown-reattemptable-jobs', {
        title: 'shutdown-reattemptable-jobs'
      }).attempts(2).save();
      // need to make sure long-task has had enough time to get into active state
      waitForJobToRun = function() {
        var fn;
        fn = function(err) {
          return kue.Job.get(jobId, function(err, job) {
            job.should.have.property('_state', "inactive");
            job.should.have.property('_attempts', 1);
            job.should.have.property('_error', "Shutdown");
            return testDone();
          });
        };
        // shutdown timer is shorter than job length
        return jobs.shutdown(100, fn);
      };
      return setTimeout(waitForJobToRun, 50);
    });
  });
});

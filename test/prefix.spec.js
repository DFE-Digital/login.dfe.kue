var kue;

kue = require('../');

describe('Kue - Prefix', function() {
  var makeJobs, stopJobs;
  makeJobs = function(queueName) {
    var jobs, opts;
    opts = {
      prefix: queueName,
      promotion: {
        interval: 10
      }
    };
    jobs = kue.createQueue(opts);
    return jobs;
  };
  stopJobs = function(jobs, callback) {
    return jobs.shutdown(callback);
  };
  // expected redis activity

  // 1397744169.196792 "subscribe" "q:events"
  // 1397744169.196852 "unsubscribe"
  it('should use prefix q by default', function(done) {
    var jobs;
    jobs = kue.createQueue();
    jobs.client.prefix.should.equal('q');
    return stopJobs(jobs, done);
  });
  // expected redis activity

  // 1397744498.330456 "subscribe" "testPrefix1:events"
  // 1397744498.330638 "unsubscribe"
  // 1397744498.330907 "subscribe" "testPrefix2:events"
  // 1397744498.331148 "unsubscribe"
  it('should accept and store prefix', function(done) {
    var jobs;
    jobs = makeJobs('testPrefix1');
    jobs.client.prefix.should.equal('testPrefix1');
    return stopJobs(jobs, function(err) {
      var jobs2;
      jobs2 = makeJobs('testPrefix2');
      jobs2.client.prefix.should.equal('testPrefix2');
      return stopJobs(jobs2, done);
    });
  });
  it('should process and complete a job using a prefix', function(testDone) {
    var job, jobs;
    jobs = makeJobs('simplePrefixTest');
    job = jobs.create('simplePrefixJob');
    job.on('complete', function() {
      return stopJobs(jobs, testDone);
    });
    job.save();
    return jobs.process('simplePrefixJob', function(job, done) {
      return done();
    });
  });
  // expected redis activity

  // 1397744498.333423 "subscribe" "jobCompleteTest:events"
  // 1397744498.334002 "info"
  // 1397744498.334358 "zcard" "jobCompleteTest:jobs:inactive"
  // 1397744498.335262 "info"
  // 1397744498.335578 "incr" "jobCompleteTest:ids"
  // etc...
  it('store queued jobs in different prefixes', function(testDone) {
    var jobs;
    jobs = makeJobs('jobCompleteTest');
    return jobs.inactiveCount(function(err, count) {
      var f, prevCount;
      prevCount = count;
      jobs.create('fakeJob', {}).save();
      f = function() {
        return jobs.inactiveCount(function(err, count) {
          count.should.equal(prevCount + 1);
          return stopJobs(jobs, testDone);
        });
      };
      return setTimeout(f, 10);
    });
  });
  it('should not pick up an inactive job from another prefix', function(testDone) {
    var job, jobs;
    jobs = makeJobs('inactiveJobs');
    // create a job but do not process
    return job = jobs.create('inactiveJob', {}).save(function(err) {
      // stop the 'inactiveJobs' prefix
      return stopJobs(jobs, function(err) {
        jobs = makeJobs('inactiveJobs2');
        // verify count of inactive jobs is 0 for this prefix
        return jobs.inactiveCount(function(err, count) {
          count.should.equal(0);
          return stopJobs(jobs, testDone);
        });
      });
    });
  });
  return it('should properly switch back to default queue', function(testDone) {
    var jobs;
    jobs = makeJobs('notDefault');
    return stopJobs(jobs, function(err) {
      var job;
      jobs = kue.createQueue();
      job = jobs.create('defaultPrefixJob');
      job.on('complete', function() {
        return stopJobs(jobs, testDone);
      });
      job.save();
      return jobs.process('defaultPrefixJob', function(job, done) {
        return done();
      });
    });
  });
});

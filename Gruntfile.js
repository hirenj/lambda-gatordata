/**
 * Upload all exported functions to lambda
 */
'use strict';
module.exports = function(grunt) {
  require('load-grunt-tasks')(grunt);

  var path = require('path');

  var config = {'functions' : {} };
  try {
    config = require('./resources.conf.json');
  } catch (e) {
  }

  process.env['AWS_REGION'] = config.region;

  var self_funcs = Object.keys(require('./index')).filter( (func) => config.functions[func] );

  var config_options = {
    'git-describe': {
      options: {},
      default: {}
    },
    lambda_package: {
      common: {
        package: 'package'
      }
    },
    env: {
      prod: {
        NODE_ENV: 'production',
      },
    }

  };
  config_options['lambda_deploy'] = {};

  self_funcs.forEach( (funcname) => {
    config_options['lambda_deploy'][funcname] = {
        options: {
          file_name: 'index.js',
          handler: 'index.'+funcname,
          region: config.region,
        },
        function: config.functions[funcname] || funcname,
        arn: null,
    };
  });

  config_options['lambda_checkversion'] = config_options['lambda_setversion'] = config_options['lambda_package_targets'] = {};
  Object.keys(config_options['lambda_deploy']).forEach(function(targ) {
    config_options['lambda_checkversion'][targ] = true;
    config_options['lambda_setversion'][targ] = true;
    config_options['lambda_package_targets'][targ] = true;
  });

  grunt.initConfig(config_options);

  grunt.registerTask('saveRevision', function() {

      var branch = 'none';
      var done = this.async();

      grunt.event.once('git-describe', function (rev) {
        grunt.option('gitRevision', branch+'-'+rev);
      });

      grunt.util.spawn({
        cmd: 'git',
        args: ['symbolic-ref','--short','HEAD']
      }, function (error, result) {
          if (error) {
            grunt.log.error([error]);
          }
          branch = result;
          grunt.task.run('git-describe');
          done();
      });

  });

  grunt.registerMultiTask('lambda_package_targets','Check for version of lambda function',function(){
    if ( ! grunt.option('packagepath') ) {
      grunt.option('packagepath',grunt.config('lambda_deploy.common.package'));
      grunt.config('lambda_deploy.common',null);
    }
    if (grunt.config('lambda_deploy.'+this.target)) {
      grunt.config('lambda_deploy.'+this.target+'.package',grunt.option('packagepath'));
    }
  });

  grunt.registerMultiTask('lambda_checkversion','Check for version of lambda function',function(){
    grunt.task.requires('git-describe');
    var done = this.async();
    var target = this.target;
    if ( ! grunt.config().lambda_deploy[target] ) {
      grunt.log.writeln("No arn");
      grunt.config('lambda_package.'+target,null);
      grunt.config('lambda_deploy.'+target,null);
      grunt.config('lambda_setversion.'+target,null);
      done();
      return;
    }
    var arn = grunt.config().lambda_deploy[target].function;
    var AWS = require('aws-sdk');
    var lambda = new AWS.Lambda();
    lambda.getFunctionConfiguration({FunctionName: arn},function(err,data) {
      var git_status = grunt.option('gitRevision');
      if (git_status.dirty) {
        grunt.log.writeln("Git repo is dirty, updating by default");
      } else {
        var current_version = data.Description;
        if (current_version === git_status.toString()) {
          grunt.config('lambda_package.'+target,null);
          grunt.config('lambda_deploy.'+target,null);
          grunt.config('lambda_setversion.'+target,null);
        }
      }
      done();
    });
  });

  grunt.registerMultiTask('lambda_setversion','Set version for lambda function',function(){
    grunt.task.requires('git-describe');
    var done = this.async();
    var target = this.target;
    if ( ! grunt.config().lambda_deploy[target] ) {
      grunt.log.writeln("No arn");
      done();
      return;
    }
    var arn = grunt.config().lambda_deploy[target].function;
    var AWS = require('aws-sdk');
    var lambda = new AWS.Lambda();
    grunt.log.writeln("Setting version for "+target+" to ",grunt.option('gitRevision').toString());
    lambda.updateFunctionConfiguration({FunctionName: arn,Description: grunt.option('gitRevision').toString(),VpcConfig: { SecurityGroupIds: [], SubnetIds: [] } },function(err,data) {
      if ( ! err ) {
        done();
      } else {
        grunt.fail.fatal(err);
      }
    });
  });

  grunt.registerTask('versioncheck',['saveRevision']);

  grunt.registerTask('deploy', function(func) {
    var tasks = ['lambda_deploy','lambda_setversion'];
    func = (func ? (':' + func) : '');
    grunt.task.run( ['env:prod','versioncheck','lambda_checkversion'+func,'force:lambda_package','force:lambda_package_targets' ].concat(tasks.map( (task) => 'force:'+task+func) ));
  });
  grunt.registerTask('test', ['lambda_invoke']);
};

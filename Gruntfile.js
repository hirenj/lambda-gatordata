/**
 * Grunt Uploader for Lambda scripts
 * Updated from original by Chris Moyer <cmoyer@aci.info>
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

	grunt.initConfig({
		lambda_invoke: {
			splitFile: {
				package: 'gatordata',
				options: {
					file_name: 'index.js',
					handler: 'splitFile',
					event: 'event.json',
				},
			},
		},
		lambda_deploy: {
			splitFile: {
				package: 'gatordata',
				options: {
					file_name: 'index.js',
					handler: 'index.splitFile',
				},
        region: config.region,
				function: config.functions['splitFiles'] || 'splitFiles',
				arn: null,
			},
			runSplitQueue: {
				package: 'gatordata',
				options: {
					file_name: 'index.js',
					handler: 'index.runSplitQueue',
				},
        region: config.region,
				function: config.functions['runSplitQueue'] || 'runSplitQueue',
				arn: null,
			},
			readAllData: {
				package: 'gatordata',
				options: {
					file_name: 'index.js',
					handler: 'index.readAllData',
				},
        region: config.region,
				function: config.functions['readAllData'] || 'readAllData',
				arn: null,
			}

		},
		lambda_package: {
			splitFile: {
				package: 'gatordata',
			},
			readAllData: {
				package: 'gatordata',
			},
			runSplitQueue: {
				package: 'gatordata',
			}
		},
		env: {
			prod: {
				NODE_ENV: 'production',
			},
		},

	});

	grunt.registerTask('deploy:readAllData', ['env:prod', 'lambda_package:readAllData', 'lambda_deploy:readAllData']);
	grunt.registerTask('deploy:runSplitQueue', ['env:prod', 'lambda_package:runSplitQueue', 'lambda_deploy:runSplitQueue']);
	grunt.registerTask('deploy:splitFile', ['env:prod', 'lambda_package:splitFile', 'lambda_deploy:splitFile']);
	grunt.registerTask('deploy', ['env:prod', 'lambda_package', 'lambda_deploy']);
	grunt.registerTask('test', ['lambda_invoke']);
};

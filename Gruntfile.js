/**
 * Grunt Uploader for Lambda scripts
 * Updated from original by Chris Moyer <cmoyer@aci.info>
 */
'use strict';
module.exports = function(grunt) {
	require('load-grunt-tasks')(grunt);

	var path = require('path');
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
				function: "test-splitFiles-Q2RTAGZXK6ON",
				arn: null,
			},
			readAllData: {
				package: 'gatordata',
				options: {
					file_name: 'index.js',
					handler: 'index.readAllData',
				},
				function: "test-readAllData-Q2RTAGZXK6ON",
				arn: null,
			}

		},
		lambda_package: {
			splitFile: {
				package: 'gatordata',
			},
			readAllData: {
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
	grunt.registerTask('deploy:splitFile', ['env:prod', 'lambda_package:splitFile', 'lambda_deploy:splitFile']);
	grunt.registerTask('test', ['lambda_invoke']);
};

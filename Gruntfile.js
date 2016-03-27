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
			default: {
				package: 'gatordata',
				options: {
					file_name: 'index.js',
					handler: 'splitFile',
					event: 'event.json',
				},
			},
		},
		lambda_deploy: {
			default: {
				package: 'gatordata',
				options: {
					file_name: 'index.js',
					handler: 'index.splitFile',
				},
				function: "test-splitFiles-Q2RTAGZXK6ON",
				arn: null,
			}
		},
		lambda_package: {
			default: {
				package: 'gatordata',
			}
		},
		env: {
			prod: {
				NODE_ENV: 'production',
			},
		},

	});

	grunt.registerTask('deploy', ['env:prod', 'lambda_package', 'lambda_deploy']);
	grunt.registerTask('test', ['lambda_invoke']);
};

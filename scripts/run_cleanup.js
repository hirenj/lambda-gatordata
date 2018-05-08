'use strict';

process.on('unhandledRejection', error => {
  // Will print "unhandledRejection err is not defined"
  console.log('unhandledRejection', error.message);
});

let index = require('./index');

let function_promise = (func) => {
  return (input => {
    return new Promise( (resolve,reject) => {
      func(input, {
        succeed: val => resolve(val),
        fail: err => reject(err)
      });
    });
  });
}

let cleanup = function_promise(index.datasetCleanup);

let execution_message = null;

let show_result = (finished) => {
  console.log('FINISHED',finished);
};

let run_lambda = (message) => {
  console.log(new Array(80).join('*'),'STARTING NEW EXECUTION',new Array(80).join('*'));
  cleanup(message).then( result => {
    if (result && result.status == 'OK') {
      return show_result(result);
    }
    if (result && result.status === 'RUNNING') {
      setTimeout( run_lambda.bind(null, result),100);
    }
    console.log('No result',result);
  }).catch( err => {
    console.error('ERROR',err);
  });
};

run_lambda(execution_message);
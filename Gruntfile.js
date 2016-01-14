'use strict';

module.exports = function(grunt) {

	grunt.initConfig({
	  mongobackup: {
		dump : {
		  options: {
			host : 'localhost',
			out : './dumps/mongo'
		  }
		},
		restore: {
		  options: {
			host : 'localhost',
			drop : true,
			path : './dumps/mongo/testdb'
		  }
		}
	  }
	});
  
	grunt.loadNpmTasks('mongobackup');

  //grunt.loadNpmTasks('grunt-contrib-jshint');
  //grunt.loadNpmTasks('grunt-contrib-watch');

  // A very basic default task.
	grunt.registerTask('default', 'Log some stuff.', function() {
		grunt.log.write('Logging some stuff...').ok();
		
		grunt.task.run([
            'mongobackup:dump',
        ]);
	});
};
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
	
	grunt.registerTask('import', 'Importiert csv-Dateien', function(file1, name, file2) {
		
		//grunt.log.writeln(file1);
		//grunt.log.writeln(name);
		//grunt.log.writeln(file2);
		
		//Prüfen, ob die Dateien richtig sind
		if (!grunt.file.isFile(file1)) {
			grunt.log.error("Die Datei mit den Gebäudeinformationen wurden nicht richtig eingetragen!");
		} else if (!grunt.file.isFile(file1)) {
			grunt.log.error("Die Datei mit den Verbindungen der Gebäuden wurde nicht richtig eingetragen!");	
		} else {
			
			//grunt.file.isFile(path1 [, path2 [, ...]])

			//grunt.file.read(filepath [, options])
			var file = grunt.file.read(file1);
			//var file = grunt.file.read('Beispieldatei.csv');
			//grunt.log.write(file);
			//var connections = grunt.file.read('Beispieldatei_Verbindungen.csv');
			var connections = grunt.file.read(file2);
			//grunt.log.write(connections);
			
			/*
			grunt.task.run([
				'mongobackup:dump',
			]);
			*/
		}
		

	});
	
	//Dies ist nur eine Beispielfunktion
	grunt.registerTask('foo', 'A sample task that logs stuff.', function(arg1, arg2) {
	  if (arguments.length === 0) {
		grunt.log.writeln(this.name + ", no args");
	  } else {
		grunt.log.writeln(this.name + ", " + arg1 + " " + arg2);
	  }
	});
};




















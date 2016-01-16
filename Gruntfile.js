'use strict';
//
// Einstellungen
//
var MONGO_HOST = "pegenau.com";
var MONGO_PORT = "16391";
var MONGO_USER = "mongoduser";
var MONGO_PASS = "6cn7Hd8RGzrseqmB";
// Datenbank mit Datensätzen
var DB = "prelife";

var MongoClient = require('mongodb').MongoClient
    , assert = require('assert');

var URL = 'mongodb://' + MONGO_HOST + ':' + MONGO_PORT + '/' + DB;

/*
Lösung für falsche $max Ergebnisse:
(leere) Strings sind größer als Zahlen
db.Beispieldatensatz.aggregate([{"$group":{"_id":0, "max_Klassen": {"$max": {"$cond": [{"$eq":["$Klassen", ""]}, 0, "$Klassen"]} } }}])
*/

var connect = function (fn) {
    MongoClient.connect(URL, function (err, db) {
        if (err) {
            console.error("Fehler bei Verbindungsaufbau zu" + URL);
            console.error(err);
            return false;
        } else {
            // Authenticate
            db.authenticate(MONGO_USER, MONGO_PASS).then(function (result) {
                //test.equal(true, result);
                if (fn) {
                    fn(db);
                    db.close();
                } else {
                    console.log("Keine Funktion. Schließe Verbindung");
                    db.close();
                }
            }, function (error) {
                console.error("Fehler bei Authentifizierung:");
                console.error(error);
            });
        }
    });
};

module.exports = function (grunt) {

    grunt.initConfig({
        mongobackup: {
            dump: {
                options: {
                    host: 'localhost',
                    out: './dumps/mongo'
                }
            },
            restore: {
                options: {
                    host: 'localhost',
                    drop: true,
                    path: './dumps/mongo/testdb'
                }
            }
        }
    });

    grunt.loadNpmTasks('mongobackup');

    grunt.registerTask('import', 'Importiert csv-Dateien', function (file1, name, file2) {
		
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
    
    /**
     * Grunt Task: metadata
     * 
     * Erzeugt eine Aggregation der Meta-Daten einer Collection und speichert sie in einer Collection
     */
    grunt.registerTask('metadata', 'Speichert Meta-Daten einer Collection', function (collectionName) {
        if (arguments.length === 0 || !collectionName) {
            grunt.log.error("Name der Collection fehlt!");
            return false;
        } else {
            var done = this.async();
            connect(function (db) {
                db.collection(collectionName, function (err, collection) {
                    if(err) {
                        grunt.log.error('Fehler beim Holen der Collection ' + collectionName);
                        grunt.log.error(err);
                        return false;
                    } else {
                        // AGGREGATION durchführen
                    }
                    /*
                    collection.aggregate('[{"$group":{"_id":0, "max_Klassen": {"$max": {"$cond": [{"$eq":["$Klassen", ""]}, 0, "$Klassen"]} } }}]', null, function(err, resp) {
                        console.log(resp);
                    });
                    */
                });
            });
        }
    });
	
    //Dies ist nur eine Beispielfunktion
    grunt.registerTask('foo', 'A sample task that logs stuff.', function (arg1, arg2) {
        if (arguments.length === 0) {
            grunt.log.writeln(this.name + ", no args");
        } else {
            grunt.log.writeln(this.name + ", " + arg1 + " " + arg2);
        }
    });
};




















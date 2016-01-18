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

var META_DATA_AGGR_URI = "maxminavg";
var META_DATA_PART = "_dc_";


var MongoClient = require('mongodb').MongoClient
    , assert = require('assert'),
    co = require('co');

var URL = 'mongodb://' + MONGO_HOST + ':' + MONGO_PORT + '/' + DB;

/*
Lösung für falsche $max Ergebnisse:
(leere) Strings sind größer als Zahlen
db.Beispieldatensatz.aggregate([{"$group":{"_id":0, "max_Klassen": {"$max": {"$cond": [{"$eq":["$Klassen", ""]}, 0, "$Klassen"]} } }}])
*/


/**
 * Ruft die übergebene Funktion fn auf und übergibt ihr die erhaltene Collection als Parameter
 * 
 * Um den Verbindungsaufbau muss sich nicht gekümmert werden.
 */
var getCollection = function (db, collectionName) {
    var ret = null;
    db.collection(collectionName, function (err, collection) {
        if (err) {
            console.error('Fehler beim Holen der Collection ' + collectionName);
            console.error(err);
            return false;
        } else {
            ret = collection;
        }
    });
    return ret;
}

var getDb = function () {
    var database;
    MongoClient.connect(URL, function (err, db) {
        if (err) {
            console.error("Fehler bei Verbindungsaufbau zu" + URL);
            console.error(err);
            return false;
        } else {
            // Authenticate
            db.authenticate(MONGO_USER, MONGO_PASS).then(function (result) {
                //test.equal(true, result);
                //fn(db);
                //closeDB(db);
                database = db;
                return db;
            }, function (error) {
                console.error("Fehler bei Authentifizierung:");
                console.error(error);
            });
        }
    });
    return database;
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
            MongoClient.connect(URL, function (err, db) {
                if (err) {
                    grunt.error.writeln("Fehler bei Verbindungsaufbau:" + URL);
                    grunt.error.writeln(err);
                    db.close();
                } else {
                    db.authenticate(MONGO_USER, MONGO_PASS, function (e) {
                        if (err) {
                            console.error("Fehler bei Authentifizierung:");
                            console.error(e);
                            db.close();
                        } else {
                            db.collection(collectionName, null, function (errorGetDB, collection) {
                                if (errorGetDB) {
                                    console.error("Fehler beim Holen der Datenbank " + collectionName);
                                    console.error(errorGetDB);
                                    db.close();
                                } else {
                                    collection.findOne({}, {}, function (errorGetDoc, doc) {
                                        if (errorGetDoc) {
                                            console.error("Fehler beim Holen eines Dokuments");
                                            console.error(errorGetDoc);
                                            db.close();
                                        } else {
                                            if (!doc) {
                                                console.error("Keine Dokumente in Collection gefunden!");
                                                console.error(errorGetDoc);
                                                db.close();
                                                return;
                                            } else {
                                                // Die Namen aller Attribute in attrs
                                                var attrs = [];
                                                for (var attr in doc) {
                                                    if (attr[0] !== '_') {
                                                        attrs.push(attr);
                                                    }
                                                }
                                                
                                                // Aggregation erstellen
                                                var stages = [{
                                                    "$group": {}
                                                }];
                                                var ops = {
                                                    "_id": 0,
                                                };
                                                attrs.forEach(function (name) {

                                                    var max = "max_" + name;
                                                    var min = "min_" + name;
                                                    var avg = "avg_" + name;

                                                    ops[max] = {
                                                        // Leere Felder werden von der MongoDB als Strings interpretiert
                                                        // Leere Strings sind länger als Zahlen
                                                        // => Maximum ohne diese Konstruktion ist ''
                                                        "$max": { "$cond": [{"$eq":["$" + name, ""]}, 0, "$" + name] }
                                                    };
                                                    ops[min] = {
                                                        "$min": "$" + name
                                                    };
                                                    ops[avg] = {
                                                        "$avg": "$" + name
                                                    };
                                                });
                                                stages[0].$group = ops;
                                                var outputCollection = collectionName + META_DATA_PART + META_DATA_AGGR_URI;
                                                stages.push({ "$out": outputCollection});
                                                
                                                // Aggregation ausführen
                                                collection.aggregate(stages, function (errorAggregation, result) {
                                                    if (errorGetDoc) {
                                                        console.error("Fehler beim Aggregieren");
                                                        console.error(errorAggregation);
                                                        db.close();
                                                    } else {
                                                        grunt.log.writeln("Aggregation erfolgreich");
                                                        db.close();
                                                    }
                                                });
                                            }
                                        }
                                    });
                                }
                            });

                        }
                    });
                }
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




















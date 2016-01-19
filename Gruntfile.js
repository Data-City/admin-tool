'use strict';
//
// Einstellungen
//
var MONGO_HOST = "pegenau.com";
var MONGO_PORT = "16391";
var MONGO_USER = "mongoduser";
var MONGO_PASS = "6cn7Hd8RGzrseqmB";
var MONGO_AUTH_DB = "admin";
var DROP_EXISTING_COLLECTIONS = true;
// Datenbank mit Datensätzen
var DB = "prelife";

var TMP_CONNECTIONS_SUFFIX = "connections_tmp";
var META_DATA_AGGR_URI = "maxminavg";
var META_DATA_PART = "_dc_";


var MongoClient = require('mongodb').MongoClient
    , assert = require('assert'),
    shell = require('shelljs');;

var URL = 'mongodb://' + MONGO_HOST + ':' + MONGO_PORT + '/' + DB;


var importCsvFile = function (filename, collectionName) {
    var importCmd = 'mongoimport --host ' + MONGO_HOST + ':' + MONGO_PORT +
        ' --db ' + DB +
        ' --collection ' + collectionName +
        ' --type csv --headerline --file ' + filename +
        ' --authenticationDatabase ' + MONGO_AUTH_DB +
        ' --username ' + MONGO_USER +
        ' --password ' + MONGO_PASS + ' -v';
    if (DROP_EXISTING_COLLECTIONS) {
        importCmd += ' --drop';
    }
    //console.log("Import-Befehl: " + importCmd);    
    var importCsv = shell.exec(importCmd);

    if (importCsv.code !== 0) {
        console.error("Es ist ein Fehler aufgetreten:");
        console.error("Code: " + importCsv.code);
        console.error("Output:");
        console.error(importCsv.output);
        return false;
    }
}




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

    grunt.registerTask('import', 'Importiert csv-Dateien', function (data_csv, collectionName, connections_csv) {
        if (!data_csv || !collectionName) {
            grunt.log.error("Nötige Parameter fehlen! Aufruf mit");
            grunt.log.error("grunt import:Daten.csv:NameDerCollection:Verbindungen.csv");
            grunt.log.error("Daten.csv\t\tKomma-separierte Datei mit den zu visualisierenden Daten");
            grunt.log.error("NameDerCollection\tName der Collection in die importiert werden soll");
            grunt.log.error("Verbindungen.csv\tOPTIONAL: Komma-separierte Datei mit Verbindungsdaten");
            return false;
        }
        
        //Prüfen, ob Input-CSV existiert
        if (!grunt.file.isFile(data_csv)) {
            grunt.log.error("Datei mit zu visualisierenden Daten (Gebaeudeinformationen) wurde nicht gefunden!");
            return false;
        } 
        
        // Falls angegeben sollte auch die CSV-Datei mit den Verbindungen passen
        if (connections_csv && !grunt.file.isFile(connections_csv)) {
            grunt.log.error("Die Datei mit den Verbindungen der Gebäuden wurde nicht gefunden! Falscher Pfad?");
            return false;
        }

        // Datei importieren - Daten
        importCsvFile(data_csv, collectionName);
        
        // Datei importieren - Verbindungsdaten
        if (connections_csv) {
            importCsvFile(connections_csv, collectionName + META_DATA_PART + TMP_CONNECTIONS_SUFFIX);
        }
        
        // Meta Daten aggregieren
        grunt.task.run('metadata:' + collectionName);
                
        // Verbindungsdaten aufbereiten
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
                    grunt.log.error("Fehler bei Verbindungsaufbau:" + URL);
                    grunt.log.error(err);
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
                                                        "$max": { "$cond": [{ "$eq": ["$" + name, ""] }, 0, "$" + name] }
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
                                                stages.push({ "$out": outputCollection });
                                                
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




















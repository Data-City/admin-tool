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


// Ab hier muss wahrscheinlich nichts mehr angepasst werden
var TMP_CONNECTIONS_SUFFIX = "connections_tmp";
var CONNECTIONS_SUFFIX = "connections";
var OUTGOING_SUFFIX = "_outgoing";
var INCOMING_SUFFIX = "_incoming";
var META_DATA_AGGR_URI = "maxminavg";
var META_DATA_PART = "_dc_";

// Ab hier sollte definitiv nichts mehr angepasst werden
var MongoClient = require('mongodb').MongoClient
    , assert = require('assert'),
    shell = require('shelljs');;

var URL = 'mongodb://' + MONGO_HOST + ':' + MONGO_PORT + '/' + DB;

    
/**
 * Bereitet temporäre Verbindungsdaten auf
 * 
 * Baut u.a. einen Rahmen um Einträge, sodass REST-Limit von max 1000 ausgelieferten
 * Dokumenten umgangen wird
 */
var prepareConnections = function (collectionName) {
    MongoClient.connect(URL, function (err, db) {
        if (err) {
            console.error("Fehler bei Verbindungsaufbau:" + URL);
            console.error(err);
            db.close();
        } else {
            db.authenticate(MONGO_USER, MONGO_PASS, function (e) {
                if (err) {
                    console.error("Fehler bei Authentifizierung:");
                    console.error(e);
                    db.close();
                } else {
                    db.collection(collectionName + META_DATA_PART + TMP_CONNECTIONS_SUFFIX, null, function (errorGetDB, collection) {
                        if (errorGetDB) {
                            console.error("Fehler beim Holen der Datenbank " + collectionName);
                            console.error(errorGetDB);
                            db.close();
                        } else {
                            // Stages definieren
                            var outgoingStages = [
                                {
                                    $group: {
                                        _id: "$Start",
                                        outgoingConnections: {
                                            $addToSet: {
                                                Ziel: "$Ziel",
                                                Gewichtung: "$Gewichtung"
                                            }
                                        },
                                        Gewichtung: {
                                            $sum: "$Gewichtung"
                                        }
                                    },
                                }, {
                                    $group: {
                                        _id: "Outgoing Connections",
                                        connections: {
                                            $addToSet: {
                                                Start: "$_id",
                                                outgoingConnections: "$outgoingConnections",
                                                Gewichtung: "$Gewichtung"
                                            }
                                        }
                                    }
                                }, {
                                    $out: collectionName + META_DATA_PART + CONNECTIONS_SUFFIX + OUTGOING_SUFFIX
                                }
                            ];

                            var incomingStages = [
                                {
                                    $group: {
                                        _id: "$Ziel",
                                        incomingConnections: {
                                            $addToSet: {
                                                Start: "$Start",
                                                Gewichtung: "$Gewichtung"
                                            }
                                        },
                                        Gewichtung: {
                                            $sum: "$Gewichtung"
                                        }
                                    },
                                }, {
                                    $group: {
                                        _id: "Incoming Connections",
                                        connections: {
                                            $addToSet: {
                                                Ziel: "$_id",
                                                incomingConnections: "$incomingConnections",
                                                Gewichtung: "$Gewichtung"
                                            }
                                        }
                                    }
                                }, {
                                    $out: collectionName + META_DATA_PART + CONNECTIONS_SUFFIX + INCOMING_SUFFIX
                                }
                            ];
                            
                            // Aggregation ausführen
                            collection.aggregate(incomingStages, function (errorAggregation, result) {
                                if (errorAggregation) {
                                    console.error("Fehler beim Aggregieren der Verbindungsdaten (incoming)");
                                    console.error(errorAggregation);
                                    db.close();
                                } else {
                                    console.log("Verbindungen (incoming) erfolgreich aufbereitet!");
                                    collection.aggregate(outgoingStages, function (errorAggregation, result) {
                                        if (errorAggregation) {
                                            console.error("Fehler beim Aggregieren der Verbindungsdaten (outgoing)");
                                            console.error(errorAggregation);
                                            db.close();
                                        } else {
                                            console.log("Verbindungen (outgoing) erfolgreich aufbereitet!");
                                            db.close();
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
};


/**
 * Importiert CSV-Datei (filename) in Collection (collectionName)
 */
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
    /**
     * Grunt Task: import
     * 
     * Importiert CSV-Dateien zur Darstellung der Häuser und Verbindungen, bereitet sie auf und sammelt Meta-Daten
     */
    grunt.registerTask('import', 'Importiert CSV-Dateien und bereitet sie auf', function (data_csv, collectionName, connections_csv) {
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
            // Verbindungsdaten aufbereiten
            prepareConnections(collectionName);
        }
        
        // Meta Daten aggregieren
        grunt.task.run('metadata:' + collectionName);
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
                                                    if (errorAggregation) {
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
};




















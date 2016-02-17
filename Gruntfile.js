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
                    },
                    minimum: {
                      $min: "$Gewichtung"
                    },

                    maximum: {
                      $max: "$Gewichtung"
                    },

                    average: {
                      $avg: "$Gewichtung"
                    }
                  },
                }, {
                  $group: {
                    _id: "Outgoing Connections",
                    connections: {
                      $addToSet: {
                        Start: "$_id",
                        outgoingConnections: "$outgoingConnections",
                        Gewichtung: "$Gewichtung",
                        minimum: "$minimum",
                        maximum: "$maximum",
                        average: "$average"
                      }
                    },
                    minimumglobalout: {
                      $min: "$minimum"
                    },

                    maximumglobalout: {
                      $max: "$maximum"
                    },

                    averageglobalout: {
                      $avg: "$average"
                    }

                  }
                },
                {
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
                    },

                    minimum: {
                      $min: "$Gewichtung"
                    },

                    maximum: {
                      $max: "$Gewichtung"
                    },

                    average: {
                      $avg: "$Gewichtung"
                    }

                  },
                }, {
                  $group: {
                    _id: "Incoming Connections",
                    connections: {
                      $addToSet: {
                        Ziel: "$_id",
                        incomingConnections: "$incomingConnections",
                        Gewichtung: "$Gewichtung",
                        minimum: "$minimum",
                        maximum: "$maximum",
                        average: "$average"
                      }
                    },
                    minimumglobalinc: {
                      $min: "$minimum"
                    },

                    maximumglobalinc: {
                      $max: "$maximum"
                    },

                    averageglobalinc: {
                      $avg: "$average"
                    }
                  }
                },
                {
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
                      console.log("Der Datensatz wurde erfolgeich importiert!");
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

/**
* Initialisierung der Datenbank
*
var initDB = function (collectionName) {

//remove exitisting database
var remove_admin_db_cmd = 'mongo ' + MONGO_AUTH_DB + ' --eval "db.dropDatabase()"';
var remove_einstellungen_db_cmd = 'mongo einstellungen --eval "db.dropDatabase()"';
var remove_prelife_db_cmd = 'mongo ' + DB + ' --eval "db.dropDatabase()"';
//create database
var add_users_data_cmd = 'mongo --eval "db.getSiblingDB('+ MONGO_AUTH_DB +').createUser({user: "mongoduser", pwd: "6cn7Hd8RGzrseqmB", roles:[{role :"readWrite", db: "local"},{role : "root", db : "admin"},{role : "readWrite",db : "production"},{role: "readWrite", db: "prelife"},{role: "readWrite", db: "admin"},{role: "userAdminAnyDatabase", db: "admin"}]})';
var add_ansichten_data_cmd = 'mongo --eval "db.getSiblingDB("einstellungen").createCollection(' + collectionName +', function(err, collection){collection.insert({"test":"value"});});'

var remove_admin_db = shell.exec(remove_admin_db_cmd);
var remove_einstellungen_db = shell.exec(remove_einstellungen_db_cmd);
var remove_prelife_db = shell.exec(remove_prelife_db_cmd);
var add_users_data = shell.exec(add_users_data_cmd);
var add_ansichten_data = shell.exec(add_ansichten_data_cmd);
}*/



module.exports = function (grunt) {
  /**
  * Grunt Task: initDB
  *
  *
  *
  grunt.registerTask('initDB', 'Initialisiert die Databank', function (collectionName) {
  initDB(collectionName);
});*/
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

  // Meta Daten aggregieren
  if(connections_csv) {
    grunt.task.run('metadata:' + collectionName + ":true");
  } else {
    grunt.task.run('metadata:' + collectionName + ":false");
  }

  // Datei importieren - Verbindungsdaten
  if (connections_csv) {
    importCsvFile(connections_csv, collectionName + META_DATA_PART + TMP_CONNECTIONS_SUFFIX);
    // Verbindungsdaten aufbereiten
    prepareConnections(collectionName);

    var done = this.async();


    connect().then(function(db) {
      // Darin werden die Verbindungsdaten gespeichert
      var incomingConnections = new Promise(function(resolve, reject) {
        var col = collectionName + META_DATA_PART + CONNECTIONS_SUFFIX + INCOMING_SUFFIX;
        getCollection(db, col).then(function(collection) {
          findOne(collection).then(function (doc) {
            resolve(doc);
          });
        });
      });

      var outgoingConnections = new Promise(function(resolve, reject) {
        var col = collectionName + META_DATA_PART + CONNECTIONS_SUFFIX + OUTGOING_SUFFIX;
        getCollection(db, col).then(function(collection) {
          findOne(collection).then(function (doc) {
            resolve(doc);
            
          });
        });
      });

      Promise.all([incomingConnections, outgoingConnections]).then(function(values) {
        incomingConnections = values[0];
        outgoingConnections = values[1];
        console.log("Incoming:");
        console.log(incomingConnections);
        console.log("Outgoing:");
        console.log(outgoingConnections);

       
            var metaDataCollection = collectionName + META_DATA_PART + META_DATA_AGGR_URI;

            getCollection(db, metaDataCollection).then(function(metaData) {
             var r = metaData.update({"_id" : 0},{
                $set: {
                  "minimumglobalout": minimumglobalout,
                  "maximumglobalout": maximumglobalout,
                  "averageglobalout": averageglobalout,
                  "minimumglobalinc": minimumglobalinc,
                  "maximumglobalinc": maximumglobalinc,
                  "averageglobalinc": averageglobalinc,
                  //"An Julia": "Gruesse von Steffen :-P"
                }
              }
            );
             console.log(r);
         
        
      });
    });


  });
}
});

/**
* Grunt Task: metadata
*
* Erzeugt eine Aggregation der Meta-Daten einer Collection und speichert sie in einer Collection
*/
grunt.registerTask('metadata', 'Speichert Meta-Daten einer Collection', function (collectionName, connectionsAvailable) {
  if (arguments.length === 0 || !collectionName) {
    grunt.log.error("Name der Collection fehlt!");
    return false;
  } else {
    var done = this.async();

    var connection = connect();

    connect().then(function(db) {
      getCollection(db, collectionName).then(function(collection) {
        findOne(collection).then(function(doc) {
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
          var aggregation = aggregate(collection, stages);

          aggregation.then(function(success) {
            var p = getCollection(db, outputCollection);
            p.then(function(maxminavg) {
              maxminavg.update({"_id" : 0},{$set:{"connectionsAvailable": connectionsAvailable}});
              db.close();
            });
          });
        });
      });
    });
  }
});
};

var findOne = function (collection, query, projection) {
  return new Promise(function (resolve, reject) {
    if(!query) {
      query = {};
    }

    if(!projection) {
      projection = {};
    }
    collection.findOne(query, projection, function (errorGetDoc, doc) {
      if (errorGetDoc) {
        console.error("Fehler beim Holen eines Dokuments");
        console.error(errorGetDoc);
        db.close();
        reject("Fehler beim Holen eines Dokuments: " + errorGetDoc);
      } else if (!doc) {
        console.error("Keine Dokumente in Collection gefunden!");
        console.error(errorGetDoc);
        db.close("Keine Dokumente in Collection gefunden: " + errorGetDoc);
        reject();
      }
      else {
        resolve(doc);
      }
    });
  });
};

/**
* Stellt Verbindung zur Datenbank her
* @return Promise Gibt Promise über Verbindung zurück. Enthält db-Objekt.
*/
var connect = function() {
  return new Promise(function (resolve, reject) {
    MongoClient.connect(URL, function (err, db) {
      if (err) {
        console.error("Fehler bei Verbindungsaufbau:" + URL);
        console.error(err);
        db.close();
        reject("Fehler bei Verbindungsaufbau:" + URL);
      } else {
        db.authenticate(MONGO_USER, MONGO_PASS, function (e) {
          if (e) {
            console.error("Fehler bei Authentifizierung:");
            console.error(e);
            db.close();
            reject("Fehler bei Authentifizierung:" + e);
          } else {
            resolve(db);
          }
        });
      }
    });
  });
};

var aggregate = function(collection, stages) {
  return new Promise(function(resolve, reject) {
    collection.aggregate(stages, function (errorAggregation, result) {
      if (errorAggregation) {
        console.error("Fehler beim Aggregieren");
        console.error(errorAggregation);
        reject("Fehler beim Aggregieren: " + errorAggregation);
      } else {
        console.log("Aggregation erfolgreich");
        resolve("Aggregation erfolgreich");
      }
    });
  });
};

var getCollection = function(db, collectionName) {
  return new Promise(
    function(resolve, reject) {
      db.collection(collectionName, null, function (error, collection) {
        if (error) {
          console.error("Fehler beim Holen von maxminavg");
          console.error(errorAggregation);
          db.close();
          reject("Fehler beim Holen der Collection '" + collectionName + "'");
        } else {
          resolve(collection);
        }
      });
    }
  );
};

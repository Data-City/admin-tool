# admin-tool

Ein Tool zum Import und zur Aufbereitung von Datensätzen und ihrer Verbindungen.

## Voraussetzungen:
* Node.js und npm installiert (https://nodejs.org)
* mongoimport liegt mindestens in Version 3.2 vor

  **Hinweis:**
Frühere Versionen könnten klappen. Tritt beim Import ein Authentifizierungsproblem auf, kann dies darauf hinweisen, dass die Version zu alt ist.
* Der Befehl mongoimport liegt im System-Pfad - [Anleitung für Windows 7](https://github.com/Data-City/admin-tool/blob/master/Anleitung Systempfad Win 7.txt)
* Das Node-Modul *grunt* ist installiert (http://gruntjs.com/)

## Installation:
* Mit **git clone https://github.com/Data-City/admin-tool.git** die Dateien herunterladen
* Mit **cd admin-tool** ins Verzeichnis wechseln
* Im Verzeichnis mit **npm install** alle Node-Abhängigkeiten installieren

## Nutzung:

Daten können mit
```
grunt import:Datensaetze.csv:CollectionName:Verbindungen.csv
```
importiert werden

#### Datensaetze.csv
Hierbei handelt es sich um den Namen einer CSV-Datei, die
* durch Kommata getrennt ist
* in der ersten Zeile die Spaltennamen enthält
* keine unötigen Leerzeilen enthält

###### Beispiel
```
Vorname,Beruf,Alter,Herkunft,Gehalt
Kate,Physikerin,36,UK,45000
Hans,Bäcker,52,DE,36000
Alexandro,Student,23,IT,8000
```
#### CollectionName
Der Name der Collection, in die importiert werden soll. 
* Existiert die Collection noch nicht, wird sie angelegt
* Existiert sie, wird der bisherige Inhalt vor dem Import gelöscht

**WICHTIG:** Keine Umlaute oder Sonderzeichen verwenden!

#### Verbindungen.csv
Grundsätzlich gelten die gleichen Regeln, wie für die Datei *Datensaetze.csv*. Ergänzend gilt:
* Die Datei enthält genau diese drei Spalten: *Start*, *Ziel*, *Gewichtung*
* Start und Ziel enthalten die Feldwerte, die später mit dem Namen assoziert werden.

###### Beispiel:
```
Start,Ziel,Gewichtung
Kate,Hans,5
Hans,Kate,300
Alexandro,Hans,150
```
**WICHTIG:** Damit die Anzeige der Verbindungen funktioniert, muss später im Frontend als Dimension *Name* die Spalte eingestellt werden, aus denen Start- und Ziel-Werte genommen wurden.
In diesem Beispiel: Die Dimension *Name* wird mit dem Feld *Vorname* belegt.

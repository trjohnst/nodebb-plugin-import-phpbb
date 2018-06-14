var fs = require("fs-extra");
var Exporter = require("../index");

Exporter.testrun(
  {
    dbhost: "localhost",
    dbport: 3306,
    dbname: "dbname",
    dbuser: "dbuser",
    dbpass: "dbpass",

    tablePrefix: "nuke_"
  },
  function(err, results) {
    if (err) {
      Exporter.teardown();
    }
  }
);

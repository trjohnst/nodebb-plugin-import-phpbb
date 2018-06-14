var fs = require("fs-extra");

require("./index").testrun(
  {
    dbhost: "localhost",
    dbport: 3306,
    dbname: "dbname",
    dbuser: "dbuser",
    dbpass: "dbpass",

    tablePrefix: "nuke_"
  },
  function(err, results) {
    fs.writeFileSync("./tmp.json", JSON.stringify(results, undefined, 2));
  }
);

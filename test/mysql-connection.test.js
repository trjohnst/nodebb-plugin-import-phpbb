var mysql = require("mysql");

var connection = mysql.createConnection({
  host: "localhost",
  user: "user",
  password: "password",
  port: 3306,
  database: "database-name"
});

connection.connect();

connection.query("SELECT * FROM nuke_users LIMIT 1;", function(err, rows) {
  if (err) {
    console.log(err);
    return;
  }

  console.log('Found a user in nuke_users with id', rows[0].user_id);
});

connection.end();

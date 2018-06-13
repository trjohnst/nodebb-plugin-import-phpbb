var async = require("async");
var mysql = require("mysql");
var _ = require("underscore");
var noop = function() {};
var logPrefix = "[nodebb-plugin-import-phpbb]";

(function(Exporter) {
  Exporter.setup = function(config, callback) {
    Exporter.log("setup");

    // mysql db only config
    // extract them from the configs passed by the nodebb-plugin-import adapter
    var _config = {
      host: config.dbhost || config.host || "localhost",
      user: config.dbuser || config.user || "root",
      password: config.dbpass || config.pass || config.password || "",
      port: config.dbport || config.port || 3306,
      database: config.dbname || config.name || config.database || "phpbb"
    };
    var _prefix = config.prefix || config.tablePrefix || "" /* nuke_ ? */;

    Exporter.log("starting with config ", _config);
    Exporter.log("starting with prefix ", _prefix);

    Exporter.config(_config);
    Exporter.config("prefix", _prefix);

    Exporter.log("connecting to mysql...");

    Exporter.connection = mysql.createConnection(_config);
    Exporter.connection.connect();

    Exporter.log("connected to mysql");

    callback(null, Exporter.config());
  };

  Exporter.getUsers = function(callback) {
    Exporter.log("getUsers");
    return Exporter.getPaginatedUsers(0, -1, callback);
  };
  Exporter.getPaginatedUsers = function(start, limit, callback) {
    Exporter.log("getPaginatedUsers");
    callback = !_.isFunction(callback) ? noop : callback;

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      prefix +
      "users.user_id as _uid, " +
      prefix +
      "users.username as _username, " +
      // prefix +
      // "users.username_clean as _alternativeUsername, " +
      prefix +
      "users.user_email as _registrationEmail, " +
      //+ prefix + 'users.user_rank as _level, '
      prefix +
      "users.user_regdate as _joindate, " +
      prefix +
      "users.user_sig as _signature, " +
      prefix +
      "users.user_website as _website, " +
      prefix +
      "users.user_posts as _reputation, " +
      prefix +
      "users.user_avatar as _picture, " +
      prefix +
      "users.user_email as _email " +
      //+ prefix + 'banlist.ban_id as _banned '
      //+ prefix + 'USER_PROFILE.USER_SIGNATURE as _signature, '
      // + prefix + 'USER_PROFILE.USER_HOMEPAGE as _website, '
      //+ prefix + 'USER_PROFILE.USER_OCCUPATION as _occupation, '
      //+ prefix + 'USER_PROFILE.USER_LOCATION as _location, '
      //+ prefix + 'USER_PROFILE.USER_AVATAR as _picture, '
      //+ prefix + 'USER_PROFILE.USER_TITLE as _title, '
      //+ prefix + 'USER_PROFILE.USER_RATING as _reputation, '
      //+ prefix + 'USER_PROFILE.USER_TOTAL_RATES as _profileviews, '
      //+ prefix + 'USER_PROFILE.USER_BIRTHDAY as _birthday '

      "FROM " +
      prefix +
      "users " +
      "WHERE " +
      prefix +
      "users.user_id = " +
      prefix +
      "users.user_id " +
      (start >= 0 && limit >= 0 ? "LIMIT " + start + "," + limit : "");

    if (!Exporter.connection) {
      err = { error: "MySQL connection is not setup. Run setup(config) first" };
      Exporter.error(err.error);
      return callback(err);
    }

    Exporter.connection.query(query, function(err, rows) {
      if (err) {
        Exporter.error(err);
        return callback(err);
      }

      //normalize here
      var map = {};
      rows.forEach(function(row) {
        Exporter.log("processing user ", _username);

        // nbb forces signatures to be less than 150 chars
        // keeping it HTML see https://github.com/akhoury/nodebb-plugin-import#markdown-note
        row._signature = Exporter.truncateStr(row._signature || "", 150);

        // from unix timestamp (s) to JS timestamp (ms)
        row._joindate = (row._joindate || 0) * 1000 || startms;

        // lower case the email for consistency
        row._email = (row._email || "").toLowerCase();

        // I don't know about you about I noticed a lot my users have incomplete urls, urls like: http://
        row._picture = Exporter.validateUrl(row._picture);
        row._website = Exporter.validateUrl(row._website);

        map[row._uid] = row;
      });

      callback(null, map);
    });
  };

  Exporter.getCategories = function(callback) {
    Exporter.log("getCategories");
    return Exporter.getPaginatedCategories(0, -1, callback);
  };
  Exporter.getPaginatedCategories = function(start, limit, callback) {
    Exporter.log("getPaginatedCategories");
    callback = !_.isFunction(callback) ? noop : callback;

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      prefix +
      "bbforums.forum_id as _cid, " +
      prefix +
      "bbforums.forum_name as _name, " +
      prefix +
      "bbforums.forum_desc as _description " +
      "FROM " +
      prefix +
      "bbforums " +
      (start >= 0 && limit >= 0 ? "LIMIT " + start + "," + limit : "");

    if (!Exporter.connection) {
      err = { error: "MySQL connection is not setup. Run setup(config) first" };
      Exporter.error(err.error);
      return callback(err);
    }

    Exporter.connection.query(query, function(err, rows) {
      if (err) {
        Exporter.error(err);
        return callback(err);
      }

      //normalize here
      var map = {};
      rows.forEach(function(row) {
        Exporter.log("processing category ", row._name);
        row._name = row._name || "Untitled Category";
        row._description = row._description || "No decsciption available";
        row._timestamp = (row._timestamp || 0) * 1000 || startms;

        map[row._cid] = row;
      });

      callback(null, map);
    });
  };

  Exporter.getTopics = function(callback) {
    Exporter.log("getTopics");
    return Exporter.getPaginatedTopics(0, -1, callback);
  };
  Exporter.getPaginatedTopics = function(start, limit, callback) {
    Exporter.log("getPaginatedTopics");
    callback = !_.isFunction(callback) ? noop : callback;

    // uses nuke_bbtopics, nuke_bbposts, nuke_bbposts_text
    // nuke_bbtopics:
    //   topic_id
    //   forum_id
    //   topic_first_post_id
    //   topic_views
    //   topic_title
    //   topic_views
    //   topic_time
    //   topic_status
    // nuke_bbposts: (has post id and corresponding topic id)
    //   poster_id
    //   topic_id
    // nuke_bbposts_text: (has corresponding post id)
    //   post_text
    // unknown: where's topic_approved?

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      prefix +
      "bbtopics.topic_id as _tid, " +
      prefix +
      "bbtopics.forum_id as _cid, " +
      // this is the 'parent-post'
      // see https://github.com/akhoury/nodebb-plugin-import#important-note-on-topics-and-posts
      // I don't really need it since I just do a simple join and get its content, but I will include for the reference
      // remember this post EXCLUDED in the exportPosts() function
      prefix +
      "bbtopics.topic_first_post_id as _pid, " +
      prefix +
      "bbtopics.topic_views as _viewcount, " +
      prefix +
      "bbtopics.topic_title as _title, " +
      prefix +
      "bbtopics.topic_time as _timestamp, " +
      // maybe use that to skip
      // Not sure what this data is supposed to be, remove?
      prefix +
      "bbtopics.topic_approved as _approved, " +
      prefix +
      "bbtopics.topic_status as _status, " +
      //+ prefix + 'TOPICS.TOPIC_IS_STICKY as _pinned, '
      prefix +
      "bbposts.poster_id as _uid, " +
      // this should be == to the _tid on top of this query
      prefix +
      "bbposts.topic_id as _post_tid, " +
      // and there is the content I need !!
      prefix +
      "bbposts_text.post_text as _content " +
      "FROM " +
      prefix +
      "bbtopics, " +
      prefix +
      "bbposts, " +
      prefix +
      "bbposts_text " +
      // see
      "WHERE " +
      prefix +
      "bbtopics.topic_first_post_id=" +
      prefix +
      "bbposts.post_id " +
      "AND " +
      prefix +
      "bbposts.post_id=" +
      prefix +
      "bbposts_text.post_id " +
      (start >= 0 && limit >= 0 ? "LIMIT " + start + "," + limit : "");

    if (!Exporter.connection) {
      err = { error: "MySQL connection is not setup. Run setup(config) first" };
      Exporter.error(err.error);
      return callback(err);
    }

    Exporter.connection.query(query, function(err, rows) {
      if (err) {
        Exporter.error(err);
        return callback(err);
      }

      //normalize here
      var map = {};
      rows.forEach(function(row) {
        Exporter.log("processing topics ", row._title);

        row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : "Untitled";
        row._timestamp = (row._timestamp || 0) * 1000 || startms;

        map[row._tid] = row;
      });

      callback(null, map);
    });
  };

  var getTopicsMainPids = function(callback) {
    if (Exporter._topicsMainPids) {
      return callback(null, Exporter._topicsMainPids);
    }
    Exporter.getPaginatedTopics(0, -1, function(err, topicsMap) {
      if (err) return callback(err);

      Exporter._topicsMainPids = {};
      Object.keys(topicsMap).forEach(function(_tid) {
        var topic = topicsMap[_tid];
        Exporter._topicsMainPids[topic.topic_first_post_id] = topic._tid;
      });
      callback(null, Exporter._topicsMainPids);
    });
  };
  Exporter.getPosts = function(callback) {
    Exporter.log("getPosts");
    return Exporter.getPaginatedPosts(0, -1, callback);
  };
  Exporter.getPaginatedPosts = function(start, limit, callback) {
    Exporter.log("getPaginatedPosts");
    callback = !_.isFunction(callback) ? noop : callback;

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      prefix +
      "posts.post_id as _pid, " +
      //+ 'POST_PARENT_ID as _post_replying_to, ' phpbb doesn't have "reply to another post"
      prefix +
      "bbposts.topic_id as _tid, " +
      prefix +
      "bbposts.post_time as _timestamp, " +
      // not being used
      prefix +
      "bbposts.post_subject as _subject, " +
      prefix +
      "bbposts_text.post_text as _content, " +
      prefix +
      "bbposts.poster_id as _uid, " +
      // maybe use this one to skip
      // Not sure what this data is supposed to be, remove?
      prefix +
      "bbposts.post_approved as _approved " +
      "FROM " +
      prefix +
      "bbposts, " +
      prefix +
      "bbposts_text " +
      // the ones that are topics main posts are filtered below
      "WHERE " +
      prefix +
      "bbposts.post_id=" +
      prefix +
      "bbposts_text.post_id " +
      "AND " +
      prefix +
      "bbposts.topic_id > 0 " +
      (start >= 0 && limit >= 0 ? "LIMIT " + start + "," + limit : "");

    if (!Exporter.connection) {
      err = { error: "MySQL connection is not setup. Run setup(config) first" };
      Exporter.error(err.error);
      return callback(err);
    }

    Exporter.connection.query(query, function(err, rows) {
      if (err) {
        Exporter.error(err);
        return callback(err);
      }
      getTopicsMainPids(function(err, mpids) {
        //normalize here
        var map = {};
        rows.forEach(function(row) {
          Exporter.log("processing posts ", row._subject"?);
          // make it's not a topic
          if (!mpids[row._pid]) {
            row._content = row._content || "";
            row._timestamp = (row._timestamp || 0) * 1000 || startms;
            map[row._pid] = row;
          }
        });

        callback(null, map);
      });
    });
  };

  Exporter.teardown = function(callback) {
    Exporter.log("teardown");
    Exporter.connection.end();

    Exporter.log("Done");
    callback();
  };

  Exporter.testrun = function(config, callback) {
    Exporter.log("testrun");
    async.series(
      [
        function(next) {
          Exporter.setup(config, next);
        },
        function(next) {
          Exporter.getUsers(next);
        },
        function(next) {
          Exporter.getCategories(next);
        },
        function(next) {
          Exporter.getTopics(next);
        },
        function(next) {
          Exporter.getPosts(next);
        },
        function(next) {
          Exporter.teardown(next);
        }
      ],
      callback
    );
  };

  Exporter.paginatedTestrun = function(config, callback) {
    Exporter.log("paginatedTestrun");
    async.series(
      [
        function(next) {
          Exporter.setup(config, next);
        },
        function(next) {
          Exporter.getPaginatedUsers(0, 1000, next);
        },
        function(next) {
          Exporter.getPaginatedCategories(0, 1000, next);
        },
        function(next) {
          Exporter.getPaginatedTopics(0, 1000, next);
        },
        function(next) {
          Exporter.getPaginatedPosts(1001, 2000, next);
        },
        function(next) {
          Exporter.teardown(next);
        }
      ],
      callback
    );
  };

  Exporter.warn = function() {
    var args = _.toArray(arguments);
    args.unshift(logPrefix);
    console.warn.apply(console, args);
  };

  Exporter.log = function() {
    var args = _.toArray(arguments);
    args.unshift(logPrefix);
    console.log.apply(console, args);
  };

  Exporter.error = function() {
    var args = _.toArray(arguments);
    args.unshift(logPrefix);
    console.error.apply(console, args);
  };

  Exporter.config = function(config, val) {
    Exporter.log("config");
    if (config != null) {
      if (typeof config === "object") {
        Exporter._config = config;
      } else if (typeof config === "string") {
        if (val != null) {
          Exporter._config = Exporter._config || {};
          Exporter._config[config] = val;
        }
        return Exporter._config[config];
      }
    }
    return Exporter._config;
  };

  // from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
  Exporter.validateUrl = function(url) {
    Exporter.log("validateUrl");
    var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
    return url && url.length < 2083 && url.match(pattern) ? url : "";
  };

  Exporter.truncateStr = function(str, len) {
    Exporter.log("truncateStr");
    if (typeof str != "string") return str;
    len = _.isNumber(len) && len > 3 ? len : 20;
    return str.length <= len ? str : str.substr(0, len - 3) + "...";
  };

  Exporter.whichIsFalsy = function(arr) {
    Exporter.log("whichIsFalsy");
    for (var i = 0; i < arr.length; i++) {
      if (!arr[i]) return i;
    }
    return null;
  };
})(module.exports);

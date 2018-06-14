var async = require("async");
var mysql = require("mysql");
var _ = require("underscore");
var validateUrl = require('./util/validateUrl');
var truncateString = require('./util/truncateString');
var noop = function() {};
var logPrefix = "[nodebb-plugin-import-phpbb]";

(function(Exporter) {
  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulesetupconfig-callback-required-function}
   */
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

  /**
   * @deprecated in favor of getPaginatedUsers, included for backwards compatibility
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetuserscallback-deprecated}
   */
  Exporter.getUsers = function(callback) {
    return Exporter.getPaginatedUsers(0, -1, callback);
  };

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetpaginatedusersstart-limit-callback-required-function}
   */
  Exporter.getPaginatedUsers = function(start, limit, callback) {
    Exporter.log("getPaginatedUsers ", start, limit);
    callback = !_.isFunction(callback) ? noop : callback;

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      // "_uid": 45, // REQUIRED
      prefix +
      "users.user_id as _uid, " +
      // "_email": "u45@example.com", // REQUIRED
      prefix +
      "users.user_email as _email, " +
      // "_username": "user45", // REQUIRED
      prefix +
      "users.username as _username, " +
      // "_joindate": 1386475817370, // OPTIONAL, [UNIT: MILLISECONDS], defaults to current, but what's the point of migrating if you don't preserve dates
      prefix +
      "users.user_regdate as _joindate, " +
      // "_alternativeUsername": "u45alt", // OPTIONAL, defaults to '', some forums provide UserDisplayName, we could leverage that if the _username validation fails
      // "_password": '', // OPTIONAL, if you have them, or you want to generate them on your own, great, if not, all passwords will be blank
      // "_signature": "u45 signature", // OPTIONAL, defaults to '', over 150 chars will be truncated with an '...' at the end
      prefix +
      "users.user_sig as _signature, " +
      // "_picture": "http://images.com/derp.png", // OPTIONAL, defaults to ''. Note that, if there is an '_piçture' on the 'normalized' object, the 'imported' objected will be augmented with a key imported.keptPicture = true, so you can iterate later and check if the images 200 or 404s
      prefix +
      "users.user_avatar as _picture, " +
      // "_pictureBlob": "...BINARY BLOB...", // OPTIONAL, defaults to null
      // "_pictureFilename": "123.png", // OPTIONAL, only applicable if using _pictureBlob, defaults to ''
      // "_path": "/myoldforum/user/123", // OPTIONAL, the old path to reach this user's page, defaults to ''
      // "_slug": "old-user-slug", // OPTIONAL
      // "_groups": [123, 456, 789], // OPTIONAL, an array of old group ids that this user belongs to, obviously this one depends on implementing the optional getPaginatedGroups function
      // "_website": "u45.com", // OPTIONAL, defaults to ''
      prefix +
      "users.user_website as _website, " +
      // "_fullname": "this is dawg", // OPTIONAL, defaults to ''
      prefix +
      "users.name as _fullname, " +
      // "_banned": 0, // OPTIONAL, defaults to 0
      // "_readCids": [1, 2, 4, 5, 6, 7], // OPTIONAL, defaults to []. read cids and tids by that user, it's more efficient to use _readCids if you know that a user has read all the topics in a category.
      // "_readTids": [1, 2, 4, 5, 6, 7], // OPTIONAL, defaults to []. untested with very large sets. So.
      // "_followingUids": [1, 2, 4, 5, 6, 7], // OPTIONAL, defaults to []. following other _Uids, untested with very large sets. So.
      // "_friendsUids": [1, 2, 4, 5, 6, 7], // OPTIONAL, defaults to []. friend other _Uids, untested with very large sets. So. if you have https://github.com/sanbornmedia/nodebb-plugin-friends installed or want to use it
      // "_location": "u45 city", // OPTIONAL, defaults to ''
      prefix +
      "users.user_from as _location, " +
      // "_reputation": 123, // OPTIONAL, defaults to 0, (there is a config for multiplying these with a number for moAr karma).Also, if you're implementing getPaginatedVotes, every vote will also impact the user's reputation
      prefix +
      "users.user_posts as _reputation " +
      // "_profileviews": 1, // OPTIONAL, defaults to 0
      // "_birthday": "01/01/1977", // OPTIONAL, [FORMAT: mm/dd/yyyy], defaults to ''
      // "_showemail": 0, // OPTIONAL, defaults to 0
      // "_lastposttime": 1386475817370, // OPTIONAL, [UNIT: MILLISECONDS], defaults to current
      // "_level": "administrator" // OPTIONAL, [OPTIONS: 'administrator' or 'moderator'], defaults to '', also note that a moderator will become a NodeBB Moderator on ALL categories at the moment.
      // "_lastonline": 1386475827370 // OPTIONAL, [UNIT: MILLISECONDS], defaults to undefined
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
        row._signature = truncateString(row._signature || "", 150);

        // from unix timestamp (s) to JS timestamp (ms)
        row._joindate = (row._joindate || 0) * 1000 || startms;

        // lower case the email for consistency
        row._email = (row._email || "").toLowerCase();

        // I don't know about you about I noticed a lot my users have incomplete urls, urls like: http://
        row._picture = validateUrl(row._picture);
        row._website = validateUrl(row._website);

        map[row._uid] = row;
      });

      callback(null, map);
    });
  };

  /**
   * @deprecated in favor of getPaginatedCategories, included for backwards compatibility
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetcategoriescallback-deprecated}
   */
  Exporter.getCategories = function(callback) {
    return Exporter.getPaginatedCategories(0, -1, callback);
  };

  /**
   * phpBB has two types of categories:
   * 1. entries in nuke_bbcategories should be treated with the "Treat this category as a section" toggle flipped on in admin
   * 2. entries in nuke_bbforums should be treated as categories with parent categories (with nuke_bbforums.cat_id = nukebb_categories.cat_id)
   * This implementation ignores nuke_bbcategories and assumes that moderators will setup sections following the migration
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetpaginatedcategoriesstart-limit-callback-required-function}
   */
  Exporter.getPaginatedCategories = function(start, limit, callback) {
    Exporter.log("getPaginatedCategories ", start, limit);
    callback = !_.isFunction(callback) ? noop : callback;

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      // "_cid": 2, // REQUIRED
      prefix +
      "bbforums.forum_id as _cid, " +
      // "_name": "Category 1", // REQUIRED
      prefix +
      "bbforums.forum_name as _name, " +
      // "_description": "it's about category 1", // OPTIONAL
      prefix +
      "bbforums.forum_desc as _description, " +
      // "_order": 1 // OPTIONAL, defauls to its index + 1
      prefix +
      "bbforums.forum_order as _order " +
      // "_path": "/myoldforum/category/123", // OPTIONAL, the old path to reach this category's page, defaults to ''
      // computed below
      // "_slug": "old-category-slug", // OPTIONAL defaults to ''
      // "_parentCid": 1, // OPTIONAL, parent category _cid defaults to null
      // "_skip": 0, // OPTIONAL, if you want to intentionally skip that record
      // "_color": "#FFFFFF", // OPTIONAL, text color, defaults to random
      // "_bgColor": "#123ABC", // OPTIONAL, background color, defaults to random
      // "_icon": "comment", // OPTIONAL, Font Awesome icon, defaults to random
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
        row._path = "/modules.php?name=Forums&file=viewforum&f=" + row._cid;

        map[row._cid] = row;
      });

      callback(null, map);
    });

    Exporter.getPaginatedSubCategories(start, limit, callback);
  };

  /**
   * @deprecated in favor of getPaginatedTopics, included for backwards compatibility
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegettopicscallback-deprecated}
   */
  Exporter.getTopics = function(callback) {
    return Exporter.getPaginatedTopics(0, -1, callback);
  };

  /**
   * Topics in nodeBB are what phpBB considers a combination of information from nuke_bbtopics and information from the post with nuke_bbposts.post_id = nuke_bbtopics.topic_first_post_id
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetpaginatedtopicsstart-limit-callback-required-function}
   */
  Exporter.getPaginatedTopics = function(start, limit, callback) {
    Exporter.log("getPaginatedTopics ", start, limit);
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

  /**
   * @deprecated in favor of getPaginatedPosts, included for backwards compatibility
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetpostscallback-deprecated}
   */
  Exporter.getPosts = function(callback) {
    return Exporter.getPaginatedPosts(0, -1, callback);
  };

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetpaginatedpostsstart-limit-callback-required-function}
   */
  Exporter.getPaginatedPosts = function(start, limit, callback) {
    Exporter.log("getPaginatedPosts ", start, limit);
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
          Exporter.log("processing posts ", row._subject);
          // make sure it's not a topic
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

  // TODO: implement or delete other methods
  // Exporter.getRooms
  // Exporter.getPaginatedRooms
  // Exporter.getMessages
  // Exporter.getPaginatedMessages
  // Exporter.getGroups
  // Exporter.getPaginatedGroups
  // Exporter.getVotes
  // Exporter.getPaginatedVotes
  // Exporter.getBookmarks
  // Exporter.getPaginatedBookmarks

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmoduleteardowncallback-required-function}
   */
  Exporter.teardown = function(callback) {
    Exporter.log("teardown");
    Exporter.connection.end();

    Exporter.log("Done");
    callback();
  };

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#a-testrun-function}
   */
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

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#a-testrun-function}
   */
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

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#logger-functions}
   */
  Exporter.warn = function() {
    var args = _.toArray(arguments);
    args.unshift(logPrefix);
    console.warn.apply(console, args);
  };

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#logger-functions}
   */
  Exporter.log = function() {
    var args = _.toArray(arguments);
    args.unshift(logPrefix);
    console.log.apply(console, args);
  };

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#logger-functions}
   */
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
})(module.exports);

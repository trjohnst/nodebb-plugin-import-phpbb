var async = require("async");
var mysql = require("mysql");
var toArray = require('lodash.toarray');
var validateUrl = require('./util/validateUrl');
var truncateString = require('./util/truncateString');
var getLimitClause = require('./util/getLimitClause');
var getcolorFromCategoryId = require('./util/getColorFromCategoryId');
var noop = function() {};
var logPrefix = "[nodebb-plugin-import-phpbb]";

(function(Exporter) {
  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulesetupconfig-callback-required-function}
   * @see {@link https://github.com/NodeBB/NodeBB/wiki/Database-Structure}
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
    if (typeof callback !== 'function') {
      callback = noop;
    }

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
      prefix +
      "users.user_password as _password, " +
      // "_signature": "u45 signature", // OPTIONAL, defaults to '', over 150 chars will be truncated with an '...' at the end
      prefix +
      "users.user_sig as _signature, " +
      // "_picture": "http://images.com/derp.png", // OPTIONAL, defaults to ''. Note that, if there is an '_piçture' on the 'normalized' object, the 'imported' objected will be augmented with a key imported.keptPicture = true, so you can iterate later and check if the images 200 or 404s
      prefix +
      "users.user_avatar as _picture, " +
      // "_pictureBlob": "...BINARY BLOB...", // OPTIONAL, defaults to null
      // "_pictureFilename": "123.png", // OPTIONAL, only applicable if using _pictureBlob, defaults to ''
      // "_path": "/myoldforum/user/123", // OPTIONAL, the old path to reach this user's page, defaults to ''
      // computed below
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
      getLimitClause(start, limit);

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
      const rowsCount = rows.length;
      let processedUserCount = 0;

      if (rows.length === 0) {
        // no results, call the callback immediately
        callback(null, map);
      } else {
        rows.forEach(function(row) {
          Exporter.log("processing user ", row._uid);

          // nodeBB has max signature lengths enforced by settings (up to 255)
          row._signature = row._signature || "";

          // from unix timestamp (s) to JS timestamp (ms)
          row._joindate = row._joindate ? new Date(row._joindate).getTime() / 1000 : startms;

          // lower case the email for consistency
          row._email = (row._email || "").toLowerCase();

          // I don't know about you about I noticed a lot my users have incomplete urls, urls like: http://
          row._picture = validateUrl(row._picture);
          row._website = validateUrl(row._website);

          row._path = '/modules.php?name=Forums&file=profile&mode=viewprofile&u=' + row._uid;

          var groupsQuery =
          "SELECT " +
          "user_to_group.group_id as group_id " +
          "FROM " +
          prefix + "users users, " +
          prefix + "bbuser_group user_to_group " +
          "WHERE " +
          "users.user_id = user_to_group.user_id " +
          "AND " +
          "users.user_id = " + row._uid;

          Exporter.connection.query(groupsQuery, function(err, groupRows) {
            row._groups = groupRows.map(groupRow => groupRow.group_id);
            map[row._uid] = row;
            processedUserCount++;

            Exporter.log("Processed user (with groups)", processedUserCount, "of", rowsCount, "this round");
            if (processedUserCount === rowsCount) {
              Exporter.log("Finished processing a round of users");
              callback(null, map);
            }
          });
        });
      }

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
    if (typeof callback !== 'function') {
      callback = noop;
    }

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      // "_cid": 2, // REQUIRED
      "forums.forum_id as _cid, " +
      // "_name": "Category 1", // REQUIRED
      "forums.forum_name as _name, " +
      // "_description": "it's about category 1", // OPTIONAL
      "forums.forum_desc as _description, " +
      // "_order": 1 // OPTIONAL, defauls to its index + 1
      "forums.forum_order as _order " +
      // "_path": "/myoldforum/category/123", // OPTIONAL, the old path to reach this category's page, defaults to ''
      // computed below
      // "_slug": "old-category-slug", // OPTIONAL defaults to ''
      // "_parentCid": 1, // OPTIONAL, parent category _cid defaults to null
      // "_skip": 0, // OPTIONAL, if you want to intentionally skip that record
      // "_color": "#FFFFFF", // OPTIONAL, text color, defaults to random
      // "_bgColor": "#123ABC", // OPTIONAL, background color, defaults to random
      // "_icon": "comment", // OPTIONAL, Font Awesome icon, defaults to random
      "FROM " +
      prefix + "bbforums forums " +
      getLimitClause(start, limit);

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
        Exporter.log("processing category ", row._cid, row._name);
        row._name = row._name || "Untitled Category " + row._cid;
        row._description = row._description || "No decsription available";
        row._timestamp = (row._timestamp || 0) * 1000 || startms;
        row._path = "/modules.php?name=Forums&file=viewforum&f=" + row._cid;
        row._icon = "fa-comments";
        row._color = "#fff";
        row._bgColor = getcolorFromCategoryId(row._cid);

        map[row._cid] = row;
      });

      callback(null, map);
    });
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
    if (typeof callback !== 'function') {
      callback = noop;
    }

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      // "_tid": 1, // REQUIRED, THE OLD TOPIC ID
      "topics.topic_id as _tid, " +
      // TODO: one of these ^V
      "topics.topic_first_post_id as _pid, " +
      // "_uid": 1, // OPTIONAL, THE OLD USER ID, Nodebb will create the topics for user 'Guest' if not provided
      "posts.poster_id as _uid, " +
      // "_uemail": "u45@example.com", // OPTIONAL, The OLD USER EMAIL. If the user is not imported, the plugin will get the user by his _uemail
      // "_guest": "Some dude" // OPTIONAL, if you dont have _uid, you can pass a guest name to be used in future features, defaults to null
      // added below if _uid is empty
      // "_cid": 1, // REQUIRED, THE OLD CATEGORY ID
      "topics.forum_id as _cid, " +
      // "_ip": "123.456.789.012", // OPTIONAL, not currently used in NodeBB core, but it might be in the future, defaults to null
      // "_title": "this is topic 1 Title", // OPTIONAL, defaults to "Untitled :id"
      "topics.topic_title as _title, " +
      // "_content": "This is the first content in this topic 1", // REQUIRED
      "posts_text.post_text as _content, " +
      // "_thumb": "http://foo.bar/picture.png", // OPTIONAL, a thumbnail for the topic if you have one, note that the importer will NOT validate the URL
      // "_timestamp": 1386475817370, // OPTIONAL, [UNIT: Milliseconds], defaults to current, but what's the point of migrating if you dont preserve dates
      "topics.topic_time as _timestamp, " +
      // "_viewcount": 10, // OPTIONAL, defaults to 0
      "topics.topic_views as _viewcount, " +
      // "_locked": 0, // OPTIONAL, defaults to 0, during migration, ALL topics will be unlocked then locked back up at the end
      // "_tags": ["tag1", "tag2", "tag3"], // OPTIONAL, an array of tags, or a comma separated string would work too, defaults to null
      // "_attachments": ["http://example.com/myfile.zip"], // OPTIONAL, an array of urls, to append to the content for download.
      // OR you can pass a filename with it
      // "_attachments": [{url: "http://example.com/myfile.zip", filename: "www.zip"}], // OPTIONAL, an array of objects with urls and filenames, to append to the content for download.
    	//   OPTIONAL, an array of objects, each object mush have the binary BLOB,
    	//   either a filename or extension, then each file will be written to disk,
    	//   if no filename is provided, the extension will be used and a filename will be generated as attachment_t_{_tid}_{index}{extension}
    	//   and its url would be appended to the _content for download
      // "_attachmentsBlobs": [ {blob: <BINARY>, filename: "myfile.zip"}, {blob: <BINARY>, extension: ".zip"} ],
      // "_deleted": 0, // OPTIONAL, defaults to 0
      // "_pinned": 1 // OPTIONAL, defaults to 0
      // "_edited": 1386475817370 // OPTIONAL, [UNIT: Milliseconds] see post._edited defaults to null
      "posts.post_edit_time as _edited, " +
      // "_reputation": 1234, // OPTIONAL, defaults to 0, must be >= 0, not to be confused with _votes (see getPaginatedVotes for votes)
      // "_path": "/myoldforum/topic/123", // OPTIONAL, the old path to reach this topic's page, defaults to ''
      // computed below
      // "_slug": "old-topic-slug" // OPTIONAL, defaults to ''
      // this is the 'parent-post'
      // see https://github.com/akhoury/nodebb-plugin-import#important-note-on-topics-and-posts
      // I don't really need it since I just do a simple join and get its content, but I will include for the reference
      // remember this post EXCLUDED in the exportPosts() function
      // maybe use that to skip
      // Not sure what this data is supposed to be, remove?
      // TODO: sort out these, may be unused
      // prefix +
      // "bbtopics.topic_approved as _approved, " +
      // prefix +
      // "bbtopics.topic_status as _status, " +
      //+ prefix + 'TOPICS.TOPIC_IS_STICKY as _pinned, '
      // this should be == to the _tid on top of this query
      "posts.topic_id as _post_tid " +
      // end select statements
      "FROM " +
      prefix +
      "bbtopics topics, " +
      prefix +
      "bbposts posts, " +
      prefix +
      "bbposts_text posts_text " +
      // see
      "WHERE " +
      "topics.topic_first_post_id = posts.post_id " +
      "AND " +
      "posts.post_id = posts_text.post_id " +
      getLimitClause(start, limit);

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
        Exporter.log("processing topics ", row._tid, row._title);

        row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : "Untitled";
        row._timestamp = (row._timestamp || 0) * 1000 || startms;

        row._path = '/modules.php?name=Forums&file=viewtopic&t=' + row._tid;

        if (!row._uid) {
          row._guest = 'Unknown poster';
        }

        map[row._tid] = row;
      });

      callback(null, map);
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
    if (typeof callback !== 'function') {
      callback = noop;
    }

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      // "SELECT " +
      // // "_pid": 65487, // REQUIRED, OLD POST ID
      // "posts.post_id as _pid, " +
      // // "_tid": 1234, // REQUIRED, OLD TOPIC ID
      // "posts.topic_id as _tid, " +
      // // "_content": "Post content ba dum tss", // REQUIRED
      // "posts_text.post_text as _content, " +
      // // "_uid": 202, // OPTIONAL, OLD USER ID, if not provided NodeBB will create under the "Guest" username, unless _guest is passed.
      // "posts.poster_id as _uid, " +
      // // "_uemail": "u45@example.com", // OPTIONAL, The OLD USER EMAIL. If the user is not imported, the plugin will get the user by his _uemail
      // // "_toPid": 65485, // OPTIONAL, OLD REPLIED-TO POST ID,
      // // "_timestamp": 1386475829970 // OPTIONAL, [UNIT: Milliseconds], defaults to current, but what's the point of migrating if you dont preserve dates.
      // "posts.post_time as _timestamp, " +
      // // "_guest": "Some dude" // OPTIONAL, if you don't have _uid, you can pass a guest name to be used in future features, defaults to null
      // //   added below if _uid is empty
      // // "_ip": "123.456.789.012", // OPTIONAL, not currently used in NodeBB core, but it might be in the future, defaults to null
      // // "_edited": 1386475829970, // OPTIONAL, [UNIT: Milliseconds], if and when the post was edited, defaults to null
      // "posts.post_edit_time as _edited " +
      // // "_reputation": 0, // OPTIONAL, defaults to 0, must be >= 0, not to be confused with _votes (see getPaginatedVotes for votes)
      // // "_attachments": ["http://example.com/myfile.zip"], // OPTIONAL, an array of urls, to append to the content for download.
    	// //   OPTIONAL, an array of objects, each object mush have the binary BLOB,
    	// //   either a filename or extension, then each file will be written to disk,
    	// //   if no filename is provided, the extension will be used and a filename will be generated as attachment_p_{_pid}_{index}{extension}
    	// //   and its url would be appended to the _content for download
      // // "_attachmentsBlobs": [ {blob: <BINARY>, filename: "myfile.zip"}, {blob: <BINARY>, extension: ".zip"} ],
      // // "_path": "/myoldforum/topic/123#post56789", // OPTIONAL, the old path to reach this post's page and maybe deep link, defaults to ''
      // //   computed below
      // // "_slug": "old-post-slug" // OPTIONAL, defaults to ''
      // // end select statements
      // "FROM " +
      // prefix + "bbposts posts, " +
      // prefix + "bbposts_text posts_text " +
      // // the ones that are topics main posts are filtered below
      // "WHERE " +
      // "posts.post_id = posts_text.post_id " +
      // "AND " +
      // "posts.topic_id > 0 " +
      // getLimitClause(start, limit);

      "SELECT " +
      // "_pid": 65487, // REQUIRED, OLD POST ID
      "posts.post_id as _pid, " +
      // "_tid": 1234, // REQUIRED, OLD TOPIC ID
      "posts.topic_id as _tid, " +
      // "_content": "Post content ba dum tss", // REQUIRED
      "posts_text.post_text as _content, " +
      // "_uid": 202, // OPTIONAL, OLD USER ID, if not provided NodeBB will create under the "Guest" username, unless _guest is passed.
      "posts.poster_id as _uid, " +
      "posts.post_time as _timestamp, " +
      "posts.post_edit_time as _edited " +
      "FROM " +
      "( " +
      "  SELECT " +
      "  post_id, topic_id, poster_id, post_time, post_edit_time " +
      "  FROM " + prefix + "bbposts " +
      "  WHERE " +
      "  post_id not in ( " +
      "    SELECT topic_first_post_id FROM " + prefix + "bbtopics " +
      "  ) " +
      "  AND " +
      "  post_id not in ( " +
      "    SELECT post_id FROM " + prefix + "bbposts a INNER JOIN ( " +
      "      SELECT topic_id FROM " + prefix + "bbtopics WHERE topic_first_post_id = '' " +
      "    ) b ON a.topic_id = b.topic_id " +
      "  ) " +
      ") posts, " +
      prefix + "bbposts_text posts_text " +
      "WHERE " +
      "posts.post_id = posts_text.post_id " +
      getLimitClause(start, limit);

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
        Exporter.log("processing posts ", row._pid, row._subject);

        row._content = row._content || "";
        row._timestamp = (row._timestamp || 0) * 1000 || startms;
        if (!row._uid) {
          row._guest = 'Unknown poster';
        }

        row._path = "/modules.php?name=Forums&file=viewtopic&p=" + row._pid + "#" + row._pid

        map[row._pid] = row;
      });

      callback(null, map);
    });
  };

  /**
   * @deprecated in favor of getPaginatedMessages, included for backwards compatibility
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetmessagescallback-deprecated}
   */
  Exporter.getMessages = function(callback) {
    return Exporter.getPaginatedMessages(0, -1, callback);
  };

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetpaginatedmessagesstart-limit-callback-optional-function}
   */
  Exporter.getPaginatedMessages = function(start, limit, callback) {
    Exporter.log("getPaginatedMessages ", start, limit);
    if (typeof callback !== 'function') {
      callback = noop;
    }

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      // "_mid": 45, // REQUIRED
      "messages.privmsgs_id as _mid, " +
    	// "_fromuid": 10, // REQUIRED
      "messages.privmsgs_from_userid as _fromuid, " +
    	// "_roomId": 20, // PREFERRED, the _roomId if you are using get(Pagianted)Rooms
    	// "_touid": 20, // DEPRECATED, if you're not using getPaginatedRooms, you can just pass the _touid value here.
      "messages.privmsgs_to_userid as _touid, " +
    	//   note: I know the camelcasing is weird here, but can't break backward compatible exporters yet.
    	// "_content": "Hello there!", // REQUIRED
    	// TODO: how to join with privmsgs_text?
      "text.privmsgs_text as _content, " +
    	// "_timestamp": 1386475817370 // OPTIONAL, [UNIT: MILLISECONDS], defaults to current
      "messages.privmsgs_date as _timestamp " +
      "FROM " +
      prefix +
      "bbprivmsgs messages, " +
      prefix +
      "bbprivmsgs_text text " +
      "WHERE " +
      "messages.privmsgs_id = text.privmsgs_text_id " +
      getLimitClause(start, limit);

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
        Exporter.log("processing message ", row._mid);

        // from unix timestamp (s) to JS timestamp (ms)
        row._timestamp = (row._timestamp || 0) * 1000 || startms;

        map[row._mid] = row;
      });

      callback(null, map);
    });
  };

  /**
   * @deprecated in favor of getPaginatedGroups, included for backwards compatibility
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetgroupscallback-deprecated}
   */
  Exporter.getGroups = function(callback) {
    return Exporter.getPaginatedGroups(0, -1, callback);
  };

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#yourmodulegetpaginatedgroupsstart-limit-callback-optional-function}
   */
  Exporter.getPaginatedGroups = function(start, limit, callback) {
    Exporter.log("getPaginatedGroups ", start, limit);
    if (typeof callback !== 'function') {
      callback = noop;
    }

    var err;
    var prefix = Exporter.config("prefix");
    var startms = +new Date();
    var query =
      "SELECT " +
      // "_gid": 45, // REQUIRED, old group id
      "group_id as _gid, " +
      // "_name": "My group name", // REQUIRED
      "group_name as _name, " +
      // "_ownerUid": 123, // REQUIRED, owner old user id, aka user._uid,
      "group_moderator as _ownerUid, " +
      // "_description": "My group description", // OPTIONAL
      "group_description as _description " +
      // "_userTitle": "My group badge", // OPTIONAL, will show instead of the _name
      // "_userTitleEnabled": 1, // OPTIONAL, to show the userTitle at all
      // "_disableJoinRequests": 0, // OPTIONAL
      // "_system": 0, // OPTIONAL, if system group
      // "_private": 0, // OPTIONAL, if private group
      // "_hidden": 0, // OPTIONAL, if hidden group
      // "_timestamp": 1386475817370 // OPTIONAL, [UNIT: MILLISECONDS], defaults to current
      "FROM " +
      prefix +
      "bbgroups " +
      "WHERE " +
      "group_name <> '' " +
      getLimitClause(start, limit);

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
        Exporter.log("processing group ", row._gid, " ", row._name);

        row._disableJoinRequests = 1; // don't allow joining by default
        row._private = 1; // Make private by default
        row._hidden = 1; // Hide the group by default

        map[row._gid] = row;
      });

      callback(null, map);
    });
  };

  // TODO: implement or delete other methods
  // Exporter.getRooms
  // Exporter.getPaginatedRooms
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

    if (typeof callback === 'function') {
      callback();
    }
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
          Exporter.getMessages(next);
        },
        function(next) {
          Exporter.getGroups(next);
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
          Exporter.getPaginatedMessages(0, 1000);
        },
        function(next) {
          Exporter.getPaginatedGroups(0, 1000);
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
    var args = toArray(arguments);
    args.unshift(logPrefix);
    console.warn.apply(console, args);
  };

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#logger-functions}
   */
  Exporter.log = function() {
    var args = toArray(arguments);
    args.unshift(logPrefix);
    console.log.apply(console, args);
  };

  /**
   * @see {@link https://github.com/akhoury/nodebb-plugin-import/blob/master/write-my-own-exporter.md#logger-functions}
   */
  Exporter.error = function() {
    var args = toArray(arguments);
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

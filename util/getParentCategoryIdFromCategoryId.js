// avoid collisions with children id's by always being above the max id
// allowed by topic_id's with type mediumint(8) unsigned
var maxChildCategoryId = 16777215;

module.exports = function(id) {
  return id + maxChildCategoryId;
};

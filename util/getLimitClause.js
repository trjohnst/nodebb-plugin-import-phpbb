module.exports = function(start, limit) {
  return (start >= 0 && limit >= 0 ? "LIMIT " + start + "," + limit : "");
};

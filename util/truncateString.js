var isNumber = require('lodash.isnumber');

module.exports = function(str, len) {
  if (typeof str != "string") return str;
  len = isNumber(len) && len > 3 ? len : 20;
  return str.length <= len ? str : str.substr(0, len - 3) + "...";
};

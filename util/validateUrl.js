// from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
module.exports = function(url) {
  var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
  return url && url.length < 2083 && url.match(pattern) ? url : "";
};

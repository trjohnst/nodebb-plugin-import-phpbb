function convertNumberToHex(number) {
  return number.toString(16);
}

function convertHexToColor(hex) {
  var color = hex;
  var diff = 6 - color.length;

  if (diff < 0) {
    color = color.substr(0,6);
  } else if (diff > 0) {
    var padding = '';

    for (var i = 0; i < diff; i++) {
      padding += '0';
    }

    color = padding + color;
  }

  color = '#' + color;

  return color;
}

module.exports = function(id) {
  var hex = convertNumberToHex(id);
  var color = convertHexToColor(hex);

  return color;
};

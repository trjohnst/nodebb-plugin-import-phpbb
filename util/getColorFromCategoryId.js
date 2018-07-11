function getRandomHexColor() {
  var r = convertNumberToHex(getRandomNumber());
  var g = convertNumberToHex(getRandomNumber());
  var b = convertNumberToHex(getRandomNumber());

  return '#' + r + g + b;
}

function getRandomNumber() {
  return Math.floor(Math.random()*255) + 1;
}

function convertNumberToHex(number) {
  var hex = number.toString(16);

  if (hex.length === 1) {
    hex = '0' + hex;
  }

  return hex;
}

var idToColor = {};

module.exports = function(id) {
  if (!idToColor[id]) {
    idToColor[id] = getRandomHexColor();
  }

  return idToColor[id];
};

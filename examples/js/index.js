function generateEndHex(length, status) {
  var buffer = new ArrayBuffer(8);
  var view = new DataView(buffer);

  view.setUint32(0, status, true);
  view.setUint32(4, length, true);

  var hexString = "";
  for (var i = 0; i < buffer.byteLength; i++) {
    var hex = view.getUint8(i).toString(16);
    hexString += hex.padStart(2, "0");
  }

  return hexString;
}

function respond(res, status) {
  var endHex = generateEndHex(res.length, status);
  //using putstr because console.log adds "\n" to the end
  putstr(res + endHex);
}

respond(
  "<h1>From my Raptor application</h1></br>some other stuff here</br>",
  200
);

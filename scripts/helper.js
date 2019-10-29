prepareLine = (char, lineLength) => {
  let line = '';
  for (let i=0 ; i<lineLength ; i++) {
    line += char;
  }
  return line;
}

module.exports = {
  log: (section, message, lineChar = '=', lineLength = 15, lineTop = false, lineBottom = false,) => {
    const line = lineTop || lineBottom ? prepareLine(lineChar, lineLength) : '';

    if (lineTop) {
      console.log(line);
    }

    console.log(`${section}::: ${message}`)

    if (lineBottom) {
      console.log(line);
    }
  }
}
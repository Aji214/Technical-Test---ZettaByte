function result(words) {
  if (!words.length) return '';
  words.sort();
  const first = words[0];
  const last = words[words.length - 1];
  let i = 0;
  while (i < first.length && first[i] === last[i]) i++;
  return first.slice(0, i);
}

console.log(result(words));

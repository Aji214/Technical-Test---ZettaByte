function result(numbers) {
  const max = Math.max(...numbers);
  for (let i = 0; i <= max; i++) {
    if (!numbers.includes(i)) {
      return i;
    }
  }
  return max + 1;
}

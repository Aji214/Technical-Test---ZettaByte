function result(data) {
  function clean(obj) {
    if (Array.isArray(obj)) {
      return obj.map(clean);
    } else if (typeof obj === 'object' && obj !== null) {
      const newObj = {};
      for (const [key, value] of Object.entries(obj)) {
        if (value !== null && value !== undefined) {
          newObj[key] = clean(value);
        }
      }
      return newObj;
    }
    return obj;
  }

  return clean(data);
}

console.log(JSON.stringify(result(data), null, 2));

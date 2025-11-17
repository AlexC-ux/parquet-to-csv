export const allKeys = new Set();

export function addNotFoundKeys(obj) {
  const objKeys = Object.keys(obj);
  if (objKeys.length == allKeys.length) {
    return obj;
  } else {
    const notFoundKeys = [];
    for (const key of allKeys) {
      if (!objKeys.includes(key)) {
        notFoundKeys.push(key);
      }
    }
    const newObj = { ...obj };
    for (const notFoundKey of notFoundKeys) {
      newObj[notFoundKey] = null;
    }
    return newObj;
  }
}

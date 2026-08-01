'use strict';

// config@3.3.8 still calls these deprecated node:util predicates. Node 24
// removed them, so keep the compatibility surface local to the Craig process.
const util = require('node:util');

if (typeof util.isRegExp !== 'function') util.isRegExp = (value) => value instanceof RegExp;
if (typeof util.isDate !== 'function') util.isDate = (value) => value instanceof Date;

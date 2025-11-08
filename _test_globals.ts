// deno-lint-ignore-file no-import-prefix
/**
 * This polyfills the global functions and properties for Jest
 * to ensure that the tests run correctly in a Deno environment.
 *
 * @module
 */

import { afterAll, afterEach, beforeAll, beforeEach, describe, it } from "jsr:@std/testing@^1.0.16/bdd";
import { expect } from "jsr:@std/expect@^1.0.17";
import { bind as bindEach } from "npm:jest-each@^30.2.0";

function getDescriptor(value: unknown) {
  return {
    value,
    writable: false,
    enumerable: true,
    configurable: true,
  };
}

// deno-lint-ignore no-explicit-any
(describe as any).each = bindEach(describe as any, false);
// deno-lint-ignore no-explicit-any
(it as any).each = bindEach(it as any, false);
Object.defineProperties(globalThis, {
  afterAll: getDescriptor(afterAll),
  afterEach: getDescriptor(afterEach),
  beforeAll: getDescriptor(beforeAll),
  beforeEach: getDescriptor(beforeEach),
  describe: getDescriptor(describe),
  it: getDescriptor(it),
  expect: getDescriptor(expect),
});

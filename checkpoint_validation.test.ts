/**
 * This module leverages the `@langchain/langgraph-checkpoint-validation`
 * library to validate the functionality of the `DenoKvSaver` class.
 *
 * Because the validation is expected to run under Jest, we need to
 * ensure that the global functions and properties are set up correctly,
 * polyfilling them with `@std/testing` and `@std/expect`.
 */

import type { CheckpointerTestInitializer } from "https://raw.githubusercontent.com/langchain-ai/langgraphjs/refs/heads/main/libs/checkpoint-validation/src/types.ts";
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  it,
} from "jsr:@std/testing@1.0.10/bdd";
import { expect } from "jsr:@std/expect@^1.0.14";
import { bind as bindEach } from "npm:jest-each@^29.7.0";

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

import { DenoKvSaver } from "./checkpoint.ts";

let store: string | undefined;

const initalizer: CheckpointerTestInitializer<DenoKvSaver> = {
  async beforeAll() {
    const path = await Deno.makeTempDir({ prefix: "deno-kv-test" });
    store = `${path}/test.db`;
  },
  checkpointerName: "@kitsonk/langchain-kv/checkpoint/DenoKvSaver",
  createCheckpointer() {
    return new DenoKvSaver({ store });
  },
  destroyCheckpointer(checkpointer) {
    return checkpointer.end();
  },
};

(async () => {
  Object.defineProperties(globalThis, {
    afterAll: getDescriptor(afterAll),
    afterEach: getDescriptor(afterEach),
    beforeAll: getDescriptor(beforeAll),
    beforeEach: getDescriptor(beforeEach),
    describe: getDescriptor(describe),
    it: getDescriptor(it),
    expect: getDescriptor(expect),
  });
  const { validate } = await import(
    "npm:@langchain/langgraph-checkpoint-validation@^0.0.2"
  );
  validate(initalizer);
})();

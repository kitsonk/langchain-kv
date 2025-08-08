/**
 * Implementation of a LangChain cache using a Deno KV store.
 *
 * @module
 */

import { get, set } from "@kitsonk/kv-toolbox/blob";
import { BaseCache, deserializeStoredGeneration, getCacheKey, serializeGeneration } from "@langchain/core/caches";
import type { StoredGeneration } from "@langchain/core/messages";
import type { Generation } from "@langchain/core/outputs";

const DEFAULT_PREFIX: Deno.KvKey = ["__langchain_cache__"];

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function asBytes(value: StoredGeneration): Uint8Array {
  return encoder.encode(JSON.stringify(value));
}

function fromBytes(value: Uint8Array): StoredGeneration {
  return JSON.parse(decoder.decode(value));
}

interface DenoKvCacheOptions {
  /**
   * `Deno.Kv` instance or path to `Deno.Kv` store. Defaults to the local
   * instance.
   */
  store?: Deno.Kv | string;
  /**
   * Prefix for keys in the store, defaults to
   * `["__langchain_cache__"]`
   */
  prefix?: Deno.KvKey;
  /**
   * Optionally an `expireIn` option can be specified to set a time-to-live
   * (TTL) for the key. The TTL is specified in milliseconds, and the key will
   * be deleted from the database at earliest after the specified number of
   * milliseconds have elapsed. Once the specified duration has passed, the key
   * may still be visible for some additional time. If the `expireIn` option is
   * not specified, the key will not expire.
   */
  expireIn?: number;
}

/**
 * A cache implementation using {@linkcode Deno.Kv}.
 */
export class DenoKvCache extends BaseCache {
  #storePromise: Promise<Deno.Kv>;
  #prefix: Deno.KvKey;
  #expireIn?: number;

  constructor(options: DenoKvCacheOptions = {}) {
    super();
    const { store, prefix = DEFAULT_PREFIX } = options;
    this.#storePromise = (!store || typeof store === "string") ? Deno.openKv(store) : Promise.resolve(store);
    this.#prefix = prefix;
    this.#expireIn = options.expireIn;
  }

  override async lookup(
    prompt: string,
    llmKey: string,
  ): Promise<Generation[] | null> {
    let idx = 0;
    let key = getCacheKey(prompt, llmKey, String(idx));
    const store = await this.#storePromise;
    let value = await get(store, [...this.#prefix, key]);
    const generations: Generation[] = [];

    while (value.value) {
      generations.push(deserializeStoredGeneration(fromBytes(value.value)));
      idx += 1;
      key = getCacheKey(prompt, llmKey, String(idx));
      value = await get(store, [...this.#prefix, key]);
    }

    return generations.length > 0 ? generations : null;
  }

  override async update(prompt: string, llmKey: string, value: Generation[]) {
    const store = await this.#storePromise;
    for (let i = 0; i < value.length; i += 1) {
      const key = getCacheKey(prompt, llmKey, String(i));
      await set(
        store,
        [...this.#prefix, key],
        asBytes(serializeGeneration(value[i])),
        {
          expireIn: this.#expireIn,
        },
      );
    }
  }

  /**
   * Closes the underlying `Deno.Kv` store.
   */
  async end() {
    (await this.#storePromise).close();
  }
}

import { get, list, remove, set } from "@kitsonk/kv-toolbox/blob";
import { BaseStore } from "@langchain/core/stores";

/**
 * Options for the {@linkcode DenoKvByteStore} class.
 */
interface DenoKvStoreFields {
  /**
   * `Deno.Kv` instance or path to `Deno.Kv` store. Defaults to the local
   * instance.
   */
  store?: Deno.Kv | string;
  /**
   * Prefix for keys in the store, defaults to
   * `["__langchain_store__"]`
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
  /**
   * The size of the batches in which the list operation is performed. Larger
   * or smaller batch sizes may positively or negatively affect the performance
   * of a list operation depending on the specific use case and iteration
   * behavior. Slow iterating queries may benefit from using a smaller batch
   * size for increased overall consistency, while fast iterating queries may
   * benefit from using a larger batch size for better performance.
   *
   * The default batch size is 100. The maximum value for this option is 500.
   * Larger values will be clamped.
   */
  batchSize?: number;
}

export class DenoKvByteStore extends BaseStore<string, Uint8Array> {
  #storePromise: Promise<Deno.Kv>;
  #prefix: Deno.KvKey;
  #batchSize?: number;
  #expireIn?: number;

  lc_namespace = ["langchain", "storage"];

  constructor(fields: DenoKvStoreFields = {}) {
    super(fields);
    const { store, prefix = ["__langchain_store__"], expireIn, batchSize } =
      fields;
    this.#storePromise = (!store || typeof store === "string")
      ? Deno.openKv(store)
      : Promise.resolve(store);
    this.#prefix = prefix;
    this.#batchSize = batchSize;
    this.#expireIn = expireIn;
  }

  override async mget(keys: string[]): Promise<(Uint8Array | undefined)[]> {
    const store = await this.#storePromise;
    const results: (Uint8Array | undefined)[] = [];
    for (const key of keys) {
      if (!key) {
        results.push(undefined);
        continue;
      }
      const value = await get(store, [...this.#prefix, key]);
      results.push(value.value ?? undefined);
    }
    return results;
  }

  override async mset(keyValuePairs: [string, Uint8Array][]): Promise<void> {
    const store = await this.#storePromise;
    for (const [key, value] of keyValuePairs) {
      const res = await set(store, [...this.#prefix, key], value, {
        expireIn: this.#expireIn,
      });
      if (!res.ok) {
        throw new Error(`Failed to set key ${key}`);
      }
    }
  }

  override async mdelete(keys: string[]): Promise<void> {
    const store = await this.#storePromise;
    for (const key of keys) {
      await remove(store, [...this.#prefix, key]);
    }
  }

  override async *yieldKeys(prefix?: string): AsyncGenerator<string> {
    const store = await this.#storePromise;
    for await (
      const { key } of list(store, { prefix: [...this.#prefix] }, {
        batchSize: this.#batchSize,
      })
    ) {
      const lastKeyPart = key[key.length - 1];
      if (
        prefix && typeof lastKeyPart === "string" &&
        !lastKeyPart.startsWith(prefix)
      ) {
        continue;
      }
      if (typeof lastKeyPart === "string") {
        yield lastKeyPart;
      }
    }
  }

  async end(): Promise<void> {
    (await this.#storePromise).close();
  }
}

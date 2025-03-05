import { query } from "@kitsonk/kv-toolbox/query";
import {
  type ListKeyOptions,
  RecordManager,
  type UpdateOptions,
} from "@langchain/core/indexing";

interface DenoKvRecordManagerOptions {
  /**
   * `Deno.Kv` instance or path to `Deno.Kv` store. Defaults to the local
   * instance.
   */
  store?: Deno.Kv | string;
  /**
   * Prefix for keys in the store, defaults to
   * `["__langchain_record_manager__"]`
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

interface DenoKvRecord {
  updatedAt: number;
  groupId: string | null;
}

export class DenoKvRecordManager extends RecordManager {
  #storePromise: Promise<Deno.Kv>;
  #prefix: Deno.KvKey;
  #expireIn?: number;

  override lc_namespace = ["langchain", "recordmanagers", "deno_kv"];

  constructor(options: DenoKvRecordManagerOptions = {}) {
    super();
    const { store, prefix = ["__langchain_record_manager__"], expireIn } =
      options;
    this.#storePromise = (!store || typeof store === "string")
      ? Deno.openKv(store)
      : Promise.resolve(store);
    this.#prefix = prefix;
    this.#expireIn = expireIn;
  }

  override createSchema(): Promise<void> {
    // noop
    return Promise.resolve();
  }

  override getTime(): Promise<number> {
    return Promise.resolve(Date.now());
  }

  override async update(
    keys: string[],
    updateOptions: UpdateOptions = {},
  ): Promise<void> {
    const store = await this.#storePromise;
    const updatedAt = await this.getTime();
    const { timeAtLeast, groupIds = Array(keys.length).fill(null) } =
      updateOptions;

    if (timeAtLeast && updatedAt < timeAtLeast) {
      throw new Error(
        `Time sync issue with database ${updatedAt} < ${timeAtLeast}`,
      );
    }

    if (groupIds.length !== keys.length) {
      throw new Error(
        `Number of keys (${keys.length}) does not match number of group_ids ${groupIds.length})`,
      );
    }

    for (const [i, key] of keys.entries()) {
      const maybeEntry = await store.get<DenoKvRecord>([...this.#prefix, key]);
      const entry = maybeEntry.value ?? { updatedAt: 0, groupId: groupIds[i] };
      entry.updatedAt = updatedAt;
      const res = await store.set([...this.#prefix, key], entry, {
        expireIn: this.#expireIn,
      });
      if (!res.ok) {
        throw new Error(`Failed to update key ${key}`);
      }
    }
  }

  override async exists(keys: string[]): Promise<boolean[]> {
    const store = await this.#storePromise;
    return await Promise.all(
      keys.map(async (key) => {
        try {
          const maybe = await store.get([...this.#prefix, key]);
          return !!maybe.versionstamp;
        } catch {
          return false;
        }
      }),
    );
  }

  override async listKeys(options: ListKeyOptions = {}): Promise<string[]> {
    const store = await this.#storePromise;
    const prefixLength = this.#prefix.length;
    const { before, after, groupIds, limit } = options;

    const q = query<DenoKvRecord>(store, { prefix: this.#prefix });
    if (before) {
      q.where("updatedAt", "<", before);
    }
    if (after) {
      q.where("updatedAt", ">", after);
    }
    if (groupIds) {
      q.where("groupId", "in", groupIds);
    }
    if (limit) {
      q.limit(limit);
    }

    const keys = await q.keys();

    return keys.map((key) => key[prefixLength] as string);
  }

  override async deleteKeys(keys: string[]): Promise<void> {
    const store = await this.#storePromise;
    for (const key of keys) {
      await store.delete([...this.#prefix, key]);
    }
  }

  /**
   * Closes the underlying `Deno.Kv` store.
   */
  async end() {
    (await this.#storePromise).close();
  }
}

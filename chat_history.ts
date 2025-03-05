import { BaseListChatMessageHistory } from "@langchain/core/chat_history";
import {
  type BaseMessage,
  mapChatMessagesToStoredMessages,
  mapStoredMessagesToChatMessages,
  type StoredMessage,
} from "@langchain/core/messages";

interface DenoKvRecordManagerOptions {
  sessionId: string;
  /**
   * `Deno.Kv` instance or path to `Deno.Kv` store. Defaults to the local
   * instance.
   */
  store?: Deno.Kv | string;
  /**
   * Prefix for keys in the store, defaults to
   * `["__langchain_message_history__"]`
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

export class DenoKvChatMessageHistory extends BaseListChatMessageHistory {
  #sessionId: string;
  #storePromise: Promise<Deno.Kv>;
  #prefix: Deno.KvKey;
  #expireIn?: number;

  override lc_namespace = ["langchain", "stores", "message", "deno_kv"];

  constructor(options: DenoKvRecordManagerOptions) {
    super(options);
    const {
      sessionId,
      prefix = ["__langchain_message_history__"],
      store,
      expireIn,
    } = options;
    this.#sessionId = sessionId;
    this.#storePromise = (!store || typeof store === "string")
      ? Deno.openKv(store)
      : Promise.resolve(store);
    this.#prefix = prefix;
    this.#expireIn = expireIn;
  }

  override async getMessages(): Promise<BaseMessage[]> {
    const store = await this.#storePromise;
    const messages: StoredMessage[] = [];
    for await (
      const { key, value } of store.list<StoredMessage>({
        prefix: [...this.#prefix, this.#sessionId],
      })
    ) {
      if (key.length === this.#prefix.length + 2) {
        messages.push(value);
      }
    }
    return mapStoredMessagesToChatMessages(messages);
  }

  override async addMessage(message: BaseMessage): Promise<void> {
    const store = await this.#storePromise;
    const maybeId = await store.get<Deno.KvU64>([
      ...this.#prefix,
      this.#sessionId,
    ]);
    let versionstamp: string | null = maybeId.versionstamp;
    if (maybeId.versionstamp === null) {
      const res = await store.set(
        [...this.#prefix, this.#sessionId],
        new Deno.KvU64(0n),
        { expireIn: this.#expireIn },
      );
      if (!res.ok) {
        throw new Error("Failed to set initial id in store");
      }
      versionstamp = res.versionstamp;
    }
    const id = maybeId?.value ?? new Deno.KvU64(0n);
    const [value] = mapChatMessagesToStoredMessages([message]);
    const res = await store
      .atomic()
      .check({
        key: [...this.#prefix, this.#sessionId],
        versionstamp,
      })
      .sum([...this.#prefix, this.#sessionId], 1n)
      .set(
        [...this.#prefix, this.#sessionId, id.value],
        value,
        { expireIn: this.#expireIn },
      )
      .commit();
    if (!res.ok) {
      throw new Error("Failed to add message to store");
    }
  }

  override async clear(): Promise<void> {
    const store = await this.#storePromise;
    const reqs: Promise<void>[] = [];
    for await (
      const { key } of store.list<StoredMessage>({
        prefix: [...this.#prefix, this.#sessionId],
      })
    ) {
      reqs.push(store.delete(key));
    }
    reqs.push(store.delete([...this.#prefix, this.#sessionId]));
    await Promise.all(reqs);
  }

  /**
   * Closes the underlying `Deno.Kv` store.
   */
  async end(): Promise<void> {
    const store = await this.#storePromise;
    store.close();
  }
}

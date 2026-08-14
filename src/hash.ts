import { createSHA256 } from 'hash-wasm'

export type IncrementalHasher = {
  update(data: Uint8Array): void
  digest(): Promise<string>
}

export async function createHasher(): Promise<IncrementalHasher> {
  const hasher = await createSHA256()
  hasher.init()
  return {
    update(data: Uint8Array) {
      hasher.update(data)
    },
    async digest() {
      return hasher.digest('hex')
    },
  }
}

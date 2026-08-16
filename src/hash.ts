import { createSHA256 } from 'hash-wasm'

export type IncrementalHasher = {
  update(data: Uint8Array): void
  digest(): Promise<string>
}

export async function hashBlob(blob: Blob): Promise<string> {
  const hasher = await createHasher()
  const reader = blob.stream().getReader()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    if (value) hasher.update(value)
  }
  return hasher.digest()
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

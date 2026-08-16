import type { FileEntry, FileProgress } from './types.ts'

export function emptyChecklist(files: FileEntry[]): FileProgress[] {
  return files.map((file) => ({
    path: file.path,
    size: file.size,
    status: 'pending',
    bytes: 0,
  }))
}

export function applyInventory(
  files: FileEntry[],
  inventory: Array<{ path: string; bytes: number }>,
): FileProgress[] {
  const present = new Map(inventory.map((item) => [item.path, item.bytes]))
  return files.map((file) => {
    const bytes = present.get(file.path) ?? 0
    const exists = present.has(file.path)
    if (file.size === 0 && exists) {
      return { path: file.path, size: 0, status: 'done', bytes: 0 }
    }
    if (file.size > 0 && bytes === file.size) {
      return { path: file.path, size: file.size, status: 'done', bytes: file.size }
    }
    if (bytes > 0) {
      return { path: file.path, size: file.size, status: 'partial', bytes }
    }
    return { path: file.path, size: file.size, status: 'pending', bytes: 0 }
  })
}

export function summarizeChecklist(list: FileProgress[]) {
  let done = 0
  let partial = 0
  let failed = 0
  let pending = 0
  let bytesDone = 0
  for (const file of list) {
    if (file.status === 'done') {
      done += 1
      bytesDone += file.size
    } else if (file.status === 'partial') {
      partial += 1
      bytesDone += file.bytes
    } else if (file.status === 'failed') {
      failed += 1
      bytesDone += file.bytes
    } else {
      pending += 1
    }
  }
  return {
    done,
    partial,
    failed,
    pending,
    remaining: partial + failed + pending,
    bytesDone,
    total: list.length,
  }
}

export function remainingFiles(list: FileProgress[], limit = Number.POSITIVE_INFINITY): FileProgress[] {
  const leftover = list.filter((file) => file.status !== 'done')
  if (!Number.isFinite(limit)) return leftover
  return leftover.slice(0, limit)
}

export function leftoverFiles(list: FileProgress[]): FileProgress[] {
  return list.filter((file) => file.status !== 'done')
}

export type ByteSpan = { start: number; end: number }

export function addByteSpan(spans: ByteSpan[], start: number, end: number): ByteSpan[] {
  if (end <= start) return spans
  const next = [...spans, { start, end }].sort((a, b) => a.start - b.start)
  const merged: ByteSpan[] = []
  for (const span of next) {
    const last = merged[merged.length - 1]
    if (!last || span.start > last.end) merged.push({ start: span.start, end: span.end })
    else last.end = Math.max(last.end, span.end)
  }
  return merged
}

export function contiguousEnd(spans: ByteSpan[], from = 0): number {
  let cursor = from
  for (const span of spans) {
    if (span.end <= cursor) continue
    if (span.start > cursor) break
    cursor = span.end
  }
  return cursor
}

export function coveredBytes(spans: ByteSpan[], start: number, end: number): number {
  let total = 0
  for (const span of spans) {
    const from = Math.max(span.start, start)
    const to = Math.min(span.end, end)
    if (to > from) total += to - from
  }
  return total
}

export function destBytesFor(file: FileProgress): number {
  if (file.status === 'done') return file.size
  if (file.status === 'partial' || file.status === 'failed') return file.bytes
  return 0
}

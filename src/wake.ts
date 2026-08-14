export class WakeGuard {
  private sentinel: WakeLockSentinel | null = null
  private visibleHandler: (() => void) | null = null

  async start(): Promise<void> {
    await this.request()
    this.visibleHandler = () => {
      if (document.visibilityState === 'visible') {
        void this.request()
      }
    }
    document.addEventListener('visibilitychange', this.visibleHandler)
  }

  async stop(): Promise<void> {
    if (this.visibleHandler) {
      document.removeEventListener('visibilitychange', this.visibleHandler)
      this.visibleHandler = null
    }
    if (this.sentinel) {
      try {
        await this.sentinel.release()
      } catch {
        // already released
      }
      this.sentinel = null
    }
  }

  private async request(): Promise<void> {
    if (!('wakeLock' in navigator)) return
    try {
      this.sentinel = await navigator.wakeLock.request('screen')
      this.sentinel.addEventListener('release', () => {
        this.sentinel = null
      })
    } catch {
      // User denied, or battery saver blocked it.
    }
  }
}

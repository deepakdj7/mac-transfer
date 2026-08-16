import type { MqttClient } from 'mqtt'
import { BROKERS, connectBroker } from './signaling.ts'

export type LobbyPeer = {
  id: string
  name: string
  hue: number
  ready: boolean
  updatedAt: number
}

export type LobbyInvite = {
  type: 'invite'
  from: string
  to: string
  room: string
  fromName: string
}

export type Lobby = {
  me: LobbyPeer
  setReady: (ready: boolean) => void
  invite: (to: string, room: string) => void
  close: () => void
}

const ADJ = ['Amber', 'Blue', 'Cedar', 'Dusk', 'Fern', 'Gold', 'Hazel', 'Ivory', 'Jade', 'Lake', 'Moss', 'North']
const NOUN = ['Mac', 'Maple', 'Otter', 'Pine', 'Quill', 'River', 'Stone', 'Teak', 'Umber', 'Vale', 'Willow', 'Yarrow']

function hueFor(id: string): number {
  let hue = 0
  for (const char of id) hue = (hue * 33 + char.charCodeAt(0)) % 360
  return hue
}

function randomName(): string {
  const adj = ADJ[Math.floor(Math.random() * ADJ.length)]
  const noun = NOUN[Math.floor(Math.random() * NOUN.length)]
  return `${adj} ${noun}`
}

function loadIdentity(): LobbyPeer {
  const stored = localStorage.getItem('mt-identity')
  if (stored) {
    try {
      const parsed = JSON.parse(stored) as LobbyPeer
      if (parsed.id && parsed.name) return { ...parsed, ready: false, updatedAt: Date.now() }
    } catch {
      // make a new one
    }
  }
  const id = crypto.randomUUID()
  const me = { id, name: randomName(), hue: hueFor(id), ready: false, updatedAt: Date.now() }
  localStorage.setItem('mt-identity', JSON.stringify({ id: me.id, name: me.name, hue: me.hue }))
  return me
}

async function publicNetworkKey(): Promise<string> {
  const urls = ['https://api.ipify.org?format=text', 'https://icanhazip.com']
  for (const url of urls) {
    try {
      const response = await fetch(url, { cache: 'no-store' })
      if (!response.ok) continue
      const ip = (await response.text()).trim()
      if (ip) return ip.replace(/[^\w.:]/g, '')
    } catch {
      // try the next echo service
    }
  }
  throw new Error('Could not see this network')
}

async function firstBroker(clientId: string, willTopic: string): Promise<MqttClient> {
  let lastError: Error | null = null
  for (const url of BROKERS) {
    try {
      return await connectBroker(url, clientId, willTopic)
    } catch (error) {
      lastError = error instanceof Error ? error : new Error('Broker connect failed')
    }
  }
  throw lastError ?? new Error('Could not reach a handshake server')
}

export async function joinLobby(handlers: {
  onPeers: (peers: LobbyPeer[]) => void
  onInvite: (invite: LobbyInvite) => void
  onStatus: (text: string) => void
}): Promise<Lobby> {
  const me = loadIdentity()
  handlers.onStatus('Finding devices on this Wi-Fi…')
  const network = await publicNetworkKey()
  const base = `mac-transfer-v2/lan/${network}`
  const peerTopic = (id: string) => `${base}/peer/${id}`
  const inboxTopic = (id: string) => `${base}/inbox/${id}`
  const client = await firstBroker(`mt-lobby-${me.id.slice(0, 8)}`, peerTopic(me.id))

  const peers = new Map<string, LobbyPeer>()
  const emit = () => {
    const now = Date.now()
    const live = [...peers.values()].filter((peer) => now - peer.updatedAt < 90000)
    handlers.onPeers(live)
  }

  const publishMe = () => {
    me.updatedAt = Date.now()
    client.publish(peerTopic(me.id), JSON.stringify(me), { retain: true, qos: 0 })
  }

  await new Promise<void>((resolve, reject) => {
    client.subscribe([`${base}/peer/+`, inboxTopic(me.id)], { qos: 0 }, (err) => {
      if (err) reject(err)
      else resolve()
    })
  })

  client.on('message', (topic, payload) => {
    const text = payload.toString()
    if (topic.startsWith(`${base}/peer/`)) {
      const id = topic.slice(`${base}/peer/`.length)
      if (!text) {
        peers.delete(id)
        emit()
        return
      }
      try {
        const peer = JSON.parse(text) as LobbyPeer
        if (!peer.id || peer.id === me.id) return
        peers.set(peer.id, { ...peer, hue: peer.hue ?? hueFor(peer.id), updatedAt: Date.now() })
        emit()
      } catch {
        // ignore
      }
      return
    }
    if (topic !== inboxTopic(me.id) || !text) return
    try {
      const invite = JSON.parse(text) as LobbyInvite
      if (invite.type === 'invite' && invite.to === me.id && invite.room) handlers.onInvite(invite)
    } catch {
      // ignore
    }
  })

  publishMe()
  const beat = window.setInterval(publishMe, 20000)
  handlers.onStatus('Open this page on the other Mac. Click their circle to send a folder.')
  emit()

  return {
    me,
    setReady(ready: boolean) {
      me.ready = ready
      publishMe()
    },
    invite(to: string, room: string) {
      const invite: LobbyInvite = { type: 'invite', from: me.id, to, room, fromName: me.name }
      client.publish(inboxTopic(to), JSON.stringify(invite), { qos: 0 })
    },
    close() {
      window.clearInterval(beat)
      try {
        client.publish(peerTopic(me.id), '', { retain: true, qos: 0 })
        client.end(true)
      } catch {
        // ignore
      }
    },
  }
}

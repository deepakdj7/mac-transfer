import mqtt, { type MqttClient } from 'mqtt'
import type { Role, SignalMessage } from './types.ts'

export const BROKERS = ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt']
const APP = 'mac-transfer-v1'

export type SignalingHandlers = {
  onPeerPresent?: () => void
  onSignal: (message: SignalMessage) => void
  onStatus?: (text: string) => void
  onError?: (error: Error) => void
}

export type SignalingRoom = {
  publish: (message: SignalMessage) => void
  close: () => void
}

function topics(roomCode: string) {
  const base = `${APP}/${roomCode}`
  return {
    signal: `${base}/signal`,
    sender: `${base}/sender`,
    receiver: `${base}/receiver`,
  }
}

export function connectBroker(url: string, clientId: string, willTopic: string): Promise<MqttClient> {
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(url, {
      clientId,
      clean: true,
      keepalive: 30,
      reconnectPeriod: 0,
      connectTimeout: 8000,
      protocolVersion: 4,
      will: {
        topic: willTopic,
        payload: '',
        retain: true,
        qos: 0,
      },
    })

    const onError = (err: Error) => {
      client.removeAllListeners()
      try {
        client.end(true)
      } catch {
        // ignore
      }
      reject(err)
    }

    client.once('connect', () => {
      client.off('error', onError)
      resolve(client)
    })
    client.once('error', onError)
  })
}

export async function joinSignalRoom(
  roomCode: string,
  role: Role,
  handlers: SignalingHandlers,
): Promise<SignalingRoom> {
  const id = crypto.randomUUID()
  const t = topics(roomCode)
  const clientId = `mt-${role}-${id.slice(0, 8)}`
  let client: MqttClient | null = null
  let lastError: Error | null = null

  handlers.onStatus?.('Finding a handshake server…')

  for (const url of BROKERS) {
    try {
      client = await connectBroker(url, clientId, t[role])
      lastError = null
      break
    } catch (err) {
      lastError = err instanceof Error ? err : new Error('Broker connect failed')
    }
  }

  if (!client) {
    throw lastError ?? new Error('Could not reach a signaling broker')
  }

  const otherRole: Role = role === 'sender' ? 'receiver' : 'sender'
  let peerSeen = false

  await new Promise<void>((resolve, reject) => {
    client!.subscribe([t.signal, t.sender, t.receiver], { qos: 0 }, (err) => {
      if (err) reject(err)
      else resolve()
    })
  })

  client.on('message', (topic, payload) => {
    const text = payload.toString()
    if (topic === t[otherRole] && text.length > 0 && !peerSeen) {
      peerSeen = true
      handlers.onPeerPresent?.()
      return
    }
    if (topic !== t.signal || !text) return
    try {
      const message = JSON.parse(text) as SignalMessage
      if ('id' in message && message.id === id) return
      if ('role' in message && message.role === role) return
      handlers.onSignal(message)
    } catch {
      // ignore malformed handshake packets
    }
  })

  client.on('error', (err) => {
    handlers.onError?.(err)
  })

  client.publish(t[role], id, { retain: true, qos: 0 })
  client.publish(t.signal, JSON.stringify({ type: 'hello', role, id } satisfies SignalMessage), { qos: 0 })
  handlers.onStatus?.('Waiting for the other Mac…')

  return {
    publish(message: SignalMessage) {
      client?.publish(t.signal, JSON.stringify(message), { qos: 0 })
    },
    close() {
      try {
        client?.publish(t[role], '', { retain: true, qos: 0 })
        client?.end(true)
      } catch {
        // ignore
      }
      client = null
    },
  }
}

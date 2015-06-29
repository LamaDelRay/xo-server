import {parse} from 'url'
import {request} from 'https'

export default function proxyConsole (ws, console, sessionId) {
  const url = parse(console.location)

  const req = request({
    method: 'connect',
    headers: {
      cookie: `session_id=${sessionId}`
    },

    host: url.host,
    path: url.path
  })

  req.on('connect', (response, socket, head) => {
    socket.on('data', data => {
      ws.send(data)
    })
    ws.on('message', data => {
      socket.write(data)
    })
  })
}

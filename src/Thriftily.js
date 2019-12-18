const thrift = require('thrift')
const EventEmitter = require('events')
class Thriftily extends EventEmitter {
  constructor (alias, { useHTTPConnection, ...clientConfig }, logger) {
    super()
    this.config = clientConfig
    this.thriftCreateConnection = useHTTPConnection ? thrift.createHttpConnection.bind(thrift) : thrift.createConnection.bind(thrift)
    this.thriftCreateClient = useHTTPConnection ? thrift.createHttpClient.bind(thrift) : thrift.createClient.bind(thrift)
  }

  start () {
    this.createConnection()
    this.createClient()
  }

  close () {
    this.connection.end()
    this.connection.destroy()
  }

  createConnection () {
    const {
      host,
      port,
      transport = thrift.TBufferedTransport,
      protocol = thrift.TBinaryProtocol,
      ...otherOptions
    } = this.config

    const connection = this.thriftCreateConnection(host, port, {
      transport,
      protocol,
      ...otherOptions
    })

    this.connection = connection

    connection.on('connect', e => this.emit('connect', e))
    connection.on('close', e => this.emit('close', e))
    connection.on('error', e => this.emit('error', e))
    connection.on('timeout', e => this.emit('timeout', e))

    return connection
  }

  createClient () {
    const client = this.thriftCreateClient(this.config.client, this.connection)
    this.client = client
    return client
  }
}

module.exports = Thriftily

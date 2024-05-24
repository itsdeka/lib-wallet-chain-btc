const { EventEmitter } = require('events')
const { Bitcoin } = require('../../wallet/src/currency.js')

class Electrum extends EventEmitter {

  constructor(config) {
    super()
    if(!config.host || !config.port) throw new Error('Network is required')
    this._subscribe()
    this.port = config.port
    this.host = config.host
    this._net = config.net || require('net')
    this.clientState = 0
    this.requests = new Map()
    this.cache = new Map()
    this.block_height = 0
    this._max_cache_size = 100
    this._reconnect_count = 0
    this._max_attempt = 10
    this._reconnect_interval = 2000
  }

  _subscribe() {
    this.on('blockchain.headers.subscribe', (height) => {
      this.block_height = height.height
      this.emit('new-block', height)
    })
    
    this.on('blockchain.scripthash.subscribe', (data) => {
      this.emit('new-tx', data)
    })
  }

  /**
  * Connect to electrum server
  * @param {Object} opts - options
  * @param {Boolean} opts.reconnect - reconnect if connection is lost.
  **/
  connect(opts = {}) {
    let isDone = false

    if(opts.reconnect) this._reconnect_count = 0
    return new Promise((resolve, reject) => {
      this._client = this._net.createConnection(this.port, this.host, () => {
        this.clientState = 1
        this._reconnect_count = 0
        resolve()
        isDone = true
      })
      this._client.on('data', (data) => {
        const response = data.toString().split('\n')
        response.forEach((data) => {
          if(!data) return
          this._handleResponse(data)
        })
      })
      this._client.once('close', async (err) => {
        this.clientState = 0
        this._reconn(resolve,reject, _err)
      })
      let _err
      this._client.once('error', (err) => {
        _err = err
        this.clientState = 0
      })
    })
  }

  async _reconn(resolve, reject, err = {}) {
    const errMsg = err.message || err.errors?.map(e => e.message).join(' ')
    if(this._reconnect_count >= this._max_attempt) return reject(new Error('gave up connecting to electrum '+ errMsg))
    setTimeout(async () => {
      if(this._reconnect_count >= this._max_attempt) return reject(new Error('gave up connecting to electrum '+ errMsg))
      this._reconnect_count++
      try {
        await this.connect()
      } catch(err) {
        if(this._reconnect_count >= this._max_attempt) return reject(err)
        await this._reconn(resolve, reject)
        return
      }
      resolve()
    }, this._reconnect_interval)
  }
  
  _rpcPayload (method, params, id) {
    return JSON.stringify({
		  jsonrpc: '2.0',
      id,
      method,
      params
    })
  }

  _makeRequest (method, params) {
    return new Promise( async (resolve, reject) => {
      if(this.clientState !== 1) {
        return reject(new Error('client not connected'))
      }
      let id = Date.now() +"-"+parseInt(Math.random() * 100000000) 
      const data = this._rpcPayload(method, params, id)
      this.requests.set(id, [resolve, reject, method])
      this._client.write(data+"\n")
    })
  }


  _handleResponse(data) {
    let resp
    try {
      resp = JSON.parse(data.toString())
    } catch(err) { 
      this.emit('request-error', err)
      return 
    }

    if(resp?.method?.includes('.subscribe')) {
      this.emit(resp.method, resp.params.pop())
      this.requests.delete(resp?.id)
      return
    }

    const _resp = this.requests.get(resp.id)
    const [resolve, reject, method] =_resp || []

    if(!resolve) return this.emit('request-error', `no handler for response id: ${resp.id} - ${JSON.stringify(resp)}`)

    const isNull = resp.result === null 
    resolve(isNull ? null : (resp.result || resp.error))
    this.requests.delete(resp.id)
  }

  async getAddressHistory(scriptHash) {
    let txData
    try {

      const history = await this._makeRequest('blockchain.scripthash.get_history', [scriptHash])
      txData = await Promise.all(history.map(async (tx, index) => {
        const txData = await this.getTransaction(tx.tx_hash, scriptHash)
        txData.height = history[index].height;
        return txData
      }))
    } catch(err) {
      return { error : err }
    }
    return txData
  }

  _processTxVout(vout) {
    return {
      address: this._getTxAddress(vout.scriptPubKey),
      value: new Bitcoin(vout.value, 'main'),
      witness_hex: vout.scriptPubKey.hex
    }
  }

  _getTransaction(txid) {
    return this._makeRequest('blockchain.transaction.get', [txid, true])
  }

  _getBalance(scriptHash) {
    return this._makeRequest('blockchain.scripthash.get_balance', [scriptHash])
  }

  async broadcastTransaction(tx) {
    return this._makeRequest('blockchain.transaction.broadcast', [tx])
  }

  async getTransaction(txid, sc) {
    const cache = this.cache
    const data = {
      txid,
      out : [],
      in : []
    }

    const getOrFetch = async (txid) => {

      if(cache.has(txid)) {
        return cache.get(txid)
      }
      const data = await this._getTransaction(txid)
      if(cache.size > this._max_cache_size) {
        cache.delete(cache.keys().next().value);
      }
      cache.set(txid, data)
      return data
    }

    const tx = await getOrFetch(txid)
    let totalOut = new Bitcoin(0, 'main')
    data.out = tx.vout.map((vout) => {
      const newvout = this._processTxVout(vout)
      newvout.index = vout.n
      newvout.txid = txid
      totalOut = totalOut.add(newvout.value)
      return newvout
    })

    let totalIn = new Bitcoin(0, 'main')
    data.in = await Promise.all(tx.vin.map(async (vin) => {
      const txDetail = await getOrFetch(vin.txid)
      const newvin = this._processTxVout(txDetail.vout[vin.vout])
      newvin.prev_txid = vin.txid
      newvin.prev_index = vin.vout
      newvin.txid = txid
      totalIn = totalIn.add(newvin.value)
      return newvin
    }))
    data.fee = totalIn.minus(totalOut)
    return data
  }

  _getTxAddress(scriptPubKey) {
    if(scriptPubKey.address) return scriptPubKey.address
    if(scriptPubKey.addresses) return scriptPubKey.addresses
    return null
  }

  async subscribeToBlocks() {
    const height = await this._makeRequest('blockchain.headers.subscribe', [])
    this.block_height = height.height
    this.emit('new-block', height)
  }

  close() {
    return new Promise((resolve) => {
      this.clientState = 0
      this._reconnect_count = this._max_attempt
      this._client.on('end', () => resolve())
      this._client.end()
    })
  }

  rpc(method, params) {
    return this._makeRequest(method, params)
  }
 
  async ping (opts) {
    const res = await this._makeRequest('server.ping', [])
    if(!res) return 'pong'
    throw new Error('ping failed')
  }

  async subscribeToAddress(scriptHash) {
    return this._makeRequest('blockchain.scripthash.subscribe', [scriptHash])
  }
}


module.exports = Electrum

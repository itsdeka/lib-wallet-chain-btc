// Copyright 2024 Tether Operations Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict'
const Bitcoin = require('./currency')
const { ConnectionManager } = require('lib-wallet')
const { STATUS } = ConnectionManager.ConnectionStatus
const net = require('net')

function getBlockReward (height) {
  const initialReward = Bitcoin.BN(50).times(100000000) // 50 BTC in satoshis
  const halvingInterval = 210000
  const halvings = Math.floor(height / halvingInterval)
  const reward = initialReward.dividedBy(Bitcoin.BN(2).pow(halvings))
  return new Bitcoin(reward, 'base')
}

/**
* @class RequestCache
* @desc Cache requests to electrum server
* @param {Object} config - configuration
* @param {Object} config.store - store to cache requests
* @param {Number} config.cache_timeout - cache timeout
* @param {Number} config.max_cache_size - max cache size
* @param {Number} config.cache_interval - cache interval
* @param {Number} config.cache_size - cache size
**/
class RequestCache {
  constructor (config) {
    this.store = config.store
    this._cache_expiry = config.cache_timeout || 300000 // 5min
    this._max_cache_size = config.max_cache_size || 10000
    this._cache_size = 0
    this._closing = false
  }

  async clear () {
    return this.store.clear()
  }

  async stop () {
    clearInterval(this._timer)
    return this.store.close()
  }

  _startCacheTimer () {
    this._timer = setInterval(() => {
      this.store.entries(async (k, [_, exp]) => {
        if (Date.now() >= exp) return await this.store.delete(k)
      })
    }, this._cache_interval)
  }

  async _getCacheIndex () {
    return await (this.store.get('cache_index')) || []
  }

  async _removeOldest () {
    const index = await this._getCacheIndex()
    const key = index.shift()
    await this.store.delete(key)
    await this.store.put('cache_index', index)
  }

  async set (key, value) {
    let data
    if (this._cache_size >= this._max_session_size) {
      await this._removeOldest()
    }
    if (!value.expiry) {
      data = [value, Date.now() + this._cache_expiry]
    } else {
      data = [value, value.expiry]
    }
    const index = await this._getCacheIndex()
    index.push(key)
    await this.store.put('cache_index', index)
    this.size = index.length
    return this.store.put(key, data)
  }

  async get (key) {
    const data = await this.store.get(key)
    return data ? data[0] : null
  }

  get size () {
    return this._cache_size
  }

  set size (val) {
    return null
  }
}

class Electrum extends ConnectionManager {
  constructor (config = {}) {
    super({
      name: 'provider'
    })
    this._subscribe()
    this.port = config.port || 8001
    this.host = config.host || 'http://127.0.0.1'
    this._net = config.net || net
    this.requests = new Map()
    this.cache = new RequestCache({ store: config.store.newInstance({ name: 'electrum-cache' }) })
    this.block_height = 0
    this._max_cache_size = 10
    this._setEndpoint({
      port: this.port,
      host: this.host
    })
  }

  static OutTypes = {
    0: 'non-standard',
    1: 'standard'
  }

  _subscribe () {
    this.on('blockchain.headers.subscribe', (height) => {
      this.block_height = height.height
      this.emit('new-block', height)
    })

    this.on('blockchain.scripthash.subscribe', (...args) => {
      this.emit('new-tx', ...args)
    })
  }

  /**
  * Connect to electrum server
  * @param {Object} opts - options
  * @param {Boolean} opts.reconnect - reconnect if connection is lost.
  **/
  connect () {
    return new Promise((resolve) => {
      if (this.isConnected()) {
        return resolve()
      }
      this.setStatus(STATUS.CONNECTING)
      this._client = this._net.createConnection(this.port, this.host, () => {
        this.setStatus(STATUS.CONNECTED)
        resolve()
      })
      this._client.on('data', (data) => {
        const response = data.toString().split('\n')
        response.forEach((data) => {
          if (!data) return
          this._handleResponse(data)
        })
      })
      this._client.once('end', () => {
        this.setStatus(STATUS.DISCONNECTED)
      })
      this._client.once('error', (err) => {
        console.log(err)
        this.setStatus(STATUS.ERROR)
      })
    })
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
    return new Promise((resolve, reject) => {
      if (!this.isConnected()) throw new Error('not connected')
      const id = Date.now() + '-' + parseInt(Math.random() * 100000000)
      const data = this._rpcPayload(method, params, id)
      this.requests.set(id, [resolve, reject, method])
      try {
        this._client.write(data + '\n')
      } catch {
      }
    })
  }

  _handleResponse (data) {
    let resp
    try {
      resp = JSON.parse(data.toString())
    } catch (err) {
      this.emit('request-error', err)
      return
    }

    if (resp?.method?.includes('.subscribe')) {
      this.emit(resp.method, ...resp.params)
      this.requests.delete(resp?.id)
      return
    }

    const _resp = this.requests.get(resp.id)
    const [resolve, reject, method] = _resp || []

    if (resp.error) {
      if (reject) {
        reject(new Error(`RPC Error: ${JSON.stringify(resp.error)} - ${method}`))
      }
      return this.requests.delete(resp.id)
    }

    if (!resolve) return this.emit('request-error', `no handler for response id: ${resp.id} - ${JSON.stringify(resp)}`)

    const isNull = resp.result === null

    resolve(isNull ? null : (resp.result || resp.error))
    this.requests.delete(resp.id)
  }

  async getAddressHistory (opts, scriptHash) {
    const history = await this._makeRequest('blockchain.scripthash.get_history', [scriptHash])
    const txData = await Promise.all(
      history.map(tx => this.getTransaction(tx.tx_hash, opts))
    )
    return txData
  }

  async getMempoolTx (opts = {}, scriptHash) {
    const history = await this._makeRequest('blockchain.scripthash.get_mempool', [scriptHash])
    const txData = await Promise.all(
      history.map(tx => this.getTransaction(tx.tx_hash, opts))
    )
    return txData
  }

  _getTransaction (txid) {
    return this._makeRequest('blockchain.transaction.get', [txid, true])
  }

  _getBalance (scriptHash) {
    return this._makeRequest('blockchain.scripthash.get_balance', [scriptHash])
  }

  async broadcastTransaction (tx) {
    return this._makeRequest('blockchain.transaction.broadcast', [tx])
  }

  _processTxVout (vout, tx) {
    return {
      address: this._getTxAddress(vout.scriptPubKey),
      value: new Bitcoin(vout.value, 'main'),
      witness_hex: vout?.scriptPubKey.hex,
      index: vout.n,
      txid: tx.txid,
      height: tx.height
    }
  }

  _procTxHeight (tx) {
    if (!tx.confirmations) {
      tx.height = 0
    } else {
      tx.height = this.block_height - (tx.confirmations - 1)
    }
    return tx
  }

  async _txGet (txid, opts) {
    const cache = this.cache

    if (opts.cache === false) {
      let data = await this._getTransaction(txid)
      data = this._procTxHeight(data)
      await cache.set(txid, data)
      return data
    }
    const cacheValue = await cache.get(txid)
    if (cacheValue && cacheValue.height !== 0) {
      return cacheValue
    }
    let data = await this._getTransaction(txid)
    data = this._procTxHeight(data)
    await cache.set(txid, data)
    return data
  }

  /**
  * @description get transaction details. Store tx in cache.
  */
  async getTransaction (txid, opts = {}) {
    const data = {
      txid,
      out: [],
      in: [],
      unconfirmed_inputs: [],
      std_out: [],
      std_in: []
    }

    const tx = await this._txGet(txid, opts)
    data.height = tx.height

    let totalOut = new Bitcoin(0, 'main')
    data.out = tx.vout.map((vout) => {
      const newvout = this._processTxVout(vout, tx)
      if (!newvout || !newvout.address) {
        data.std_out.push(false)
        return null
      }
      data.std_out.push(true)
      totalOut = totalOut.add(newvout.value)
      newvout.tx_height = tx.height
      return newvout
    }).filter(Boolean)

    let totalIn = new Bitcoin(0, 'main')
    data.in = await Promise.all(tx.vin.map(async (vin) => {
      if (vin.coinbase) {
        const value = getBlockReward(tx.height - 1)
        data.std_in.push(false)
        return {
          prev_txid: `${vin.coinbase}00000000`,
          prev_index: 0,
          prev_tx_height: tx.height - 1,
          txid: vin.coinbase,
          address: vin.coinbase,
          out_type: 0,
          value
        }
      }
      data.std_in.push(false)
      const txDetail = await this._txGet(vin.txid, opts)
      const newvin = this._processTxVout(txDetail.vout[vin.vout], tx)
      newvin.prev_txid = vin.txid
      newvin.prev_index = vin.vout
      newvin.prev_tx_height = txDetail.height
      if (txDetail.height === 0) data.unconfirmed_inputs.push(vin.txid)
      totalIn = totalIn.add(newvin.value)
      return newvin
    }))

    if (totalIn.toNumber() === 0) {
      data.fee = totalIn
    } else {
      data.fee = totalIn.minus(totalOut)
    }

    return data
  }

  _getTxAddress (scriptPubKey) {
    if (scriptPubKey.address) return scriptPubKey.address
    // if (scriptPubKey.addresses) return scriptPubKey.addresses
    // Non standard outputs like OP_RETURN, multi-sig
    return null
  }

  async subscribeToBlocks () {
    const height = await this._makeRequest('blockchain.headers.subscribe', [])
    this.block_height = height.height
    this.emit('new-block', height)
  }

  async close () {
    super.destroy()
    await this._stopClient()
    await this.cache.stop()
  }

  _stopClient () {
    return new Promise((resolve) => {
      if (!this._client) return resolve()

      this._client.once('close', () => {
        resolve()
        this.setStatus(STATUS.DISCONNECTED)
      })
      this._client.end()
    })
  }

  async reconnect () {
    await this._stopClient()
  }

  rpc (method, params) {
    return this._makeRequest(method, params)
  }

  async subscribeToAddress (scriptHash) {
    return this._makeRequest('blockchain.scripthash.subscribe', [scriptHash])
  }

  async unsubscribeFromAddress (scriptHash) {
  }
}

module.exports = Electrum

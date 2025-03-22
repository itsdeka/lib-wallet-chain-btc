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
const { WalletPay } = require('lib-wallet')
const TxEntry = WalletPay.TxEntry

class Balance {
  constructor (confirmed, pending, mempool, txid) {
    // @desc: confirmed balance. Tx that have more than X amount of confirmations
    this.confirmed = new Bitcoin(confirmed || 0, 'main')
    // @desc: pending balance. Tx that have less than X amount of confirmations but more than 0
    this.pending = new Bitcoin(pending || 0, 'main')
    // @desc: mempool balance. Tx that are in the mempool, 0 confirmations
    this.mempool = new Bitcoin(mempool || 0, 'main')

    this.txid = txid || {
      confirmed: [],
      pending: [],
      mempool: []
    }
  }

  addTxid (state, txid, amount) {
    for (const state in this.txid) {
      this.txid[state] = this.txid[state].filter(([tx]) => {
        if (tx === txid) {
          this.minusBalance(state, amount)
          return false
        }
        return true
      })
    }
    this.addBalance(state, amount)
    this.txid[state].push([txid, amount])
  }

  getTx (state, key) {
    return this.txid[state].filter(([tx]) => {
      return tx === key
    }).pop()
  }

  addBalance (state, amount) {
    this[state] = this[state].add(amount)
  }

  minusBalance (state, amount) {
    this[state] = this[state].minus(amount)
  }

  combine (t2) {
    const total = new Balance(0, 0, 0)
    total.mempool = this.mempool.minus(t2.mempool)
    total.confirmed = this.confirmed.minus(t2.confirmed)
    total.pending = this.pending.minus(t2.pending)
    return total.formatted()
  }

  formatted () {
    return {
      confirmed: this.confirmed,
      pending: this.pending,
      mempool: this.mempool,
      consolidated: this.confirmed.add(this.pending).add(this.mempool)
    }
  }
}

class AddressManager {
  constructor (config) {
    // @desc: address store that keeps track of balances
    this.store = config.store.newInstance({ name: 'addr' })
    // @desc: transaction history store that holds tx details from electrum
    this.history = config.store.newInstance({ name: 'tx-history' })
    // @desc: Transactions that has been broadcasted
    this.outgoings = config.store.newInstance({ name: 'broadcasted' })
  }

  async init () {
    await this.store.init()
  }

  async close () {
    await this.store.close()
    await this.history.close()
  }

  _newAddr () {
    return {
      in: new Balance(0, 0, 0),
      out: new Balance(0, 0, 0),
      fee: new Balance(0, 0, 0)
    }
  }

  async has (addr) {
    return !!this.get(addr)
  }

  async clear () {
    await this.store.clear()
    await this.history.clear()
    await this.outgoings.clear()
  }

  async newAddress (addr) {
    const exist = await this.get(addr)
    if (exist) return exist
    const data = this._newAddr()
    await this.store.put(addr, data)
    return data
  }

  set (addr, data) {
    return this.store.put(addr, data)
  }

  async get (addr) {
    const data = await this.store.get(addr)
    if (!data) return null
    return {
      in: new Balance(data.in.confirmed, data.in.pending, data.in.mempool, data.in.txid),
      out: new Balance(data.out.confirmed, data.out.pending, data.out.mempool, data.out.txid),
      fee: new Balance(data.fee.confirmed, data.fee.pending, data.fee.mempool, data.fee.txid)
    }
  }

  /**
  * @desc Get transaction history by block height
  */
  async getTxHeight (height) {
    const prf = 'i:'
    let results = []
    await this.history.entries(async (key, value) => {
      if (key.indexOf(prf) !== 0 || !value) return
      const h = key.split(':')[1]
      if (+h !== height) return
      results = results.concat(value)
    }, { gt: prf + height, lt: `${prf}${height + 1}` }, {})
    return results
  }

  _getDbTx (txid) {
    return this.history.get('txid:' + txid)
  }

  async getHeight (txid) {
    return this.history.get(`tx:${txid}`)
  }

  async storeTx (tx) {
    await this.history.delete(`i:0:${tx.txid}`, tx)
    await this.history.delete(`i:${tx.height-1}:${tx.txid}`, tx)
    await this.history.put(`i:${tx.height}:${tx.txid}`, tx)
    await this.history.put(`tx:${tx.txid}`, tx.height)
  }
  
  async updateTxLabel (opts) {
    const tx = await this._getDbTx(opts.txid)
    if (!tx) throw new Error('Transaction not found')
    tx.label = opts.label
    await this.storeTx(tx)
  }

  getMempoolTx () {
    return this.history.get('i:' + 0)
  }

  /**
  * @desc get transaction history from history store
  * @param {function} fn callback function to process each transaction
  * @returns {Promise}
  */
  async getTransactions (opts = {}) {
    let results = []
    let skipped = 0
    const limit = opts.limit || 1000
    const offset = opts.offset || 0

    await this.history.entries(async (key, value) => {
      if (skipped < offset) {
        skipped++
        return
      }
      if (results.length >= limit) return
      if (key.indexOf('i:') !== 0 || !value) return
      results = results.concat(new TxEntry(value))
    }, { gt: 'i:0', lt: `i:${'9'.repeat(1000000)}`, reverse: !opts.reverse })
    return results
  }

  addSentTx (tx) {
    return this.outgoings.put(tx.txid, tx)
  }

  getSentTx (txid) {
    return this.outgoings.get(txid)
  }
}

module.exports = {
  AddressManager,
  Balance
}

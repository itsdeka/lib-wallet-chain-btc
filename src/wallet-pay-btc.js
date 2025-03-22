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

const { WalletPay, HdWallet } = require('lib-wallet')
const Provider = require('./provider.js')
const KeyManager = require('./wallet-key-btc.js')
const Transaction = require('./transaction.js')
const SyncManager = require('./sync-manager.js')
const Bitcoin = require('./currency')
const FeeEstimate = require('./fee-estimate.js')
const {
  BlockCounter,
  StateDb
} = require('./utils.js')

const WalletPayError = Error

class WalletPayBitcoin extends WalletPay {
  static networks = ['regtest', 'mainnet', 'testnet', 'signet', 'bitcoin']
  static events = ['ready', 'synced-path', 'new-tx']

  /**
  * Creates a new WalletPayBitcoin instance.
  * @param {Object} config - Configuration object.
  * @param {Object} [config.provider=Electrum] - Block data provider.
  * @param {Object} config.store - Store instance.
  * @param {Object} [config.key_manager=WalletKeyBtc] - Key manager instance.
  * @param {Seed} [config.seed] - Seed for key manager.
  * @param {string} config.network - Blockchain network.
  * @param {number} [config.gapLimit=20] - Gap limit for scanning balances.
  * @param {number} [config.min_block_confirm=1] - Minimum number of block confirmations.
  * @throws {WalletPayError} If an invalid network is provided.
  */
  constructor (config) {
    super(config)
    if (!WalletPayBitcoin.networks.includes(this.network)) throw new WalletPayError('Invalid network')

    this._electrum_config = config.electrum || {}
    this.gapLimit = config.gapLimit || 20
    this.min_block_confirm = config.min_block_confirm || 1
    this.ready = false
    this.currency = Bitcoin
    this.keyManager = config.key_manager || null
    // @desc: Only supported address type.
    this._addressType = 'p2wpkh'
    this.Currency = Bitcoin
    this._feeEst = new FeeEstimate()
  }

  /**
   * Destroys the instance, closing connections and pausing sync.
   * @async
   */
  async _destroy () {
    await this.pauseSync()
    await this.provider.close()
    await this._syncManager.close()
    await this.state.store.close()
    await this._hdWallet.close()
    await this.keyManager.close()
    this.ready = false
  }

  async initialize (wallet) {
    if (this.ready) return

    // @desc use default key manager
    if (!this.keyManager) {
      this.keyManager = new KeyManager({ seed: wallet.seed, network: this.network })
      await this.keyManager.init()
    }

    // Add asset to wallet
    await super.initialize(wallet)

    if (!this.keyManager.network) {
      this.keyManager.setNetwork(this.network)
    }

    if (!this.provider) {
      this._electrum_config.store = this.store
      this.provider = new Provider(this._electrum_config)
    }

    if (!this.provider.isConnected()) {
      await this.provider.connect()
    }

    this._loadPlugin('provider')

    let coinType
    if (['bitcoin', 'mainnet'].includes(this.network)) {
      coinType = "0'"
    } else {
      coinType = "1'"
    }

    this._hdWallet = new HdWallet({
      store: this.store.newInstance({ name: 'hdwallet' }),
      coinType,
      purpose: "84'",
      gapLimit: this.gapLimit
    })

    this.state = new StateDb({
      store: this.store.newInstance({ name: 'state' })
    })

    this._syncManager = new SyncManager({
      state: this.state,
      gapLimit: this.gapLimit,
      hdWallet: this._hdWallet,
      utxoManager: this._utxoManager,
      provider: this.provider,
      keyManager: this.keyManager,
      currentBlock: this.latest_block,
      minBlockConfirm: this.min_block_confirm,
      store: this.store,
      addressType: this._addressType
    })

    this.block = new BlockCounter({ state: this.state })
    await this.block.init()

    let isBlockLoaded = false
    const newBlock = new Promise((resolve) => {
      this.block.on('new-block', async (block) => {
        this.emit('new-block', block)
        await this._syncManager.updateBlock(block)
        if (!isBlockLoaded) {
          isBlockLoaded = true
          resolve()
        }
      })
    })

    await this.state.init()
    await this._syncManager.init()
    await this._hdWallet.init()
    const electrum = new Promise((resolve) => {
      this.provider.once('new-block', () => {
        this.ready = true
        this.emit('ready')
        resolve()
      })
    })

    this.provider.on('new-block', async (block) => {
      this.block.setBlock(block)
    })
    await this.provider.subscribeToBlocks()

    this._syncManager.on('synced-path', (...args) => {
      this.emit('synced-path', ...args)
    })
    this._syncManager.on('new-tx', (...args) => {
      this.emit('new-tx', ...args)
    })

    return Promise.all([newBlock, electrum])
  }

  _onNewTx () {
    return new Promise((resolve) => {
      this.once('new-tx', () => resolve())
    })
  }

  async _getNewAddr (config) {
    const addrType = this._addressType
    const res = await this._hdWallet.getNewAddress(config.inout, (path) => {
      return this.keyManager.pathToScriptHash(path, addrType)
    })
    await this._syncManager.watchAddress([res.hash, res.addr], config.inout)
    return res.addr
  }

  /**
   * @description Get a new address
   * @returns {Object}
   **/
  async getNewAddress () {
    return this._getNewAddr({ inout: 'ext' })
  }

  /**
   * @description get an internal change  address
  **/
  async _getInternalAddress () {
    return this._getNewAddr({ inout: 'in' })
  }

  /**
   * @description get wallet transaction history
   * @param {function} fn function that gets called with list of transaction. by block number
   * @returns {Promise}
   **/
  getTransactions (opts, fn) {
    return this._syncManager.getTransactions(opts, fn)
  }

  /**
   * @description  get addresses and their balances
   * @param {object} opts
   * @returns Map
   **/
  async getFundedTokenAddresses (opts) {
    const accts = {}
    const addrs = await this._hdWallet.getAllAddress()

    await Promise.all(addrs.map(async (addr) => {
      const bal = await this.getBalance({}, addr)
      if (bal.consolidated.toNumber() > 0) {
        accts[addr] = bal
      }
    }))
    return accts
  }

  /**
   * @description get balance of entire wallet or 1 address
   * @params {object} opts place holder, empty object
   * @param {string?} addr bitcoin address
   * @returns {Balance} balance object
  **/
  getBalance (opts, addr) {
    return this._syncManager.getBalance(addr)
  }

  /**
  * Sync transactions
  * @param {Object} opts - Options
  * @param {Number} opts.reset - Restart sync from 0
  */
  async syncTransactions (opts = {}) {
    const { _syncManager } = this

    if (opts?.reset) {
      await _syncManager.reset()
    }

    await _syncManager.syncAccount(opts)
  }

  // Pause syncing transactions from electrum
  async pauseSync () {
    return new Promise((resolve) => {
      if (!this._syncManager._isSyncing) return resolve()
      this._syncManager.once('sync-end', () => resolve())
      this._syncManager.stopSync()
    })
  }

  // @desc send transaction
  // @param {Object} opts - options
  // @param {Object} outgoing - transaction details
  // @param {String} outgoing.address - destination address
  // @param {String} outgoing.amount - amount to send
  // @param {String} outgoing.unit - unit of amount
  // @param {String} outgoing.fee - fee to pay in sat/vbyte. example: 10,
  sendTransaction (opts, outgoing) {
    let notify
    const p = new Promise((resolve, reject) => {
      const tx = new Transaction({
        network: this.network,
        provider: this.provider,
        keyManager: this.keyManager,
        getInternalAddress: this._getInternalAddress.bind(this),
        syncManager: this._syncManager
      })

      tx.send(outgoing).then((sent) => {
        if (notify) notify(sent)
        this._syncManager.watchTxMempool(sent.txid)
        this._syncManager.on('tx:mempool:' + sent.txid, () => {
          resolve(sent)
        })
      }).catch((err) => {
        reject(err)
      })
    })

    p.broadcasted = (fn) => {
      notify = fn
    }
    return p
  }

  /**
   * @description add transaction description
   * @param {Object} opts - options
   * @param {String} opts.txid - transaction id
   * @param {String} opts.label - transaction label
   * @returns {Promise}
   **/
  async addTransactionDescription(opts) {
    await this._syncManager.updateTxLabel(opts)
  }

  isValidAddress (opts, address) {
    return this._makeRequest('blockchain.address.get_balance', [address])
  }

  static parsePath (path) {
    return HdWallet.parsePath(path)
  }

  async getFeeEstimate () {
    return this._feeEst.getFeeEstimate()
  }
}

module.exports = WalletPayBitcoin

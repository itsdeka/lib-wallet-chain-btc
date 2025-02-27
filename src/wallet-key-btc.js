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
let bip32
const bitcoin = require('bitcoinjs-lib')
const { BIP32Factory } = require('bip32')

let ecc = require('@bitcoinerlab/secp256k1')

async function loadWASM () {
  ecc = await ecc
  bip32 = BIP32Factory(ecc)
}

class WalletKeyBitcoin {
  constructor (config = {}) {
    this._config = config
  }

  async init () {
    await loadWASM()
    const config = this._config
    if (config.seed) {
      this.seed = config.seed
      this.bip32 = bip32.fromSeed(this.seed.seed, bitcoin.networks.bitcoin)
      this.ready = true
    } else {
      this.ready = false
    }

    if (config.network) {
      this.setNetwork(config.network)
    }
  }

  setNetwork (network) {
    if (network === 'mainnet') network = 'bitcoin'
    this.network = bitcoin.networks[network]
    if (!this.network) throw new Error('invalid network passed')
  }

  close () {
    this.seed = null
    this.bip32 = null
  }

  setSeed (seed) {
    if (this.seed) throw new Error('Seed already set')
    if (!this.network) throw new Error('Network not set')
    if (!seed) throw new Error('Seed is required')
    this.seed = seed
    this.bip32 = bip32.fromSeed(this.seed.seed, this.network)
    this.ready = true
  }

  /**
  * @param {string} path - BIP32 path
  * @param {string} addrType - Address type. example: p2wkh
  * @returns {string} - Address
  * @desc Derives a bitcoin address from a BIP32 path
  */
  addrFromPath (path, addrType) {
    const node = this.bip32.derivePath(path)
    const address = bitcoin.payments[addrType]({ pubkey: node.publicKey, network: this.network }).address
    return {
      address,
      publicKey: node.publicKey.toString('hex'),
      privateKey: node.toWIF(),
      path
    }
  }

  /**
  * @description Generate a script hash from a address
  * @param {string} addr - bitcoin address
  * @returns {string} script hash in hex string
  * */
  addressToScriptHash (addr) {
    const script = bitcoin.address.toOutputScript(addr, this.network)
    const hash = bitcoin.crypto.sha256(script)
    const reversedHash = Buffer.from(hash.reverse())
    return reversedHash.toString('hex')
  }

  /**
  * @description generate a script hash from HD path
  * @param {string} path HD path
  * @param {string} addrtype. address type: p2wpkh
  * @return {Object} Hash as string hex and address object
  **/
  pathToScriptHash (path, addrType) {
    const addr = this.addrFromPath(path, addrType)
    const hash = this.addressToScriptHash(addr.address)
    return { hash, addr }
  }
}

module.exports = WalletKeyBitcoin

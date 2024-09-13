const { EventEmitter } = require('events')
/**
 * Manages watching addresses for new transactions and changes.
 * @extends EventEmitter
 */
class AddressWatch extends EventEmitter {
  /**
   * Creates a new AddressWatch instance.
   * @param {Object} config - The configuration object.
   * @param {Object} config.state - The state management object.
   * @param {Object} config.provider - The provider for blockchain interactions.
   * @param {number} [config.maxScriptWatch=10] - Maximum number of script hashes to watch.
   */
  constructor (config) {
    super()
    this.state = config.state
    this.provider = config.provider
    this.maxScriptWatch = config.maxScriptWatch || 10
  }

  /**
   * Starts watching previously stored script hashes for changes.
   * @fires AddressWatch#new-tx
   * @throws {Error} If there's an issue subscribing to addresses.
   */
  async startWatching () {
    const { state, provider } = this
    const scriptHashes = (await state.getWatchedScriptHashes('in')).concat(
      await state.getWatchedScriptHashes('ext')
    )

    provider.on('new-tx', async (changeHash) => {
      this.emit('new-tx', changeHash)
    })

    await Promise.all(scriptHashes.map(async ([scripthash]) => {
      return provider.subscribeToAddress(scripthash)
    }))
  }

  /**
   * Watches a new address by its script hash.
   * @param {string} scriptHash - The script hash of the address to watch.
   * @param {string} addrType - The type of address ('in' for internal or 'ext' for external).
   * @throws {Error} If there's an issue subscribing to the address.
   */
  async watchAddress (scriptHash, addrType) {
    const { state, maxScriptWatch, provider } = this
    const hashList = await state.getWatchedScriptHashes(addrType)
    if (hashList.length >= maxScriptWatch) {
      hashList.shift()
    }
    const balHash = await provider.subscribeToAddress(scriptHash)
    if (balHash?.message) {
      throw new Error('Failed to subscribe to address ' + balHash.message)
    }
    hashList.push([scriptHash, balHash])
    await state.addWatchedScriptHashes(hashList, addrType)
  }

  /**
   * Retrieves the list of currently watched addresses.
   * @returns {Promise<{inlist: Array, extlist: Array}>} An object containing internal and external watched addresses.
   */
  async getWatchedAddress () {
    const inlist = await this.state.getWatchedScriptHashes('in')
    const extlist = await this.state.getWatchedScriptHashes('ext')
    return { inlist, extlist }
  }
}

module.exports = AddressWatch

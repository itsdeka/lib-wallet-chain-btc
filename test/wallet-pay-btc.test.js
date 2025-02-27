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
const { solo, test } = require('brittle')
const {
  BitcoinPay,
  WalletStore,
  KeyManager,
  BIP39Seed,
  newElectrum,
  activeWallet,
  regtestNode,
  promiseSteps,
  BitcoinCurrency,
  rmDataDir,
  pause
} = require('./test-helpers.js')

test.configure({ timeout: 60000 })

function randomAmount() {
 return +(Math.random()).toFixed(5)
}

test('Create an instances of WalletPayBitcoin', async function (t) {
  const km = new KeyManager({
    seed: await BIP39Seed.generate()
  })
  await km.init()
  const btcPay = new BitcoinPay({
    asset_name: 'btc',
    key_manager: km,
    provider: await newElectrum(),
    store: new WalletStore(),
    network: 'regtest'
  })
  await btcPay.initialize({})

  t.ok(btcPay.ready, 'instance is ready')
  t.comment('destoying instance')
  await btcPay.destroy()
})

test('getNewAddress no duplicate addresses, after recreation', async function (t) {
  const store = new WalletStore()
  const seed = await BIP39Seed.generate()
  const km = new KeyManager({
    seed
  })
  await km.init()
  const btcPay = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: km,
    store,
    network: 'regtest'
  })
  await btcPay.initialize({})

  const addr1 = await btcPay.getNewAddress()
  const addr2 = await btcPay.getNewAddress()
  t.ok(addr1.address !== addr2.address, '2 address should not match')
  t.ok(addr1.path !== addr2.path, '2 addr path should not match')
  const path1 = BitcoinPay.parsePath(addr1.path)
  const path2 = BitcoinPay.parsePath(addr2.path)
  t.ok((path2.index - path1.index) === 1, 'index increased by 1')
  const lastIndex = path2.index

  const btcPay2 = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: km,
    store,
    network: 'regtest'
  })
  await btcPay2.initialize({})

  let addr3 = await btcPay2.getNewAddress()
  addr3 = BitcoinPay.parsePath(addr3.path)
  t.ok(lastIndex + 1 === addr3.index, 'hd path index increased by 1, after recreating instances')

  await btcPay.destroy()
  await btcPay2.destroy()
})

test('getNewAddress - address reuse logic', async (t) => {
  // Generate an new wallet and send some bitcoin to the address
  // generate wallet with same seed, resync and make sure that the address is not reused
  const seed = await BIP39Seed.generate()

  t.comment('create new wallet')
  let km = new KeyManager({
    seed
  })
  await km.init()
  const regtest = await regtestNode()
  const btcPay = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: km,
    store: new WalletStore(),
    network: 'regtest'
  })
  await btcPay.initialize({})

  const lastExt = await btcPay._hdWallet.getLastExtPath()
  t.ok(lastExt === btcPay._hdWallet.INIT_EXTERNAL_PATH, 'first instance last external path is the default path when created')
  const addr = await btcPay.getNewAddress()
  t.comment('sending btc to new address')
  const amount = 0.0001
  await regtest.sendToAddress({ address: addr.address, amount })
  await btcPay._onNewTx()
  t.comment('mining blocks')
  await regtest.mine(2)
  const _pathBalanceChecked = [false, false]
  btcPay.once('synced-path', async (pt, path, hasTx) => {
    t.ok(path === addr.path, 'synced path matches address path')
    t.ok(hasTx, 'address has balance')
    _pathBalanceChecked[0] = true
  })
  await btcPay.syncTransactions()

  t.comment('create second wallet with seed of previous wallet')
  km = new KeyManager({
    seed
  })
  await km.init()
  const btcPay2 = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: km,
    store: new WalletStore(),
    network: 'regtest'
  })
  await btcPay2.initialize({})
  const lastExt2 = await btcPay2._hdWallet.getLastExtPath()
  t.ok(lastExt2 === btcPay2._hdWallet.INIT_EXTERNAL_PATH, 'second instance last path is the default path when created')

  btcPay2.once('synced-path', async (pt, path, hasTx) => {
    t.ok(path === addr.path, 'second instance synced path matches address path')
    t.ok(hasTx, 'second instance address has transactions')
    _pathBalanceChecked[1] = true
  })
  await btcPay2.syncTransactions()

  t.comment('new address is generated')
  const addr2 = await btcPay2.getNewAddress()

  const parsed = BitcoinPay.parsePath(addr.path)
  const parsed2 = BitcoinPay.parsePath(addr2.path)
  // Checking that address is not reused when it's already paid
  t.ok(addr.address !== addr2.address, 'address is not reused')
  t.ok(parsed.index + 1 === parsed2.index, 'index increased by 1 since last instance')
  t.ok(parsed2.change === parsed.change, 'address type is same')
  t.ok(_pathBalanceChecked.indexOf(false) === -1, 'path balance checked for both instances')

  await btcPay.destroy()
  await btcPay2.destroy()
})

test('getTransactions', async (t) => {
  const btcPay = await activeWallet()

  t.comment('syncing transactions')
  await btcPay.syncTransactions({ reset: true })

  let last = 0
  const limit = 5
  const tx0 = await btcPay.getTransactions({ limit })
  t.ok(tx0.length === limit, 'tx length is same as limit')
  tx0.forEach((tx) => {
    if (!last) {
      last = tx.height
      return
    }
    t.ok(last >= tx.height, 'tx height is in descending order height: ' + tx.height)
    last = tx.height
  })
  const tx1 = await btcPay.getTransactions({ limit, reverse: true })
  t.ok(tx1.length === limit, 'tx length is same as limit. reverse order')
  last = 0
  let queryTx
  tx1.forEach((tx) => {
    if (!last) {
      last = tx.height
      return
    }
    t.ok(last <= tx.height, 'tx height is in ascending order height: ' + tx.height)
    last = tx.height
    queryTx = tx
  })
  const tx2 = await btcPay.getTransactions({ limit: 1, offset: 1, reverse: true })
  const tx3 = await btcPay.getTransactions({ limit: 2, offset: 2, reverse: true })
  
  t.alike(tx1[1], tx2[0], 'limit 1, offset 1 works')
  t.alike(tx1.slice(2, 4), tx3, 'limit 2 offset 2 works')

  await btcPay.destroy()
});


test('getTransaction - outgoing, incoming and internal tx', async (t) => {
  const regtest = await regtestNode()
  const btcPay = await activeWallet({ newWallet: true })

  const addr1 = await btcPay.getNewAddress()
  const addrInt = await btcPay.getNewAddress()
  const { result: addr2} = await regtest.getNewAddress()
  const amount = 0.9

  //Incoming Transaction
  await regtest.sendToAddress({ address: addr1.address, amount })
  await regtest.mine(1)
  await pause(1000)
  await btcPay.syncTransactions()
  const incomingTx = (await btcPay.getTransactions({  }))[0]
  t.ok(incomingTx.isIncoming(), 'Incoming transaction direction is correct')
  t.ok(new BitcoinCurrency(incomingTx.amount).eq(new BitcoinCurrency(amount, 'main')), 'Incoming transaction amount is correct')

  ////Outgoing Transaction
  const outgoingAmount = 0.109
  const outgoingTx = await btcPay.sendTransaction({}, { address: addr2, amount: outgoingAmount, unit: 'main', fee: 1 })
  await pause(3000)
  await regtest.mine(1)
  await btcPay.syncTransactions()
  const outgoingTxEntry = (await btcPay.getTransactions({ })).filter((tx) => tx.txid === outgoingTx.txid)[0]
  t.ok(outgoingTxEntry.isOutgoing(), 'Outgoing transaction direction is correct')
  t.ok(new BitcoinCurrency(outgoingTxEntry.amount).eq(new BitcoinCurrency(outgoingAmount, 'main')), 'Outgoing transaction amount is correct')

  //Internal Transaction 
  const internalAmount = 0.103
  const internalTx = await btcPay.sendTransaction({}, { address: addrInt.address, amount: internalAmount, unit: 'main', fee: 2 })
  await regtest.mine(1)
  await btcPay.syncTransactions()
  await pause(3000)
  const internalTxEntry = (await btcPay.getTransactions({ })).filter((tx) => tx.txid === internalTx.txid)[0]

  t.ok(internalTxEntry.isInternal(), 'Internal transaction direction is correct')
  const internalSend = internalTxEntry.to_address_meta.filter((out) => {
    return new BitcoinCurrency(out.amount).eq(new BitcoinCurrency(internalAmount, 'main'))
  })
  t.ok(internalSend.length === 1, 'internal amount is correct')

  const dbtx = await btcPay.getTransactions({ })
  const seen = new Set();

  for (const item of dbtx) {
    if (seen.has(item.txid)) {
      t.fail('duplicate found')
      return true;
    }
    seen.add(item.txid);
  }
  t.ok(true, 'duplicates not found in db')

  await btcPay.destroy()
});

(async () => {
  test('balance transition', async (tst) => {
    const t = tst.test('create address, send btc and check balance')
    const regtest = await regtestNode()
    t.comment('create new wallet')
    const btcPay = await activeWallet({ newWallet: true })

    async function newTx (tx) {
      t.comment('checking balance transition between confirmed/pending/mempool: ', state, send)
      for (const key in send) {
        const addr = key
        const amount = send[key]
        let balance
        try {
          balance = await btcPay.getBalance({}, addr)
        } catch (e) {
          console.log(e)
          continue
        }

        t.ok(tx.direction === 0, 'tx direction is correct')
        t.ok(+tx.amount.toMainUnit() === amount, 'amount is correct')

        const bal = balance[state].toMainUnit()
        t.ok(bal === amount.toString(), `address balance matches sent amount ${state} ${addr} - ${amount} - ${bal}`)
        if (state === 'pending') {
          t.ok(balance.mempool.toMainUnit() === '0', 'mempool balance is 0')
          t.ok(balance.confirmed.toMainUnit() === '0', 'confirmed balance is 0')
        }
        if (state === 'confirmed') {
          t.ok(balance.mempool.toMainUnit() === '0', 'mempool balance is 0')
          t.ok(balance.pending.toMainUnit() === '0', 'pending balance is 0')
        }
        if (state === 'mempool') {
          t.ok(balance.confirmed.toMainUnit() === '0', 'confirmed balance is 0')
          t.ok(balance.pending.toMainUnit() === '0', 'pending balance is 0')
        }
        pass[state].resolve(state)
      }
    }
    const pass = promiseSteps(['mempool', 'pending', 'confirmed'])
    const send = {}
    let state = ''
    btcPay._syncManager.on('new-tx', newTx)
    t.comment('getting new address')
    const addr = await btcPay.getNewAddress()
    const amount = randomAmount()
    send[addr.address] = amount
    t.comment(`Sending  - ${addr.address} - ${amount}`)
    t.comment('wait for mempool tx')
    state = 'mempool'
    await regtest.sendToAddress({ address: addr.address, amount })
    await pass.mempool.promise
    t.comment('mining block for pending tx')
    state = 'pending'
    await regtest.mine(1)
    await pass.pending.promise
    state = 'confirmed'
    t.comment('mining block for confirmed tx')
    await regtest.mine(1)
    await pass.confirmed.promise

    const totalBalance = await btcPay.getBalance({})
    t.ok(+totalBalance.consolidated.toMainUnit() === amount, 'total wallet balance matches')
    await btcPay.destroy()
    t.end()
  })
})()

test('pauseSync - internal and external', async () => {
  async function runTest (sType, opts) {
    const btcPay = await activeWallet()
    const max = opts.max
    test('pauseSync: ' + sType, async (t) => {
      let lastPath = null
      let count = 0
      const first = (pt, path) => {
        if (pt !== sType) return
        if (count > max) throw new Error('count exceeded. did not halt')
        count++
        lastPath = path
        if (count > max) {
          return btcPay.pauseSync()
        }
      }
      const afterPause = async (pt, path) => {
        const last = BitcoinPay.parsePath(lastPath)
        const parsed = BitcoinPay.parsePath(path)
        t.ok(last.purpose === parsed.purpose, sType + ' resume sync: purpose')
        t.ok(last.coin_type === parsed.coin_type, sType + ' resume sync: coin_type')
        t.ok(last.account === parsed.account, sType + ' resume sync: account')
        t.ok(last.change === parsed.change, sType + ' resume sync: change')
        t.ok(last.index - parsed.index === -1, sType + ' resume sync: index increased by 1')
        btcPay.off('synced-path', afterPause)
      }
      btcPay.on('synced-path', first)
      await btcPay.syncTransactions(opts)
      t.ok(count - 1 === max, 'syncing stopped after iteration count:' + max)
      btcPay.off('synced-path', first)
      btcPay.on('synced-path', afterPause)
      await btcPay.syncTransactions()
      await btcPay.destroy()
    })
  }
  await runTest('external', { max: 5 })
  await runTest('internal', { restart: true, max: 10 })
})

test('syncing paths in order', async () => {
  async function runTest (sType, opts) {
    const btcPay = await activeWallet()
    const max = 5
    test('sync in order: ' + sType, async (t) => {
      let count = 0
      const prev = []
      let restartCheck = false
      const handler = async (pt, path) => {
        if (opts.restart && !restartCheck) {
          t.ok(path === btcPay._hdWallet.INIT_EXTERNAL_PATH, 'initial path is correct after restarting')
          restartCheck = true
        }
        if (pt !== sType) return
        count++

        if (prev.length === 0) {
          prev.push(path)
          return
        }
        const last = BitcoinPay.parsePath(prev[prev.length - 1])
        const parsed = BitcoinPay.parsePath(path)
        t.ok(last.purpose === parsed.purpose, sType + ' path order: purpose')
        t.ok(last.coin_type === parsed.coin_type, sType + ' path order: coin_type')
        t.ok(last.account === parsed.account, sType + ' path order: account')
        t.ok(last.change === parsed.change, sType + ' path order: change')
        t.ok(last.index - parsed.index === -1, sType + ' path order: index increased by 1')
        prev.push(path)
        if (count === max) {
          btcPay.off('synced-path', handler)
          await btcPay.destroy()
          t.end()
        }
      }
      btcPay.on('synced-path', handler)
      await btcPay.syncTransactions(opts)
    })
  }
  await runTest('external', {})
  await runTest('internal', { restart: true })
})

test('syncTransaction - catch up missed tx', async (t) => {
  const regtest = await regtestNode()
  t.comment('new  wallet')
   rmDataDir()
  const btcPay = await activeWallet({ newWallet: true, tmpStore: true })
  await btcPay.syncTransactions()
  const seed = btcPay.keyManager.seed.exportSeed({ string: false })
  t.ok(seed.mnemonic, 'seed phrase exported')
  const payAddr = await btcPay.getNewAddress()
  const payAddr2 = await btcPay.getNewAddress()
  const amount = 0.01
  const sendAmt = new BitcoinCurrency(amount, 'main')
  t.comment('generate address and send btc')
  await regtest.sendToAddress({ address: payAddr.address, amount })
  t.comment('waiting for tx to be detected')
  await btcPay._onNewTx()
  const bal1 = await btcPay.getBalance({})
  await btcPay.syncTransactions()
  t.ok(bal1.consolidated.eq(new BitcoinCurrency(amount, 'main')), 'balances match')
  t.comment('destroying instance')

  const txg = await btcPay.getTransactions()
  await btcPay.destroy()

  t.comment('sending btc' + payAddr2.path)
  await regtest.sendToAddress({ address: payAddr2.address, amount })
  await regtest.mine(2)
  t.comment('create new instance with same seed')
  const bp = await activeWallet({ newWallet: false, phrase: seed.mnemonic, tmpStore: true })
  const bpseed = bp.keyManager.seed.exportSeed({ string: false })
  t.ok(bpseed.seed === seed.seed, 'new instance has same seed as prev instance')

  const txh = await bp.getTransactions()
  t.alike(txg,txh, 'funded tx found')
  let bal = await bp.getBalance({}, payAddr.address)
  t.ok(bal.consolidated.eq(sendAmt), 'first tx balance found')
  bal = await bp.getBalance({}, payAddr2.address)
  t.ok(bal.consolidated.toNumber() === 0, 'second tx balance is zero')

  t.comment('sync wallet')

  await bp.syncTransactions()
  await bp.destroy()
})

test('getFundedTokenAddress', async (t) => {

  const regtest = await regtestNode()
  t.comment('create new wallet')
  const btcPay = await activeWallet()

  t.comment('syncing transactions')
  btcPay.on('synced-path', async (pt, path) => {
    const { hash, addr } = btcPay.keyManager.pathToScriptHash(path, 'p2wpkh')
    const eBal = await btcPay.provider._getBalance(hash)
    const bals = await btcPay.getFundedTokenAddresses(addr.address)
    if(eBal.confirmed > 0){
      t.ok(eBal.confirmed === bals[addr.address]?.consolidated.toNumber(), `path balance matches electrum balance ${path}: ${eBal.confirmed}`)
    }
  })
  await btcPay.syncTransactions({ reset: true })
})

test('getFundedTokenAddress. new wallet', async (t) => {

  const regtest = await regtestNode()
  t.comment('create new wallet')
  const btcPay = await activeWallet({ newWallet : true})

  const send = []
  for (let i = 0; i < 5 ; i++) {
    const addr = await btcPay.getNewAddress()
    const amount = randomAmount()
    send.push([addr, amount])
    t.comment(`sending : ${addr.address} - ${amount}`)
    await regtest.sendToAddress({ address: addr.address, amount })
  }
  t.comment('mining')
  await btcPay._onNewTx()
  await regtest.mine(1)
  t.comment('syncing transactions')
  await btcPay.syncTransactions({ reset: true })
  const bals = await btcPay.getFundedTokenAddresses()
  send.forEach((tx) => {
    const addr = tx[0].address
    const amt  = tx[1]
    const bal = bals[addr]
    t.ok(+bal.pending.toMainUnit() === amt, `balance ok ${addr}`)
  })

})

test('syncTransaction - balance check', async (t) => {
  const regtest = await regtestNode()
  t.comment('create new wallet')
  const btcPay = await activeWallet({ newWallet: true })
  // Send some bitcoin to the address and check if the amounts match as its getting sycned
  const payAddr = await btcPay.getNewAddress()
  const amount = 0.0888
  t.comment('generate address and send btc ' + payAddr.path)
  await regtest.sendToAddress({ address: payAddr.address, amount })
  t.comment('mining blocks')
  await regtest.mine(2)
  let checked = false
  async function checkBal (pt, path, hasTx, gapCount) {
    if (checked) return
    checked = true
    t.ok(path === payAddr.path, 'first path is checked')
    const { hash, addr } = btcPay.keyManager.pathToScriptHash(path, 'p2wpkh')
    const eBal = await btcPay.provider._getBalance(hash)
    let bal
    try {
      bal = await btcPay.getBalance({}, addr.address)
    } catch (e) {
      return
    }
    const balTotal = bal.confirmed.add(bal.pending).toBaseUnit()
    t.ok(eBal.confirmed.toString() === balTotal, `addr: ${addr.address} confirmed matches electrum ${eBal.confirmed} - ${balTotal}`)
    t.ok(eBal.unconfirmed.toString() === bal.mempool.toBaseUnit(), `addr: ${addr.address} mempool matches electrum`)
    t.ok(new BitcoinCurrency(eBal.confirmed, 'base').eq(new BitcoinCurrency(amount, 'main')), 'amount matches sent amount')
    btcPay.off('synced-path', checkBal)
    await btcPay.pauseSync()
    await btcPay.destroy()
    t.end()
  }
  btcPay.on('synced-path', checkBal)
  t.comment('syncing transactions..')
  await btcPay.syncTransactions({ restart: true })
})

test('import seed with balance', async (t) => {
  const regtest = await regtestNode()
  t.comment('create new wallet')
  const btcPay = await activeWallet({ newWallet: true, tmpStore: true })
  const seed = btcPay.keyManager.seed.exportSeed({ string: false })
  const addr = await btcPay.getNewAddress()
  const amount = randomAmount()
  t.comment('generate address and send btc')
  await regtest.sendToAddress({ address: addr.address, amount })
  await regtest.mine(3)
  await btcPay.syncTransactions()
  const txList = await btcPay.getTransactions()
  t.ok(txList.length === 1, '1 tx found')
  await btcPay.destroy()

  t.comment('import seed')
  const btcPay2 = await activeWallet({ newWallet: false, phrase: seed.mnemonic, tmpStore: true })
  await btcPay2.syncTransactions()
  const bal = await btcPay2.getBalance({}, addr.address)
  const txList2 = await btcPay2.getTransactions()
  t.alike(txList2, txList, 'tx history matches')

  t.ok(bal.consolidated.eq(new BitcoinCurrency(amount, 'main')), 'balances match')
  await btcPay2.destroy()
})

test('bip84 test vectors', async function (t) {
  // LINK: https://github.com/bitcoin/bips/blob/master/bip-0084.mediawiki
  const mnemonic = 'abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about'
  const km = new KeyManager({
    seed: await BIP39Seed.generate(mnemonic)
  })
  await km.init()
  const btcPay = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: km,
    store: new WalletStore(),
    network: 'bitcoin'
  })
  await btcPay.initialize({})

  const addr1 = await btcPay.getNewAddress()
  const addr2 = await btcPay.getNewAddress()
  const changeAddr = await btcPay._getInternalAddress()
  const changeAddr2 = await btcPay._getInternalAddress()

  t.ok(addr1.address === 'bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu', 'first receive address')
  t.ok(addr1.path === "m/84'/0'/0'/0/0", 'first receive path')
  t.ok(addr1.privateKey === 'KyZpNDKnfs94vbrwhJneDi77V6jF64PWPF8x5cdJb8ifgg2DUc9d', 'first receive private key')
  t.ok(addr1.publicKey === '0330d54fd0dd420a6e5f8d3624f5f3482cae350f79d5f0753bf5beef9c2d91af3c', 'first receive public key')

  t.ok(addr2.address === 'bc1qnjg0jd8228aq7egyzacy8cys3knf9xvrerkf9g', 'second recieve address')
  t.ok(addr2.path === "m/84'/0'/0'/0/1", 'second receive path')
  t.ok(addr2.privateKey === 'Kxpf5b8p3qX56DKEe5NqWbNUP9MnqoRFzZwHRtsFqhzuvUJsYZCy', 'second receive privateKey')

  t.ok(changeAddr.address === 'bc1q8c6fshw2dlwun7ekn9qwf37cu2rn755upcp6el', 'First change address')
  t.ok(changeAddr.path === "m/84'/0'/0'/1/0", 'first change path')

  t.ok(changeAddr2.path === "m/84'/0'/0'/1/1", 'second change path')
  t.ok(changeAddr2.address === 'bc1qggnasd834t54yulsep6fta8lpjekv4zj6gv5rf', 'second change address')

  const km2 = new KeyManager({
    seed: await BIP39Seed.generate()
  })
  await km2.init()

  const bp = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: km2,
    store: new WalletStore(),
    network: 'bitcoin'
  })
  await bp.initialize({})
  const bpAddr = await bp.getNewAddress()
  for (const key in addr1) {
    const bVal = bpAddr[key]
    const aVal = addr1[key]
    if (key === 'path') {
      t.ok(bVal === aVal, `generate same ${key} with different mnemonic`)
      continue
    }
    t.ok(bVal !== aVal, `generate different ${key} with different mnemonic`)
  }
  await btcPay.destroy()
  await bp.destroy()
})

const { solo, test } = require('brittle')
const {
  BitcoinPay,
  WalletStore,
  KeyManager,
  BIP39Seed,
  newElectrum,
  HdWallet,
  activeWallet,
  regtestNode,
  pause,
  BitcoinCurrency
} = require('./test-helpers.js')

test.configure({ timeout: 600000 })


test('Create an instances of WalletPayBitcoin', async function (t) {
  const btcPay = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: new KeyManager({
      seed: await BIP39Seed.generate()
    }),
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
  const btcPay = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: new KeyManager({
      seed
    }),
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
    key_manager: new KeyManager({
      seed
    }),
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

  const regtest = await regtestNode()
  const seed = await BIP39Seed.generate()
  const btcPay = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: new KeyManager({
      seed
    }),
    store: new WalletStore(),
    network: 'regtest'
  })
  await btcPay.initialize({})
  const lastExt = await btcPay._hdWallet.getLastExtPath()
  t.ok(lastExt === HdWallet.INIT_EXTERNAL_PATH, 'first instance last external path is the default path when created')
  const addr = await btcPay.getNewAddress()
  const amount = 0.0001
  await regtest.sendToAddress({ address: addr.address, amount })
  await btcPay._onNewTx()
  await regtest.mine(2)

  let _pathBalanceChecked = false
  btcPay.once('synced-path', async (pt, path, hasTx) => {
    t.ok(path === addr.path, 'synced path matches address path')
    t.ok(hasTx, 'address has balance')
    _pathBalanceChecked = true
  })

  await btcPay.syncTransactions()

  if (!_pathBalanceChecked) t.fail('path balance not checked')

  const btcPay2 = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: new KeyManager({
      seed
    }),
    store: new WalletStore(),
    network: 'regtest'
  })
  await btcPay2.initialize({})
  const lastExt2 = await btcPay2._hdWallet.getLastExtPath()
  t.ok(lastExt2 === HdWallet.INIT_EXTERNAL_PATH, 'second instance last path is the default path when created')
  _pathBalanceChecked = true
  btcPay2.once('synced-path', async (pt, path, hasTx) => {
    t.ok(path === addr.path, 'second instance synced path matches address path')
    t.ok(hasTx, 'second instance address has transactions')
    _pathBalanceChecked = true
  })
  await btcPay2.syncTransactions()
  const addr2 = await btcPay2.getNewAddress()

  const parsed = BitcoinPay.parsePath(addr.path)
  const parsed2 = BitcoinPay.parsePath(addr2.path)
  // Checking that address is not reused when it's already paid
  t.ok(addr.address !== addr2.address, 'address is not reused')
  t.ok(parsed.index + 1 === parsed2.index, 'index increased by 1')
  t.ok(parsed2.change === parsed.change, 'address type is same')

  await btcPay.destroy()
  await btcPay2.destroy()
})

test('getTransactions', async (t) => {
  const btcPay = await activeWallet()

  t.comment('syncing transactions')
  await btcPay.syncTransactions({ restart: true })

  let last = 0
  const max = 5
  let c = 0
  await btcPay.getTransactions(async(tx) => {
    const h = tx[0].height
    if (!last) {
      last = h
      return
    }
    t.ok(last < h, 'tx height is in descending order height: ' + h)
    last = h
    c++
    if(c === max) {
      await btcPay.destroy()
      t.end()
    }
  })
  if(c !== max) t.fail('tx not received')
});

(async () => {
  solo('balance check', async (tst) => {
    const t = tst.test('create address, send btc and check balance')
    const regtest = await regtestNode()
    t.comment('create new wallet')
    const btcPay = await activeWallet({ newWallet: true })
    //const max = btcPay._syncManager._max_script_watch
    const max = 2
    let res

    async function newTx () {
      t.comment('checking balance', state, send)
      for( let key in send) {
        let addr = key
        const amount = send[key]
        let balance 
        try {
          balance = await btcPay.getBalance({}, addr)
        } catch(e) {
          console.log(e)
          continue 
        }
        const bal = balance[state].toMainUnit()
        t.ok(bal === amount.toString(), `address balance matches sent amount ${state} ${addr} - ${amount} - ${bal}`)
        pass[state].resolve(state)
      }
    }
    let pass = {}
    for(let state of ['mempool', 'pending', 'confirmed']) {
      pass[state] = {}
      pass[state].promise = new Promise((resolve, reject) => {
        pass[state].resolve = resolve
        pass[state].reject = reject
      })
    }
    const send = {}
    let state = ''
    btcPay._syncManager.on('new-tx', newTx)
    t.comment('getting new address')
    const addr = await btcPay.getNewAddress()
    const amount = +(Math.random() * 0.01).toFixed(5)
    send[addr.address] = amount
    t.comment(`Sending  - ${addr.address} - ${amount}`)
    t.comment('wait for mempool tx')
    state = 'mempool'
    await regtest.sendToAddress({ address: addr.address, amount })
    const z = await pass.mempool.promise
    t.comment('mining block for pending tx')
    state = 'pending'
    await regtest.mine(1)
    await pass.pending.promise
    state = 'confirmed'
    t.comment('mining block for confirmed tx')
    await regtest.mine(1)
    await pass.confirmed.promise
    await btcPay.destroy()
    t.end()
  })
})();

test('pauseSync - internal and external', async (t) => {
  async function runTest(sType, opts){
    const btcPay = await activeWallet()
    let max = 5
    test('pauseSync: '+sType, async (t) => {
      let lastPath = null 
      let count = 0 
      const first = (pt, path, hastx, gap) => {
        if(pt !== sType) return
        if(count === max) t.fail('count exceeded. did not halt')
        count++
        lastPath = { path, gap }
        if(count === max) {
          return btcPay.pauseSync()
        }
      }
      const afterPause = async (pt, path, hasTx, gapCount) => {
        const last = BitcoinPay.parsePath(lastPath.path)
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
      t.ok(count === max, 'syncing stopped after iteration count:'+ max)
      btcPay.off('synced-path', first)
      btcPay.on('synced-path', afterPause)
      await btcPay.syncTransactions()
      await btcPay.destroy()
    })
  }
  await runTest('external')
  await runTest('internal', { reset: true })
})

// Sycning paths must be in order
test('syncing paths in order', async (t) => {
  async function runTest(sType, opts){
    const btcPay = await activeWallet()
    let max = 5
    test('sync in order: '+sType, async (t) => {
      let lastPath = null 
      let count = 0 
      const prev = []
      const handler = async (pt, path, hasTx, gapCount) => {
        if(pt !== sType) return
        count++
        if(prev.length === 0 ) {
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
        if(count === max) {
          btcPay.off('synced-path', handler)
          await btcPay.destroy()
          t.end()
        }
      }
      btcPay.on('synced-path', handler)
      await btcPay.syncTransactions(opts)
    })
  }
  await runTest('external')
  await runTest('internal', { restart: true })
})

test('syncTransaction - balance check', async (t) => {
  const regtest = await regtestNode()
  t.comment('create new wallet')
  const btcPay = await activeWallet({ newWallet: true })
  // Send some bitcoin to the address and check if the amounts match as its getting sycned
  const payAddr = await btcPay.getNewAddress()
  const amount = 0.0888
  t.comment('generate address and send btc')
  await regtest.sendToAddress({ address: payAddr.address, amount })
  t.comment('mining blocks')
  await regtest.mine(2)
  t.comment('waiting for electrum to update')
  await btcPay._onNewTx()
  async function checkBal (pt, path, hasTx, gapCount) {
    const [sc, addr] = btcPay.keyManager.pathToScriptHash(path, 'p2wpkh')
    const eBal = await btcPay.provider._getBalance(sc)
    let bal
    try {
      bal = await btcPay.getBalance({}, addr.address)
    } catch(e) {
      return
    }
    const balTotal = bal.confirmed.add(bal.pending).toBaseUnit()
    t.ok(eBal.confirmed.toString() === balTotal, `addr: ${addr.address} confirmed matches electrum ${eBal.confirmed} - ${balTotal}`)
    t.ok(eBal.unconfirmed.toString() === bal.mempool.toBaseUnit(), `addr: ${addr.address} mempool matches electrum`)
    t.ok(new BitcoinCurrency(eBal.confirmed, 'base').eq(new BitcoinCurrency(amount, 'main')), 'amount matches sent amount')
    btcPay.off('synced-path', checkBal)
    await btcPay.destroy()
    t.end()
  }
  btcPay.on('synced-path', checkBal)
  t.comment('syncing transactions..')
  await btcPay.syncTransactions({ restart: true })
})

test('bip84 test vectors', async function (t) {
  // LINK: https://github.com/bitcoin/bips/blob/master/bip-0084.mediawiki
  const mnemonic = 'abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about'
  const btcPay = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: new KeyManager({
      seed: await BIP39Seed.generate(mnemonic)
    }),
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
  t.ok(addr1.WIF === 'KyZpNDKnfs94vbrwhJneDi77V6jF64PWPF8x5cdJb8ifgg2DUc9d', 'first receive WIF')
  t.ok(addr1.publicKey === '0330d54fd0dd420a6e5f8d3624f5f3482cae350f79d5f0753bf5beef9c2d91af3c', 'first receive public key')

  t.ok(addr2.address === 'bc1qnjg0jd8228aq7egyzacy8cys3knf9xvrerkf9g', 'second recieve address')
  t.ok(addr2.path === "m/84'/0'/0'/0/1", 'second receive path')
  t.ok(addr2.WIF === 'Kxpf5b8p3qX56DKEe5NqWbNUP9MnqoRFzZwHRtsFqhzuvUJsYZCy', 'second receive WIF')

  t.ok(changeAddr.address === 'bc1q8c6fshw2dlwun7ekn9qwf37cu2rn755upcp6el', 'First change address')
  t.ok(changeAddr.path === "m/84'/0'/0'/1/0", 'first change path')

  t.ok(changeAddr2.path === "m/84'/0'/0'/1/1", 'second change path')
  t.ok(changeAddr2.address === 'bc1qggnasd834t54yulsep6fta8lpjekv4zj6gv5rf', 'second change address')

  const bp = new BitcoinPay({
    asset_name: 'btc',
    provider: await newElectrum(),
    key_manager: new KeyManager({
      seed: await BIP39Seed.generate()
    }),
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

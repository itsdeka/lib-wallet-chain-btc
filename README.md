# lib-wallet-pay-btc

Bitcoin payment method for the wallet library. Powered by Electrum.

## Features:
- Support for P2WPKH/BIP84 HD path traversal.
- internal UTXO managment, not reliant on electrum.
- internal balance calculation. not reliant on electrum.
- Transaction broadcasting
- Modular design. drop in Seed/storage/block source components


## Usage

```javascript
// Start with a storage engine
// 
const storeEngine = new WalletStoreMemory()
await storeEngine.init()

// Generate a seed or use a mnemonic phrase
const seed = await BIP39Seed.generate(/** Can enter mnemonic phrase here to **/)

// Connect to an electrum server.
// This class needs a storage engine for cacheing.
// host and port are the electrum server details.
// Additional options can be passed to the Electrum class with regards to caching.
const provider = await Electrum({ store: storeEngine, host, port })
await provider.connect()

// Start new Bitcoin wallet
const btcPay = new BitcoinPay({
  // Asset name is a unique key for the assets
  // allow multiple assets of same type per wallet
  asset_name: 'btc',
  // Electrum provider.
  provider,
  // Key manager: Handlles address generation library from seed.
  key_manager: new KeyManager({
    seed
  }),
  // Wallet store: Storage engine for the wallet
  store: storeEngine
  // Network: network type, regtest, testnet, mainnet
  network: 'regtest',
  // Min confs: Minimum number of confirmations to consider a transaction confirmed
  min_block_confirmations: 1,
  // Gap limit: Number of addresses to look ahead for transactions.
  gap_limit: 20,
})
// Start wallet.
await btcPay.initialize({})

// Listen to each path that has transactions.
// This wallet follow BIP84 standard for address generation and 
// the gap limit by default is 20.
btcPay.on('sync-path', (pathType, path, hasTx, progress) => {
  console.log('Syncing path', pathType, path, hasTx, progress)
})

// Parse blockchain for transactions to your wallet.
// This needs to be run when recreating a wallet. 
// This can take long depending on the number of addresses a wallet has created.
const pay = btcPay.syncTransactions({ 
  reset : false // Passing true will resync from scratch 
})

pay.broadcasted((tx)=>{
  // transaction is broadcasted but not updated insternal state
})

const tx = await pay
// transaction has been broadcasted and internal state is updated 

// Pause the sync process. 
// If the application needs to sleep and come back to resume syncing.
await btcPay.pauseSync()


// Get a new address. This will add the address to watch list for incoming payments. You should limit address generation to prevent spam.
// This will return address, HD PATH, pubkey and WIF private key of the address. 
const { address } = await btcPay.getNewAddress()

// Get balance of an address
// Balance is returned in format of:
// Confirmed: Confirmed balance. This is transactions that have more than min number of confirmations 
// Pending: Pending balance. Transactions that have less than min number of confirmations
// Mempool: Mempool balance. Transactions that are in the mempool and have no confirmations.
// If you pass an address, it will return balance of that address in your wallet
// If you don't pass an address, it will return total balance of all addresses in your wallet.
const addrBalance = await btcPay.getBalance({}, address)

// Get total balance accress all addresses
const walletBalance = await btcPay.getBalance({})

// Send bitcoin to an address
// Result will contain:
// - txid: Transaction ID
// - feeRate: Fee rate in sat/vByte
// - fee: Fee in satoshis
// - vSize: Virtual size of the transaction
// - hex: Raw transaction hex
// - utxo: UTXO used in the transaction
// - vout: Vout bytes of the transaction
// - changeAddress: Change address of the transaction. which contains, address, WIF, path, pub key.
const result = await btcPay.sendTransaction({}, {
  to: 'bcr111...', // bitcoin address of the recipient
  
  // Amounts of bitcoin to send 
  amount: 0.0001, // Value of amount 
  unit: 'main', // unit of amount: main = Bitcoin and base = satoshi unit

  fee: 10, // Fees in sats per vbyte. 10 = 10 sat/vByte
}))

// Get a transaction by txid
const tx = await btcPay.getTransaction(result.txid)

// Get a list of transactions
const txs = await btcPay.getTransactions(query)

// is address a valid bitcoin address
const isvalid = await btcPay.isValidAddress('bcrt1qxeyapzy3ylv67qnxjtwx8npd8ypjkuy8xstu0m')

// Destroy instance of the wallet. This stops all wallet activity.
// You need to recreate btcPay instance to use the wallet again.
await btcPay.destroy()


```

## Methods


### Methods

The following methods are available on this module:

#### `getNewAddress()`

* **Description**: Generates a new Bitcoin address.
* **Return Value**: A Promise that resolves to the newly generated address.
* **Parameters**:
        + `opts` (optional): An object containing configuration options for the method. Currently, no specific properties are required.

Example usage:
```javascript
const wallet = new WalletPayBitcoin();
const newAddress = await wallet.getNewAddress();
console.log(newAddress); // Output: a newly generated Bitcoin address
```

#### `getBalance(opts, addr)`

* **Description**: Retrieves the balance of an address or the entire wallet.
* **Return Value**: A Promise that resolves to the balance in BTC (or a rejection with an error message).
* **Parameters**:
        + `opts` (optional): An object containing configuration options for the method. Currently, no specific properties are required.
        + `addr`: The address or a filter object to retrieve balances for multiple addresses.

Example usage:
```javascript
const wallet = new WalletPayBitcoin();
const balance = await wallet.getBalance();
console.log(balance); // Output: the balance of the entire wallet

const balanceForAddress = await wallet.getBalance({ address: '<addr>' });
console.log(balanceForAddress); // Output: the balance for a specific address
```

#### `syncTransactions(opts)`

* **Description**: Syncs transactions with Electrum.
* **Return Value**: A Promise that resolves when syncing is complete (or a rejection with an error message).
* **Parameters**:
        + `opts` (optional): An object containing configuration options for the method. Currently, no specific properties are required.

Example usage:
```javascript
const wallet = new WalletPayBitcoin();
await wallet.syncTransactions();
console.log('Syncing complete!'); // Output: confirmation message when syncing is done
```

#### `pauseSync()`

* **Description**: Pauses syncing transactions from Electrum.
* **Return Value**: A Promise that resolves immediately (or a rejection with an error message).

Example usage:
```javascript
const wallet = new WalletPayBitcoin();
wallet.pauseSync();
console.log('Syncing paused!'); // Output: confirmation message when syncing is paused
```

#### `sendTransaction(opts, outgoing)`

* **Description**: Sends a transaction to a specified address.
* **Return Value**: A Promise that resolves when the transaction is sent (or a rejection with an error message).
* **Parameters**:
        + `outgoing`: An object containing configuration options for the method. Required properties include:
                - `address`
                - `amount`
                - `unit` `main` for btc and `base` for sats 
                - `fee` in sats per byte: 
        + `opts`: 

Example usage:
```javascript
const wallet = new WalletPayBitcoin();
const txOpts = {
  address: '',
  unit: 'sats',
  amount: 10000,
  fee: 10  
};
const tx = await wallet.sendTransaction(txOpts, true);
console.log('Transaction sent!'); // Output: confirmation message when the transaction is sent
```

## Testing

- There is extensive integration tests for this package. 
- We use Brittle for testing. Checkout package.json for various test commands.
- Integration tests need a electrum server connected to a regtest bitcoin node.
- To setup testing enviroment see: [Test tools repo](https://github.com/tetherto/wallet-lib-test-tools/blob/main/src/bitcoin/README.md)

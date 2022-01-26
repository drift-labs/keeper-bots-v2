### Set up .env
```shell
cp .env.example .env
```

Update values in `.env` accordingly

Make sure the private key you choose for FILLER_PRIVATE_KEY as a user account initialized with drift clearing house

### Build Node Server
```shell
yarn
tsc
```

### Run Node Serve
```shell
node src/index.js
```
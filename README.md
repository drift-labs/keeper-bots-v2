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

### Initialize User
```shell
yarn
node lib/initializeUser.js
```

### Run Node Serve
```shell
node lib/index.js
```
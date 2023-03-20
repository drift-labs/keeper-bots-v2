<div align="center">
  <img height="120x" src="https://uploads-ssl.webflow.com/611580035ad59b20437eb024/616f97a42f5637c4517d0193_Logo%20(1)%20(1).png" />

  <h1 style="margin-top:20px;">Keeper Bots for Drift Protocol v2</h1>

  <p>
    <a href="https://docs.drift.trade/tutorial-keeper-bots"><img alt="Docs" src="https://img.shields.io/badge/docs-tutorials-blueviolet" /></a>
    <a href="https://discord.com/channels/849494028176588802/878700556904980500"><img alt="Discord Chat" src="https://img.shields.io/discord/889577356681945098?color=blueviolet" /></a>
    <a href="https://opensource.org/licenses/Apache-2.0"><img alt="License" src="https://img.shields.io/github/license/project-serum/anchor?color=blueviolet" /></a>
  </p>
</div>

# Setting up


This repo has two main branches:

* `master`: bleeding edge, may be unstable, currently running on the `devnet` cluster
* `mainnet-beta`: stable, currently running on the `mainnet-beta` cluster

## Setup Environment

### yaml Config file:

A `.yaml` file can be used to configure the bot setup now. See `config.example.yaml` for a commented example.

Then you can run the bot by loading the config file:
```shell
yarn run dev --config-file=example.config.yaml
```

Here is a table defining the various fields and their usage/defaults:

| Field             | Type   | Description | Default |
| ----------------- | ------ | --- | --- |
| global            | object | global configs to apply to all running bots | - |
| global.endpoint   | string | RPC endpoint to use | - |
| global.wsEndpoint | string | (optional) Websocket endpoint to use | derived from `global.endpoint` |
| global.keeperPrivateKey  | string | (optional) The private key to use to pay/sign transactions | `KEEPER_PRIVATE_KEY` environment variable |
| global.initUser   | bool   | Set `true` to init a fresh userAccount | `false` |
| global.websocket  | bool   | Set `true` to run the selected bots in websocket mode if compatible| `false` |
| global.runOnce    | bool   | Set `true` to run only one iteration of the selected bots | `false` |
| global.debug      | bool   | Set `true` to enable debug logging | `false` |
| global.subaccounts | list  | (optional) Which subaccount IDs to load | `0` |
| enabledBots       | list   | list of bots to enable, matching configs must be present under `botConfigs` | - |
| botConfigs        | object | configs for associated bots | - |
| botConfigs.<bot_type> | object | config for a specific <bot_type> | - |


### Install dependencies

Run from repo root to install npm dependencies:
```shell
yarn
```


## Initialize User

A `ClearingHouseUser` must be created before interacting with the `ClearingHouse` program.

```shell
yarn run dev --init-user
```

Alternatively, you can put the private key into a browser wallet and use the UI at https://app.drift.trade to initialize the user.

## Depositing Collateral

Some bots (i.e. trading, liquidator and JIT makers) require collateral in order to keep positions open, a helper function is included to help with depositing collateral.
A user must be initialized first before collateral may be deposited.

```shell
# deposit 10,000 USDC
yarn run dev --force-deposit 10000
```

Alternatively, you can put the private key into a browser wallet and use the UI at https://app.drift.trade to deposit collateral.

# Run Bots

After creating your `config.yaml` file as above, run with:
  
```shell
yarn run dev --config-file=config.yaml
```

By default, some [Prometheus](https://prometheus.io/) metrics are exposed on `localhost:9464/metrics`.

# Notes on some bots

## JIT Maker Bot

Read the docs: https://docs.drift.trade/just-in-time-jit-auctions

⚠ requires collateral

This is mainly to show how to participate in JIT auctions (you dont want to run this as is).



## Liquidator Bot

Read the docs: https://docs.drift.trade/liquidators

By default the liquidator will attempt to liqudate (inherit the risk of)
endangered positions in all markets. Set `botConfigs.liquidator.perpMarketIndicies` and/or `botConfigs.liquidator.spotMarketIndicies`
in the config file to restrict which markets you want to liquidate.

FROM node:20.18.1 AS builder
RUN npm install -g husky

COPY package.json yarn.lock ./

WORKDIR /app

COPY . .
RUN yarn install
RUN node esbuild.config.js

FROM node:20.18.1-alpine
COPY --from=builder /app/lib/ ./lib/

EXPOSE 9464

CMD ["node", "./lib/index.js"]

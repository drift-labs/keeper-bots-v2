FROM public.ecr.aws/bitnami/node:18
RUN yum install -y git
ENV NODE_ENV=production
RUN npm install -g yarn
RUN npm install -g typescript
RUN npm install -g ts-node

WORKDIR /app

COPY . .
RUN yarn install
RUN yarn build
RUN yarn install --production

EXPOSE 9464

CMD [ "yarn", "start:all" ]
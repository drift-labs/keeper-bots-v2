FROM public.ecr.aws/bitnami/node:16
RUN apt-get install git
ENV NODE_ENV=production
RUN npm install -g yarn
RUN npm install -g typescript

WORKDIR /app
COPY . .
RUN yarn
RUN yarn build

EXPOSE 9464

CMD [ "yarn", "start:all" ]
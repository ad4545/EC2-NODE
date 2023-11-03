const express = require("express");
const app = express();
const cors = require("cors");
const dotenv = require("dotenv");
const { Kafka } = require("kafkajs");
const { createServer } = require('node:http');
const server = createServer(app)


dotenv.config();
app.use(express.json());
app.use(
  cors({
    origin: "*",
  })
);

const kafka = new Kafka({
  clientId: "node-producer",
  brokers: [
    "b-2.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9096","b-1.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9096"
  ],
  sasl: {
    mechanism: "scram-sha-512",
    username: "KAFKA_CONNECT",
    password: "client01",
  },
  ssl: true,
});

const taskProducer = kafka.producer({
  allowAutoTopicCreation: false,
  transactionTimeout: 30000,
});

const batteryConsumer = kafka.consumer({ groupId: "group-2" });

app.get("/", (req, res) => {
  res.send("Server is running");
});


const io = require("socket.io")(server, {
  cors: {
    origin: "*",
  },
});

let socket = io.on("connection", (socket) => {
  socket.on('task',async(msg)=>{
      let finalMsg = JSON.stringify(msg.task)
      await taskProducer.send({
        topic:`task_${msg.robot}`,
        messages:[{key:'',value:finalMsg}],
        ttl:5000
      })
      console.log('task published')
  })
   return socket
});

server.listen(9000, async() => {
  console.log("connected")
  await taskProducer.connect();
  await batteryConsumer.connect()
  await batteryConsumer.subscribe({topics:['battery']})
  await batteryConsumer.run({
    eachMessage:async({topic,partition,message})=>{
      let msg = message.value.toString()
      socket.emit('robot-battery',msg)
      console.log(msg,'battery')
    }
  })
}); 
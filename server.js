const express = require("express");
const app = express();
const cors = require("cors");
const dotenv = require("dotenv");
const { Kafka } = require("kafkajs");

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
    "b-1-public.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9196",
    "b-2-public.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9196",
  ],
  sasl: {
    mechanism: "scram-sha-512",
    username: "KAFKA_CONNECT",
    password: "client01",
  },
  ssl: true,
});

var max_battery = 0;
var winner_robot = "";

const batteryConsumer = kafka.consumer({ groupId: "group-1" });
const taskProducer = kafka.producer({
  allowAutoTopicCreation: false,
  transactionTimeout: 30000,
});

async function checkBattery() {
  await batteryConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const msg = JSON.parse(message.value.toString());
      const data = msg["data"];
      const battery = data.battery;
      if (battery > max_battery) {
        max_battery = battery;
        winner_robot = data.robot_name;
      }
    },
  });
}

app.get("/", (req, res) => {
  res.send("Server is running");
});

const server = app.listen(8000, () => [console.log("connected")]);

const io = require("socket.io")(server, {
  cors: {
    origin: "*",
  },
});

io.on("connection", (socket) => {
  socket.on("setup", async (mssg) => {
    console.log(mssg.id);
    await batteryConsumer.connect();
    await taskProducer.connect();
    await batteryConsumer.subscribe({ topics: ["battery"] });
    checkBattery();
  });
  socket.on("task", async (task) => {
    console.log(winner_robot);
    task.robot_name = winner_robot;
    const task_to = JSON.stringify(task);
    await taskProducer.send({
      topic: "task",
      messages: [{ key: "", value: task_to }],
    });
    console.log("process done");
  });
});

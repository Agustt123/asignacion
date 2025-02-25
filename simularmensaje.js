

const amqp = require('amqplib');

async function sendMessage() {
    try {
        const connection = await amqp.connect('amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672'); // Cambia esto si RabbitMQ está en otro servidor
        const channel = await connection.createChannel();
        const queue = 'asignacion';

        const message = {
            empresa: "270",
            feacture:"asignacion",
            channel:"a234",
            cadete: 50,
            quien: 49,
            dataQR: {
                local: 1,
                did: "68",
                cliente: "1",
                empresa: 270
            }
        };

        await channel.assertQueue(queue, { durable: true });
        channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), { persistent: true });

        console.log("[x] Mensaje enviado a la cola:", message);

        setTimeout(() => {
            connection.close();
        }, 500);
    } catch (error) {
        console.error("Error enviando mensaje a RabbitMQ:", error);
    }
}

sendMessage();

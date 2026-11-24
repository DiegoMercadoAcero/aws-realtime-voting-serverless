import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const sqsClient = new SQSClient({});
const QUEUE_URL = process.env.VOTES_QUEUE_URL;

export const handler = async (event) => {
    try {
        // El mensaje viene como string JSON desde el WebSocket
        // Ejemplo esperado: {"action": "vote", "option": "OpcionA"}
        const body = JSON.parse(event.body);
        
        if (!body.option) {
            return { statusCode: 400, body: "Falta la opción de voto" };
        }

        // Enviar el voto a la cola SQS
        const command = new SendMessageCommand({
            QueueUrl: QUEUE_URL,
            MessageBody: JSON.stringify({
                option: body.option,
                timestamp: new Date().toISOString()
            })
        });

        await sqsClient.send(command);
        console.log(`Voto encolado para: ${body.option}`);

        return { statusCode: 200, body: "Voto recibido y encolado" };
    } catch (error) {
        console.error("Error al encolar voto:", error);
        return { statusCode: 500, body: "Error al procesar solicitud" };
    }
};

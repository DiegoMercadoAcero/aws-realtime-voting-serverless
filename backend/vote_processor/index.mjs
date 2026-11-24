import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, ScanCommand, DeleteCommand } from "@aws-sdk/lib-dynamodb";
import { ApiGatewayManagementApiClient, PostToConnectionCommand } from "@aws-sdk/client-apigatewaymanagementapi";

const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

const VOTES_TABLE = process.env.VOTES_TABLE;
const CONNECTIONS_TABLE = process.env.CONNECTIONS_TABLE;
const WEBSOCKET_ENDPOINT = process.env.WEBSOCKET_ENDPOINT; // Formato: https://xxxx.execute-api.us-east-1.amazonaws.com/prod

const apiGwClient = new ApiGatewayManagementApiClient({ endpoint: WEBSOCKET_ENDPOINT });

export const handler = async (event) => {
    try {
        // 1. Procesar cada voto que viene en el evento de SQS
        for (const record of event.Records) {
            const body = JSON.parse(record.body);
            const option = body.option;

            // Incremento atómico en DynamoDB (evita condiciones de carrera)
            await docClient.send(new UpdateCommand({
                TableName: VOTES_TABLE,
                Key: { optionId: option },
                UpdateExpression: "ADD votes :inc",
                ExpressionAttributeValues: { ":inc": 1 }
            }));
            console.log(`Voto contabilizado para: ${option}`);
        }

        // 2. Obtener los resultados totales actualizados
        const votesData = await docClient.send(new ScanCommand({ TableName: VOTES_TABLE }));
        const results = votesData.Items || [];

        // 3. Obtener todas las conexiones activas
        const connectionsData = await docClient.send(new ScanCommand({ TableName: CONNECTIONS_TABLE }));
        const connections = connectionsData.Items || [];

        // 4. Enviar (Broadcast) los resultados a todos los clientes conectados
        const postCalls = connections.map(async ({ connectionId }) => {
            try {
                await apiGwClient.send(new PostToConnectionCommand({
                    ConnectionId: connectionId,
                    Data: JSON.stringify({ type: "UPDATE", results: results })
                }));
            } catch (e) {
                // Si el error es 410 (Gone), el usuario cerró la pestaña; limpiamos su registro
                if (e.statusCode === 410 || e.$metadata?.httpStatusCode === 410) {
                    console.log(`Limpiando conexión inactiva: ${connectionId}`);
                    await docClient.send(new DeleteCommand({
                        TableName: CONNECTIONS_TABLE,
                        Key: { connectionId }
                    }));
                } else {
                    console.error(`Error enviando a ${connectionId}:`, e);
                }
            }
        });

        await Promise.all(postCalls);
        return { statusCode: 200, body: "Procesado y difundido" };

    } catch (error) {
        console.error("Error crítico procesando lote SQS:", error);
        // Al lanzar el error, SQS sabrá que falló y NO borrará los mensajes, reintentándolos después (Resiliencia)
        throw error; 
    }
};

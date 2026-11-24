import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, DeleteCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const TABLE_NAME = process.env.CONNECTIONS_TABLE;

export const handler = async (event) => {
    const connectionId = event.requestContext.connectionId;
    const routeKey = event.requestContext.routeKey;

    try {
        if (routeKey === "$connect") {
            // Guardar nueva conexión
            await docClient.send(new PutCommand({
                TableName: TABLE_NAME,
                Item: { connectionId: connectionId }
            }));
            console.log(`Conectado: ${connectionId}`);
        } else if (routeKey === "$disconnect") {
            // Eliminar conexión cerrada
            await docClient.send(new DeleteCommand({
                TableName: TABLE_NAME,
                Key: { connectionId: connectionId }
            }));
            console.log(`Desconectado: ${connectionId}`);
        }
        return { statusCode: 200, body: "OK" };
    } catch (error) {
        console.error("Error en conexión:", error);
        return { statusCode: 500, body: "Error interno" };
    }
};


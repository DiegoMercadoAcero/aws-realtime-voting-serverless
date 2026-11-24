# Sistema de Votaciones en Tiempo Real (AWS Serverless & Event-Driven)

Aplicación web de votaciones en vivo construida bajo una arquitectura 100% Serverless y orientada a eventos en AWS, desplegada automáticamente mediante Infrastructure as Code (Python + Boto3).

## Arquitectura
* **Frontend:** Amazon S3 (Static Website Hosting) + HTML5/JS/Chart.js.
* **Comunicación en Tiempo Real:** Amazon API Gateway (WebSocket API).
* **Cómputo:** AWS Lambda (Node.js 20.x).
* **Resiliencia y Desacoplamiento:** Amazon SQS.
* **Base de Datos:** Amazon DynamoDB (On-Demand).

## Estructura del Proyecto
```text
├── backend/          # Funciones AWS Lambda (Node.js)
├── frontend/         # Aplicación web estática
├── infra/            # Scripts IaC (deploy.py, destroy.py, chaos_test.py)
└── docs/             # Documentación técnica y diagrama de arquitectura

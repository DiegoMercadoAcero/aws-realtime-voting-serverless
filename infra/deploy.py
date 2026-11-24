import boto3
import json
import time
import zipfile
import os
import uuid
from botocore.exceptions import ClientError

# Configuración General
REGION = "us-east-1"
PROJECT_PREFIX = "votaciones-rt"
UNIQUE_ID = str(uuid.uuid4())[:6]  # Sufijo único para evitar colisiones de nombres en S3

# Clientes de Boto3
iam = boto3.client("iam", region_name=REGION)
dynamodb = boto3.client("dynamodb", region_name=REGION)
sqs = boto3.client("sqs", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
apigw = boto3.client("apigatewayv2", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sts = boto3.client("sts", region_name=REGION)

ACCOUNT_ID = sts.get_caller_identity()["Account"]
STATE_FILE = "infra/deployment_state.json"

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

def create_zip(source_dir, output_filename):
    with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, source_dir)
                zf.write(full_path, rel_path)
    with open(output_filename, "rb") as f:
        return f.read()

def get_or_create_iam_role():
    print("1. Configurando permisos IAM...")
    # Verificar si estamos en AWS Academy (usa LabRole por defecto)
    try:
        role = iam.get_role(RoleName="LabRole")
        print("   -> Detectado entorno AWS Academy. Usando 'LabRole'.")
        return role["Role"]["Arn"], False
    except ClientError:
        pass

    # Si es cuenta estándar, crear rol propio
    role_name = f"{PROJECT_PREFIX}-lambda-role-{UNIQUE_ID}"
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    role = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust_policy)
    )
    
    # Adjuntar políticas necesarias
    policies = [
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
        "arn:aws:iam::aws:policy/AmazonSQSFullAccess",
        "arn:aws:iam::aws:policy/AmazonAPIGatewayInvokeFullAccess"
    ]
    for p in policies:
        iam.attach_role_policy(RoleName=role_name, PolicyArn=p)
    
    print(f"   -> Rol IAM creado: {role_name}. Esperando 10s a que se propague...")
    time.sleep(10)
    return role["Role"]["Arn"], True

def deploy():
    state = {"region": REGION, "unique_id": UNIQUE_ID}
    print(f"🚀 Iniciando despliegue automatizado en {REGION} (ID: {UNIQUE_ID})...\n")

    # 1. IAM Role
    role_arn, created_role = get_or_create_iam_role()
    if created_role:
        state["role_name"] = role_arn.split("/")[-1]
    save_state(state)

    # 2. Tablas DynamoDB
    print("2. Creando tablas DynamoDB...")
    votes_table = f"{PROJECT_PREFIX}-votos-{UNIQUE_ID}"
    conn_table = f"{PROJECT_PREFIX}-conexiones-{UNIQUE_ID}"

    for t_name, key_name in [(votes_table, "optionId"), (conn_table, "connectionId")]:
        dynamodb.create_table(
            TableName=t_name,
            KeySchema=[{"AttributeName": key_name, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": key_name, "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST"  # Serverless bajo demanda (capa gratuita)
        )
    
    print("   -> Esperando a que las tablas estén activas...")
    dynamodb.get_waiter("table_exists").wait(TableName=votes_table)
    dynamodb.get_waiter("table_exists").wait(TableName=conn_table)
    state["votes_table"] = votes_table
    state["conn_table"] = conn_table
    save_state(state)

    # 3. Cola SQS (Capa de Resiliencia)
    print("3. Creando cola SQS para resiliencia...")
    queue_name = f"{PROJECT_PREFIX}-cola-{UNIQUE_ID}"
    queue_resp = sqs.create_queue(
        QueueName=queue_name,
        Attributes={"VisibilityTimeout": "30"}
    )
    queue_url = queue_resp["QueueUrl"]
    queue_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    queue_arn = queue_attrs["Attributes"]["QueueArn"]
    state["queue_url"] = queue_url
    save_state(state)
    print(f"   -> Cola creada: {queue_name}")

    # 4. API Gateway (WebSocket) - Lo creamos antes de las Lambdas para obtener su Endpoint
    print("4. Creando API Gateway WebSocket...")
    api_resp = apigw.create_api(
        Name=f"{PROJECT_PREFIX}-ws-{UNIQUE_ID}",
        ProtocolType="WEBSOCKET",
        RouteSelectionExpression="$request.body.action"
    )
    api_id = api_resp["ApiId"]
    ws_url = f"wss://{api_id}.execute-api.{REGION}.amazonaws.com/prod"
    callback_url = f"https://{api_id}.execute-api.{REGION}.amazonaws.com/prod"
    state["api_id"] = api_id
    save_state(state)

    # 5. Empaquetar y Desplegar AWS Lambdas
    print("5. Empaquetando y desplegando funciones Lambda...")
    lambdas_config = [
        {
            "name": f"{PROJECT_PREFIX}-connect-{UNIQUE_ID}",
            "dir": "backend/connect_handler",
            "zip": "infra/connect.zip",
            "env": {"CONNECTIONS_TABLE": conn_table}
        },
        {
            "name": f"{PROJECT_PREFIX}-ingestor-{UNIQUE_ID}",
            "dir": "backend/vote_ingestor",
            "zip": "infra/ingestor.zip",
            "env": {"VOTES_QUEUE_URL": queue_url}
        },
        {
            "name": f"{PROJECT_PREFIX}-processor-{UNIQUE_ID}",
            "dir": "backend/vote_processor",
            "zip": "infra/processor.zip",
            "env": {
                "VOTES_TABLE": votes_table,
                "CONNECTIONS_TABLE": conn_table,
                "WEBSOCKET_ENDPOINT": callback_url
            }
        }
    ]

    lambda_arns = {}
    state["lambdas"] = []

    for cfg in lambdas_config:
        zip_bytes = create_zip(cfg["dir"], cfg["zip"])
        resp = lam.create_function(
            FunctionName=cfg["name"],
            Runtime="nodejs20.x",
            Role=role_arn,
            Handler="index.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=15,
            Environment={"Variables": cfg["env"]}
        )
        lambda_arns[cfg["dir"]] = resp["FunctionArn"]
        state["lambdas"].append(cfg["name"])
        os.remove(cfg["zip"])  # Limpiar zip temporal
        print(f"   -> Lambda desplegada: {cfg['name']}")
    
    save_state(state)
    time.sleep(5)  # Esperar inicialización de Lambdas

    # 6. Conectar SQS -> Lambda Processor (Trigger de eventos)
    print("6. Conectando cola SQS con Lambda Processor...")
    esm = lam.create_event_source_mapping(
        EventSourceArn=queue_arn,
        FunctionName=f"{PROJECT_PREFIX}-processor-{UNIQUE_ID}",
        BatchSize=10,
        Enabled=True
    )
    state["esm_uuid"] = esm["UUID"]
    save_state(state)

    # 7. Configurar Rutas del WebSocket en API Gateway
    print("7. Configurando rutas WebSocket ($connect, $disconnect, vote)...")
    routes = [
        ("$connect", lambda_arns["backend/connect_handler"], f"{PROJECT_PREFIX}-connect-{UNIQUE_ID}"),
        ("$disconnect", lambda_arns["backend/connect_handler"], f"{PROJECT_PREFIX}-connect-{UNIQUE_ID}"),
        ("vote", lambda_arns["backend/vote_ingestor"], f"{PROJECT_PREFIX}-ingestor-{UNIQUE_ID}")
    ]

    for route_key, l_arn, l_name in routes:
        integration = apigw.create_integration(
            ApiId=api_id,
            IntegrationType="AWS_PROXY",
            IntegrationUri=f"arn:aws:apigateway:{REGION}:lambda:path/2015-03-31/functions/{l_arn}/invocations"
        )
        apigw.create_route(
            ApiId=api_id,
            RouteKey=route_key,
            Target=f"integrations/{integration['IntegrationId']}"
        )
        # Dar permiso a API Gateway para invocar la Lambda
        try:
            lam.add_permission(
                FunctionName=l_name,
                StatementId=f"apigw-{route_key.replace('$', '')}-{UNIQUE_ID}",
                Action="lambda:InvokeFunction",
                Principal="apigateway.amazonaws.com",
                SourceArn=f"arn:aws:execute-api:{REGION}:{ACCOUNT_ID}:{api_id}/*/{route_key}"
            )
        except ClientError:
            pass

    # Desplegar Stage 'prod'
    apigw.create_stage(ApiId=api_id, StageName="prod", AutoDeploy=True)
    print(f"   -> WebSocket activo en: {ws_url}")

    # 8. Crear Bucket S3, inyectar URL en Frontend y subir sitio web
    print("8. Desplegando Frontend estático en Amazon S3...")
    bucket_name = f"{PROJECT_PREFIX}-web-{UNIQUE_ID}"
    s3.create_bucket(Bucket=bucket_name)
    state["bucket_name"] = bucket_name
    save_state(state)

    # Desbloquear acceso público para sitio web estático
    s3.delete_public_access_block(Bucket=bucket_name)
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bucket_name}/*"
        }]
    }
    s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(bucket_policy))
    s3.put_bucket_website(
        Bucket=bucket_name,
        WebsiteConfiguration={"IndexDocument": {"Suffix": "index.html"}}
    )

    # Leer HTML original, reemplazar el placeholder y subir a S3
    with open("frontend/index.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    
    final_html = html_content.replace("__WEBSOCKET_URL_PLACEHOLDER__", ws_url)
    
    s3.put_object(
        Bucket=bucket_name,
        Key="index.html",
        Body=final_html.encode("utf-8"),
        ContentType="text/html"
    )

    website_url = f"http://{bucket_name}.s3-website-{REGION}.amazonaws.com"
    
    print("\n" + "="*60)
    print("✅ ¡DESPLIEGUE COMPLETADO CON ÉXITO!")
    print("="*60)
    print(f"🌐 URL de tu Aplicación Web : {website_url}")
    print(f"🔌 Endpoint WebSocket       : {ws_url}")
    print(f"📦 Estado guardado en       : {STATE_FILE}")
    print("="*60)

if __name__ == "__main__":
    deploy()

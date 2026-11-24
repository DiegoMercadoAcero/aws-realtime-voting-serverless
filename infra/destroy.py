import boto3
import json
import os
from botocore.exceptions import ClientError

STATE_FILE = "infra/deployment_state.json"

def destroy():
    if not os.path.exists(STATE_FILE):
        print("❌ No se encontró archivo deployment_state.json. Nada que borrar.")
        return

    with open(STATE_FILE, "r") as f:
        state = json.load(f)

    region = state["region"]
    iam = boto3.client("iam", region_name=region)
    dynamodb = boto3.client("dynamodb", region_name=region)
    sqs = boto3.client("sqs", region_name=region)
    lam = boto3.client("lambda", region_name=region)
    apigw = boto3.client("apigatewayv2", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    print("🧹 Iniciando limpieza completa de recursos AWS...\n")

    # 1. Borrar S3
    if "bucket_name" in state:
        try:
            s3.delete_object(Bucket=state["bucket_name"], Key="index.html")
            s3.delete_bucket(Bucket=state["bucket_name"])
            print(f"   -> Bucket S3 eliminado: {state['bucket_name']}")
        except ClientError as e:
            print(f"   [Aviso S3]: {e}")

    # 2. Borrar API Gateway
    if "api_id" in state:
        try:
            apigw.delete_api(ApiId=state["api_id"])
            print(f"   -> API Gateway eliminado: {state['api_id']}")
        except ClientError as e:
            print(f"   [Aviso API GW]: {e}")

    # 3. Borrar Trigger SQS -> Lambda
    if "esm_uuid" in state:
        try:
            lam.delete_event_source_mapping(UUID=state["esm_uuid"])
            print("   -> Event Source Mapping SQS eliminado.")
        except ClientError as e:
            print(f"   [Aviso ESM]: {e}")

    # 4. Borrar Lambdas
    for l_name in state.get("lambdas", []):
        try:
            lam.delete_function(FunctionName=l_name)
            print(f"   -> Lambda eliminada: {l_name}")
        except ClientError as e:
            print(f"   [Aviso Lambda]: {e}")

    # 5. Borrar Cola SQS
    if "queue_url" in state:
        try:
            sqs.delete_queue(QueueUrl=state["queue_url"])
            print("   -> Cola SQS eliminada.")
        except ClientError as e:
            print(f"   [Aviso SQS]: {e}")

    # 6. Borrar Tablas DynamoDB
    for t_key in ["votes_table", "conn_table"]:
        if t_key in state:
            try:
                dynamodb.delete_table(TableName=state[t_key])
                print(f"   -> Tabla DynamoDB eliminada: {state[t_key]}")
            except ClientError as e:
                print(f"   [Aviso DynamoDB]: {e}")

    # 7. Borrar Rol IAM (si fue creado por el script)
    if "role_name" in state:
        try:
            r_name = state["role_name"]
            attached = iam.list_attached_role_policies(RoleName=r_name)["AttachedPolicies"]
            for p in attached:
                iam.detach_role_policy(RoleName=r_name, PolicyArn=p["PolicyArn"])
            iam.delete_role(RoleName=r_name)
            print(f"   -> Rol IAM eliminado: {r_name}")
        except ClientError as e:
            print(f"   [Aviso IAM]: {e}")

    os.remove(STATE_FILE)
    print("\n✨ Todos los recursos han sido eliminados limpiamente.")

if __name__ == "__main__":
    destroy()

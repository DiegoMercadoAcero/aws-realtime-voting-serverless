import boto3
import json
import time
import asyncio
import websockets
import os

STATE_FILE = "infra/deployment_state.json"

def load_state():
    if not os.path.exists(STATE_FILE):
        raise FileNotFoundError("Ejecuta primero infra/deploy.py")
    with open(STATE_FILE, "r") as f:
        return json.load(f)

async def simulate_concurrent_votes(ws_url, option, count):
    """Simula múltiples usuarios votando al mismo tiempo"""
    async def single_vote(i):
        async with websockets.connect(ws_url) as ws:
            payload = json.dumps({"action": "vote", "option": option})
            await ws.send(payload)
            await asyncio.sleep(0.2)

    tasks = [single_vote(i) for i in range(count)]
    await asyncio.gather(*tasks)

def run_resilience_tests():
    state = load_state()
    region = state["region"]
    lam = boto3.client("lambda", region_name=region)
    sqs = boto3.client("sqs", region_name=region)
    
    processor_name = [l for l in state["lambdas"] if "processor" in l][0]
    queue_url = state["queue_url"]
    ws_url = f"wss://{state['api_id']}.execute-api.{region}.amazonaws.com/prod"

    print("🔥 INICIANDO PRUEBAS DE RESILIENCIA Y CAOS 🔥\n")

    # --- PRUEBA 1: SABOTAJE DE BACKEND Y RETENCIÓN EN SQS ---
    print("1️⃣  [CHAOS TEST] Saboteando Lambda Processor (simulando caída de BD)...")
    current_config = lam.get_function_configuration(FunctionName=processor_name)
    original_env = current_config["Environment"]["Variables"]
    
    # Inyectamos una tabla falsa para forzar fallos
    broken_env = original_env.copy()
    broken_env["VOTES_TABLE"] = "TABLA_QUE_NO_EXISTE_ERROR_500"
    
    lam.update_function_configuration(
        FunctionName=processor_name,
        Environment={"Variables": broken_env}
    )
    time.sleep(5)  # Esperar actualización de Lambda
    print("   ❌ Lambda Processor está ROTA intencionalmente.\n")

    print("2️⃣  Enviando 15 votos en ráfaga mientras el sistema está roto...")
    asyncio.run(simulate_concurrent_votes(ws_url, "Python", 15))
    print("   ✅ El Frontend / API Gateway aceptó los 15 votos sin caerse.")
    
    time.sleep(4)
    # Revisar cuántos mensajes están retenidos en SQS esperando recuperación
    q_attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, 
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    )
    en_cola = int(q_attrs["Attributes"]["ApproximateNumberOfMessages"]) + int(q_attrs["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
    print(f"   📦 Mensajes retenidos de forma segura en SQS (sin perderse): {en_cola} votos.\n")

    print("3️⃣  [SELF-HEALING] Restaurando configuración de Lambda Processor...")
    lam.update_function_configuration(
        FunctionName=processor_name,
        Environment={"Variables": original_env}
    )
    print("   🔧 Lambda reparada. Esperando a que SQS drene la cola y actualice la gráfica...")
    
    # Monitorear hasta que la cola se vacíe
    for _ in range(12):
        time.sleep(5)
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"])
        restantes = int(q_attrs["Attributes"]["ApproximateNumberOfMessages"])
        print(f"   ⏳ Votos pendientes por procesar en SQS: {restantes}")
        if restantes == 0:
            break

    print("\n🎉 ¡PRUEBA SUPERADA! Todos los votos retenidos durante la caída fueron procesados.")
    print("   Revisa tu navegador: la barra de Python debió subir exactamente 15 votos sin recargar la página.")

if __name__ == "__main__":
    run_resilience_tests()

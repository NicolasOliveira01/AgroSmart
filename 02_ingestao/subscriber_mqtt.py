"""
Subscriber MQTT do AgroSmart.
Assina o tópico em que o ESP32 publica, acumula as leituras num buffer
e grava lotes em JSON Lines (uma leitura por linha).
"""
import json
import os
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

# ---------- Configurações ----------
BROKER = "broker.hivemq.com"
PORTA = 1883
TOPICO = "agrosmart-fiap/talhao01/leituras"  # idêntico ao do ESP32

PASTA_SAIDA = "dados_landing"
LOTE_MAX = 20      # grava ao juntar 20 leituras...
INTERVALO_S = 30   # ...ou a cada 30 segundos, o que vier primeiro

buffer = []
trava = threading.Lock()


# ---------- Gravação dos lotes ----------
def gravar_lote(leituras):
    """Grava uma lista de leituras em um arquivo JSONL nomeado pela data e hora (UTC)."""
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    agora = datetime.now(timezone.utc)
    nome = f"{agora:%Y-%m-%dT%H-%M-%S}-{agora.microsecond // 1000:03d}Z.jsonl"
    caminho = os.path.join(PASTA_SAIDA, nome)

    with open(caminho, "x", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(l, ensure_ascii=False) for l in leituras))

    print(f"[lote] {len(leituras)} leituras -> {nome}")


def descarregar():
    """Esvazia o buffer e grava o conteúdo, se houver algo."""
    with trava:
        if not buffer:
            return
        leituras = list(buffer)
        buffer.clear()
    gravar_lote(leituras)


# ---------- Callbacks do MQTT ----------
def ao_conectar(cliente, userdata, flags, codigo, properties):
    if codigo.is_failure:
        print(f"[mqtt] falha ao conectar: {codigo}")
        return
    cliente.subscribe(TOPICO)
    print(f"[mqtt] conectado, assinando: {TOPICO}")


def ao_desconectar(cliente, userdata, flags, codigo, properties):
    print(f"[mqtt] desconectado ({codigo}), tentando reconectar...")


def ao_receber(cliente, userdata, msg):
    try:
        leitura = json.loads(msg.payload)
    except json.JSONDecodeError:
        print(f"[aviso] mensagem inválida ignorada: {msg.payload[:80]!r}")
        return

    leitura["_recebido_em"] = datetime.now(timezone.utc).isoformat()
    print(
        f"[recebido] {leitura.get('talhao')} | {leitura['timestamp']} | "
        f"temp={leitura.get('temperatura')}°C  "
        f"ar={leitura.get('umidade_ar')}%  "
        f"solo={leitura.get('umidade_solo')}%  "
        f"luz={leitura.get('luminosidade')}%"
    )

    with trava:
        buffer.append(leitura)
        cheio = len(buffer) >= LOTE_MAX
    if cheio:
        descarregar()


# ---------- Execução ----------
def main():
    cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    cliente.on_connect = ao_conectar
    cliente.on_disconnect = ao_desconectar
    cliente.on_message = ao_receber

    cliente.connect(BROKER, PORTA)
    cliente.loop_start()  # recebe as mensagens numa thread separada

    try:
        while True:
            time.sleep(INTERVALO_S)
            descarregar()  # garante gravação mesmo com poucas mensagens
    except KeyboardInterrupt:
        print("\nEncerrando...")
    finally:
        descarregar()       # não perde o que ficou no buffer
        cliente.loop_stop()
        cliente.disconnect()


if __name__ == "__main__":
    main()
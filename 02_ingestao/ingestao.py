"""
Subscriber MQTT do AgroSmart.
Assina o tópico em que o ESP32 publica, acumula as leituras num buffer
e grava lotes em JSON Lines (uma leitura por linha).
Ao encerrar, envia todos os arquivos para o stage interno do Snowflake
e apaga localmente os que foram enviados com sucesso.
"""
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ---------- Configurações ----------
BROKER = "broker.hivemq.com"
PORTA = 1883
TOPICO = "agrosmart-fiap/talhao01/leituras"  # idêntico ao do ESP32

PASTA_SAIDA = "dados_landing"
LOTE_MAX = 20      # grava ao juntar 20 leituras...
INTERVALO_S = 30   # ...ou a cada 30 segundos, o que vier primeiro

STAGE = "@AGROSMART.RAW.STG_LEITURAS/leituras/"
TABELA = "AGROSMART.RAW.LEITURAS_SENSORES"

buffer = []
trava = threading.Lock()

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
        f"[recebido] {leitura.get('talhao')} | {leitura.get('timestamp')} | "
        f"temp={leitura.get('temperatura')}°C  "
        f"ar={leitura.get('umidade_ar')}%  "
        f"solo={leitura.get('umidade_solo')}%  "
        f"luz={leitura.get('luminosidade')}%  "
        f"risco={leitura.get('risco_geral')}"
    )

    with trava:
        buffer.append(leitura)
        cheio = len(buffer) >= LOTE_MAX
    if cheio:
        descarregar()

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

# ---------- Snowflake ----------
def conectar_snowflake():
    """Abre uma conexão com o Snowflake usando usuário e senha do .env."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

def enviar_arquivo(conn, arquivo):
    """Envia um arquivo para o stage.
    Retorna True se o Snowflake confirmou o envio, False caso contrário."""
    caminho = arquivo.resolve().as_posix()  # o PUT exige barras normais, inclusive no Windows

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"PUT 'file://{caminho}' {STAGE} "
                "AUTO_COMPRESS=TRUE OVERWRITE=FALSE"
            )
    except snowflake.connector.errors.Error as e:
        print(f"[erro] {arquivo.name}: {e}")
        return False

    print(f"[snowflake] enviado {arquivo.name}")
    return True

def enviar_arquivos(conn):
    """Percorre os .jsonl da pasta de saída, envia cada um para o stage
    e apaga localmente os que foram confirmados."""
    arquivos = sorted(Path(PASTA_SAIDA).glob("*.jsonl"))
    if not arquivos:
        print("[snowflake] nenhum arquivo para enviar")
        return

    print(f"[snowflake] enviando {len(arquivos)} arquivo(s)...")
    enviados = 0
    for arquivo in arquivos:
        if enviar_arquivo(conn, arquivo):
            arquivo.unlink()  # só apaga o que o Snowflake confirmou
            enviados += 1

    print(f"[snowflake] {enviados}/{len(arquivos)} enviado(s)")

def carregar_tabela(conn):
    """Carrega na tabela raw os arquivos do stage que ainda não foram carregados."""
    sql = f"""
        copy into {TABELA} (_arquivo, _linha_arquivo, _carregado_em, _doc)
        from (
            select
                metadata$filename,
                metadata$file_row_number,
                sysdate(),
                $1
            from {STAGE}
        )
        file_format = (type = json)
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            colunas = [c[0].lower() for c in cur.description]
            resultados = [dict(zip(colunas, linha)) for linha in cur.fetchall()]
    except snowflake.connector.errors.Error as e:
        print(f"[erro] COPY INTO falhou: {e}")
        return

    # sem arquivos novos, o Snowflake responde só com uma mensagem, sem a coluna "file"
    if "file" not in colunas:
        print("[copy] nenhum arquivo novo para carregar")
        return

    total = 0
    for r in resultados:
        print(f"[copy] {r['status']:<16} {r['file']} ({r['rows_loaded']} linhas)")
        total += r["rows_loaded"] or 0

    print(f"[copy] {total} linha(s) carregada(s) em {TABELA}")


# ---------- Execução ----------
def main():
    # conecta primeiro: se a credencial estiver errada, falha antes de começar a coleta
    conn = conectar_snowflake()
    print("[snowflake] conectado")

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
        cliente.loop_stop()    # para de receber antes do último flush
        cliente.disconnect()
        descarregar()          # não perde o que ficou no buffer
        enviar_arquivos(conn)  # sobe tudo e apaga os enviados
        carregar_tabela(conn)   # stage → tabela raw
        conn.close()

if __name__ == "__main__":
    main()

"""
Subscriber MQTT do AgroSmart.
Assina o tópico em que o ESP32 publica, acumula as leituras num buffer
e grava lotes em JSON Lines (uma leitura por linha).
A cada INTERVALO_CARGA_S (padrão 5 min) e ao encerrar, envia os arquivos para o
stage interno do Snowflake, roda o COPY INTO e apaga localmente os que foram enviados.
Funciona rodando à mão (Ctrl+C encerra) e em container (docker compose stop envia
SIGTERM, que é tratado do mesmo jeito).

Logs: console + arquivo logs/ingestao.log (na raiz do projeto), com rotação.
O nível vem da variável LOG_LEVEL do .env (padrão INFO; use DEBUG para ver cada leitura).
"""
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
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
INTERVALO_CARGA_S = int(os.getenv("INTERVALO_CARGA_S", "300"))  # carga no Snowflake (padrão: 5 min)

STAGE = "@AGROSMART.RAW.STG_LEITURAS/leituras/"
TABELA = "AGROSMART.RAW.LEITURAS_SENSORES"

# caminho montado a partir do script, e não da pasta de onde ele é executado
PASTA_LOGS = Path(__file__).resolve().parent.parent / "logs"
ARQUIVO_LOG = PASTA_LOGS / "ingestao.log"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# um logger por componente: o nome aparece em cada linha do log
log = logging.getLogger("ingestao")
log_mqtt = logging.getLogger("mqtt")
log_lote = logging.getLogger("lote")
log_sf = logging.getLogger("snowflake")
log_copy = logging.getLogger("copy")

buffer = []
trava = threading.Lock()
parar = threading.Event()  # sinalizado pelo SIGTERM (docker compose stop)

# contadores da execução, mostrados no resumo ao encerrar
contadores = {
    "recebidas": 0,
    "invalidas": 0,
    "lotes": 0,
    "arquivos_enviados": 0,
    "arquivos_com_erro": 0,
    "linhas_carregadas": 0,
}


def somar(chave, valor=1):
    """Incrementa um contador. Usa a trava porque o callback do MQTT roda em outra thread."""
    with trava:
        contadores[chave] += valor


# ---------- Logging ----------
def configurar_logging():
    """Envia os logs para o console e para um arquivo com rotação."""
    PASTA_LOGS.mkdir(exist_ok=True)

    formato = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)-9s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formato)

    # ao chegar em 5 MB vira ingestao.log.1 e começa outro; guarda até 3 antigos
    arquivo = RotatingFileHandler(
        ARQUIVO_LOG, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    arquivo.setFormatter(formato)

    raiz = logging.getLogger()
    raiz.setLevel(LOG_LEVEL)
    raiz.addHandler(console)
    raiz.addHandler(arquivo)

    # bibliotecas externas: só avisos e erros, senão o DEBUG vira ruído
    for nome in ("snowflake.connector", "botocore", "boto3", "urllib3", "paho"):
        logging.getLogger(nome).setLevel(logging.WARNING)


# ---------- Callbacks do MQTT ----------
def ao_conectar(cliente, userdata, flags, codigo, properties):
    if codigo.is_failure:
        log_mqtt.error("falha ao conectar no broker: %s", codigo)
        return
    cliente.subscribe(TOPICO)
    log_mqtt.info("conectado, assinando: %s", TOPICO)


def ao_desconectar(cliente, userdata, flags, codigo, properties):
    if codigo.is_failure:
        log_mqtt.warning("desconectado (%s), tentando reconectar...", codigo)
    else:
        log_mqtt.info("desconectado do broker")


def ao_receber(cliente, userdata, msg):
    try:
        leitura = json.loads(msg.payload)
    except json.JSONDecodeError:
        somar("invalidas")
        log_mqtt.warning("mensagem inválida ignorada: %r", msg.payload[:80])
        return

    leitura["_recebido_em"] = datetime.now(timezone.utc).isoformat()
    somar("recebidas")
    log_mqtt.debug(
        "recebido %s | %s | temp=%s°C ar=%s%% solo=%s%% luz=%s%% risco=%s",
        leitura.get("talhao"),
        leitura.get("timestamp"),
        leitura.get("temperatura"),
        leitura.get("umidade_ar"),
        leitura.get("umidade_solo"),
        leitura.get("luminosidade"),
        leitura.get("risco_geral"),
    )

    with trava:
        buffer.append(leitura)
        cheio = len(buffer) >= LOTE_MAX
    if cheio:
        # um erro aqui roda na thread do MQTT: se não for tratado, a recepção para
        try:
            descarregar()
        except Exception:
            log_lote.exception("falha ao gravar lote")


# ---------- Gravação dos lotes ----------
def gravar_lote(leituras):
    """Grava uma lista de leituras em um arquivo JSONL nomeado pela data e hora (UTC)."""
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    # dois lotes no mesmo milissegundo teriam o mesmo nome: o modo "x" recusa
    # sobrescrever, então espera 1 ms e tenta com um novo horário
    for _ in range(5):
        agora = datetime.now(timezone.utc)
        nome = f"{agora:%Y-%m-%dT%H-%M-%S}-{agora.microsecond // 1000:03d}Z.jsonl"
        caminho = os.path.join(PASTA_SAIDA, nome)
        temporario = caminho + ".tmp"
        try:
            if os.path.exists(caminho):
                raise FileExistsError(caminho)
            # grava num .tmp e só renomeia no final: a carga procura apenas *.jsonl,
            # então nunca envia um arquivo que ainda está sendo escrito
            with open(temporario, "x", encoding="utf-8") as f:
                f.write("\n".join(json.dumps(l, ensure_ascii=False) for l in leituras))
            os.replace(temporario, caminho)
            break
        except FileExistsError:
            log_lote.debug("nome %s já existe, tentando outro", nome)
            time.sleep(0.001)
    else:
        raise FileExistsError(f"não foi possível gerar um nome único em {PASTA_SAIDA}")

    somar("lotes")
    log_lote.info("%d leituras -> %s", len(leituras), nome)


def descarregar():
    """Esvazia o buffer e grava o conteúdo, se houver algo."""
    with trava:
        if not buffer:
            return
        leituras = list(buffer)
        buffer.clear()
    try:
        gravar_lote(leituras)
    except OSError:
        log_lote.error("lote de %d leituras não foi gravado", len(leituras))
        raise


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
        log_sf.error("falha no PUT de %s: %s", arquivo.name, e)
        return False

    log_sf.info("enviado %s", arquivo.name)
    return True

def enviar_arquivos(conn):
    """Percorre os .jsonl da pasta de saída, envia cada um para o stage
    e apaga localmente os que foram confirmados."""
    arquivos = sorted(Path(PASTA_SAIDA).glob("*.jsonl"))
    if not arquivos:
        log_sf.info("nenhum arquivo para enviar")
        return

    log_sf.info("enviando %d arquivo(s)...", len(arquivos))
    enviados = 0
    for arquivo in arquivos:
        if enviar_arquivo(conn, arquivo):
            arquivo.unlink()  # só apaga o que o Snowflake confirmou
            enviados += 1

    somar("arquivos_enviados", enviados)
    somar("arquivos_com_erro", len(arquivos) - enviados)
    if enviados < len(arquivos):
        log_sf.warning(
            "%d/%d enviado(s); os que falharam continuam em %s",
            enviados, len(arquivos), PASTA_SAIDA,
        )
    else:
        log_sf.info("%d/%d enviado(s)", enviados, len(arquivos))

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
        log_copy.error("COPY INTO falhou: %s", e)
        return

    # sem arquivos novos, o Snowflake responde só com uma mensagem, sem a coluna "file"
    if "file" not in colunas:
        log_copy.info("nenhum arquivo novo para carregar")
        return

    total = 0
    for r in resultados:
        nivel = logging.INFO if r["status"] == "LOADED" else logging.WARNING
        log_copy.log(nivel, "%-16s %s (%s linhas)", r["status"], r["file"], r["rows_loaded"])
        total += r["rows_loaded"] or 0

    somar("linhas_carregadas", total)
    log_copy.info("%d linha(s) carregada(s) em %s", total, TABELA)


def descarregar_sem_parar():
    """Como descarregar(), mas registra o erro em vez de derrubar o processo."""
    try:
        descarregar()
    except Exception:
        log_lote.exception("falha ao gravar lote")


def executar_carga():
    """Grava o que está no buffer, envia os .jsonl para a stage e roda o COPY INTO.

    Abre uma conexão nova a cada carga: num processo que roda por horas, uma
    conexão única pode expirar. Se algo falhar, os arquivos continuam na pasta
    e sobem na próxima carga (o COPY INTO ignora arquivos já carregados)."""
    descarregar_sem_parar()

    # sem arquivos novos não há o que carregar: pula a carga inteira, sem conectar.
    # O COPY INTO liga o warehouse, e rodá-lo sem dados só consumiria créditos.
    # Se uma carga anterior deixou arquivos pendentes na stage, o próximo COPY
    # com dados novos carrega tudo junto.
    if not any(Path(PASTA_SAIDA).glob("*.jsonl")):
        log_sf.info("nenhum dado novo, carga pulada")
        return

    try:
        conn = conectar_snowflake()
    except Exception:
        log_sf.exception("falha ao conectar; os arquivos ficam em %s para a próxima carga", PASTA_SAIDA)
        return
    try:
        enviar_arquivos(conn)
        carregar_tabela(conn)
    except Exception:
        log_sf.exception("falha durante a carga; o que não subiu fica para a próxima")
    finally:
        conn.close()


def registrar_ciclo(anterior):
    """Resumo só do último intervalo de carga.
    Retorna os contadores atuais, que viram a base do próximo ciclo."""
    with trava:
        atual = dict(contadores)
    d = {k: atual[k] - anterior[k] for k in atual}
    log.info(
        "ciclo | recebidas=%d inválidas=%d lotes=%d arquivos_enviados=%d "
        "arquivos_com_erro=%d linhas_carregadas=%d",
        d["recebidas"], d["invalidas"], d["lotes"], d["arquivos_enviados"],
        d["arquivos_com_erro"], d["linhas_carregadas"],
    )
    return atual


def registrar_resumo(inicio):
    """Uma linha com os totais da execução: serve de indicador rápido de funcionamento."""
    duracao = timedelta(seconds=round(time.monotonic() - inicio))
    with trava:
        c = dict(contadores)
    log.info(
        "resumo | recebidas=%d inválidas=%d lotes=%d arquivos_enviados=%d "
        "arquivos_com_erro=%d linhas_carregadas=%d duração=%s",
        c["recebidas"], c["invalidas"], c["lotes"], c["arquivos_enviados"],
        c["arquivos_com_erro"], c["linhas_carregadas"], duracao,
    )


# ---------- Execução ----------
def main():
    inicio = time.monotonic()
    log.info(
        "iniciando ingestão | broker=%s tópico=%s carga a cada %ds nível=%s log=%s",
        BROKER, TOPICO, INTERVALO_CARGA_S, LOG_LEVEL, ARQUIVO_LOG,
    )

    # testa a conexão antes de começar: se a credencial estiver errada, falha logo.
    # Cada carga abre a sua própria conexão depois.
    conectar_snowflake().close()
    log_sf.info("conexão testada")

    # docker compose stop envia SIGTERM (e não Ctrl+C): tratado do mesmo jeito,
    # para fazer a última carga antes de sair
    signal.signal(signal.SIGTERM, lambda *_: parar.set())

    cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    cliente.enable_logger(logging.getLogger("paho"))  # erros internos da biblioteca vão para o log
    cliente.on_connect = ao_conectar
    cliente.on_disconnect = ao_desconectar
    cliente.on_message = ao_receber

    cliente.connect(BROKER, PORTA)
    cliente.loop_start()  # recebe as mensagens numa thread separada

    proximo_lote = time.monotonic() + INTERVALO_S
    proxima_carga = time.monotonic() + INTERVALO_CARGA_S
    with trava:
        base_ciclo = dict(contadores)

    try:
        # passos de 1 s (em vez de dormir 30 s de uma vez) para reagir rápido ao Ctrl+C e ao SIGTERM
        while not parar.is_set():
            time.sleep(1)
            agora = time.monotonic()
            if agora >= proximo_lote:
                descarregar_sem_parar()  # garante gravação mesmo com poucas mensagens
                proximo_lote = agora + INTERVALO_S
            if agora >= proxima_carga:
                executar_carga()
                base_ciclo = registrar_ciclo(base_ciclo)
                proxima_carga = time.monotonic() + INTERVALO_CARGA_S
        log.info("encerrando (sinal de parada)...")
    except KeyboardInterrupt:
        log.info("encerrando (Ctrl+C)...")
    finally:
        cliente.loop_stop()    # para de receber antes da última carga
        cliente.disconnect()
        executar_carga()       # grava o buffer e sobe o que faltou
        registrar_resumo(inicio)

if __name__ == "__main__":
    configurar_logging()
    try:
        main()
    except Exception:
        # qualquer erro não tratado (ex.: credencial errada) vai com traceback para o arquivo
        log.exception("erro inesperado, encerrando")
        sys.exit(1)

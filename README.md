# 🌱 AgroSmart — Fase 7: Plataforma integrada de monitoramento da lavoura

> **Como está a saúde da minha lavoura e o que eu devo fazer agora?**

O AgroSmart usa tecnologia e dados para monitorar a lavoura e apoiar decisões no campo. Nesta Fase 6, a entrega foi fazer uma **pipeline de dados ponta a ponta**: os dados nascem em um sistema embarcado (simulado), são ingeridos, armazenados, tratados, passam por regras de decisão e terminam em um dashboard que mostra o que está acontecendo e o que fazer a seguir.

## 🧭 Grupo

| Nome | RM |
|------|----|
| Henrique Parra | RM551973 |
| Guilherme Barreto | RM97674 |
| Roberto Oliveira | RM551460 |
| Nicolas Oliveira | RM98939 |
| Marcos Vinicius | RM99894 |

---

## Arquitetura

<p align="center">
  <img src="docs/diagrama.png" alt="Diagrama da arquitetura" width="900">
</p>

### Geração e Decisão

O sistema embarcado foi simulado no **Wokwi**, com um **ESP32** ligado a três sensores:

- **DHT22**: mede a temperatura e a umidade do ar;
- **LDR**: mede a luminosidade;
- **Potenciômetro**: simula a umidade do solo.

Ao iniciar a simulação, o **ESP32** se conecta ao **Wi-Fi**, lê os sensores e monta um **JSON** com cada leitura. Esse **JSON** é publicado em um tópico do **HiveMQ**, um **broker MQTT público**. O broker funciona como intermediário: recebe as mensagens do ESP32 e as repassa para quem assinou o tópico. Assim, quem produz o dado e quem consome não precisam se conectar diretamente.

#### Regras simples

| Condição | Risco | Recomendação | Por quê |
|----------|-------|--------------|---------|
| Umidade do solo < 30% | Alto | Irrigar o talhão | A planta começa a sofrer com a falta de água |
| Umidade do solo > 85% | Médio | Suspender a irrigação e verificar a drenagem | Solo encharcado sufoca as raízes e favorece doenças |
| Temperatura > 35 °C | Médio | Monitorar estresse térmico | Calor excessivo reduz a fotossíntese e o crescimento |
| Temperatura < 5 °C | Alto | Risco de geada: proteger a lavoura | Geada pode matar as plantas em poucas horas |

#### Decisão automática

As regras rodam **dentro do próprio ESP32**, a cada leitura (uma por segundo), sem nenhuma intervenção humana. O firmware lê os sensores, compara cada valor com os limites da tabela acima e já publica o JSON com:

- **`alertas`**: a lista de problemas detectados, cada um com o seu risco e a recomendação de ação;
- **`risco_geral`**: o maior risco entre os alertas da leitura, ou `Nenhum` quando nenhuma regra dispara.

As regras são independentes, então mais de uma pode disparar na mesma leitura (por exemplo, solo seco + estresse térmico). Como a decisão é tomada no dispositivo, na borda (*edge*), ela não depende da nuvem: o dado já sai do campo classificado, e as etapas seguintes só armazenam, tratam e exibem o que foi decidido.

A escala de risco tem quatro níveis: `Nenhum` (0), `Baixo` (1), `Médio` (2) e `Alto` (3). O nível `Baixo` está reservado para regras futuras; nenhuma regra atual o utiliza.

<p align="center">
    <img src="docs/wokwi.png" alt="Diagrama da arquitetura" width="50%">
    <br>
    <em>Circuito do ESP32 simulado no Wokwi, com DHT22, LDR e potenciômetro sendo simulado</em>
</p>

<p align="center">
    <img src="docs/broker_mqtt_recebendo_dados.png" alt="Diagrama da arquitetura" width="50%">
    <br>
    <em>Broker MQTT recebendo os dados enviados pelo esp32</em>
</p>


### Ingestão

O script **Python** se conecta ao **HiveMQ**, assina o tópico e passa a receber cada leitura publicada pelo ESP32.

```json
{
  "sensor_id": "SENSOR-T1-01",
  "talhao": "TALHAO-01",
  "timestamp": "2026-10-09T15:00:40Z",
  "temperatura": 72.4,
  "umidade_ar": 64.5,
  "umidade_solo": 6,
  "luminosidade": 93.8,
  "alertas": [
    {
      "problema": "solo_seco",
      "risco": "Alto",
      "recomendacao": "Irrigar o talhão"
    },
    {
      "problema": "estresse_termico",
      "risco": "Médio",
      "recomendacao": "Monitorar estresse térmico"
    }
  ],
  "risco_geral": "Alto",
  "_recebido_em": "2026-10-09T15:01:34.227704+00:00"
}
```

Quando o buffer chega a 20 leituras ou passam 30 segundos, o que acontecer primeiro, as leituras são gravadas em um arquivo **.jsonl**, com um JSON por linha. Ao final da execução, o script se autentica no **Snowflake**, envia os arquivos para a **stage** e executa o `COPY INTO`, que carrega os dados brutos na tabela source.

<p align="center">
    <img src="docs/jsonl.png" alt="Diagrama da arquitetura" width="50%">
    <br>
    <em>Arquivo jsonl criado com todos os json recebidos do wokwi</em>
</p>

<p align="center">
    <img src="docs/stage.png" alt="Diagrama da arquitetura" width="50%">
    <br>
    <em>Depois de ter feito o upload do arquivo jsonl na stage</em>
</p>

<p align="center">
    <img src="docs/source.png" alt="Diagrama da arquitetura" width="50%">
    <br>
    <em>Depois de ter feito o copy into e os dados dos arquivos jsonl na tabela do snowflake na source</em>
</p>

#### Logs da aplicação

O script de ingestão registra tudo o que acontece usando o módulo `logging` do Python. Cada linha tem data e hora, nível e componente, e é gravada em dois lugares:

- **console**: para acompanhar a execução em tempo real;
- **arquivo `logs/ingestao.log`**, na raiz do projeto: fica o histórico. O arquivo tem rotação: ao chegar em 5 MB vira `ingestao.log.1` e um novo é criado, mantendo no máximo 3 arquivos antigos.

| Nível | Quando é usado |
|-------|----------------|
| `DEBUG` | Cada leitura recebida do broker (uma por segundo) |
| `INFO` | Funcionamento normal: conexão, lote gravado, arquivo enviado, `COPY INTO` |
| `WARNING` | Algo fora do esperado, mas o sistema continua: mensagem inválida, queda do broker |
| `ERROR` | Falha: erro no envio para a stage, `COPY INTO` com erro, falha inesperada (com o *traceback* completo) |

O nível mínimo é definido pela variável `LOG_LEVEL` no `.env`: `INFO` (padrão) para um log enxuto, ou `DEBUG` para ver também cada leitura capturada.

Ao encerrar, o script registra um **resumo da execução**, que serve como indicador rápido de funcionamento: se `recebidas` e `linhas_carregadas` batem e não há arquivos com erro, nenhuma leitura se perdeu no caminho.

```
2026-11-05 00:40:56 | INFO    | ingestao  | iniciando ingestão | broker=broker.hivemq.com tópico=agrosmart-fiap/talhao01/leituras nível=INFO log=C:\repositorios\AgroSmart\logs\ingestao.log
2026-11-05 00:40:59 | INFO    | snowflake | conectado
2026-11-05 00:41:05 | INFO    | mqtt      | conectado, assinando: agrosmart-fiap/talhao01/leituras
2026-11-05 00:41:35 | INFO    | lote      | 14 leituras -> 2026-11-05T03-41-35-816Z.jsonl
2026-11-05 00:42:05 | INFO    | lote      | 14 leituras -> 2026-11-05T03-42-05-819Z.jsonl
2026-11-05 00:42:07 | INFO    | ingestao  | encerrando (Ctrl+C)...
2026-11-05 00:42:07 | INFO    | lote      | 1 leituras -> 2026-11-05T03-42-07-555Z.jsonl
2026-11-05 00:42:08 | INFO    | snowflake | 3/3 enviado(s)
2026-11-05 00:42:10 | INFO    | copy      | 29 linha(s) carregada(s) em AGROSMART.RAW.LEITURAS_SENSORES
2026-11-05 00:42:10 | INFO    | ingestao  | resumo | recebidas=29 inválidas=0 lotes=3 arquivos_enviados=3 arquivos_com_erro=0 linhas_carregadas=29 duração=0:01:13
```

Com `LOG_LEVEL=DEBUG`, cada leitura também aparece no log:

```
2026-11-05 00:41:06 | DEBUG   | mqtt      | recebido TALHAO-01 | 2026-11-05T03:41:06Z | temp=28.4°C ar=52.0% solo=25.1% luz=57.3% risco=Alto
```

### Tratamento

O **dbt** lê a tabela source e transforma os dados em camadas:

- **STG**: converte o JSON bruto em uma tabela relacional, com uma coluna para cada campo e os tipos corretos;
- **FINAL**: expõe os dados prontos para o consumo do Power BI.

<p align="center">
    <img src="docs/final_alertas.png" alt="Diagrama da arquitetura" width="50%">
    <br>
    <em>Tabela alertas na camada final do dbt</em>
</p>

<p align="center">
    <img src="docs/final_leituras.png" alt="Diagrama da arquitetura" width="50%">
    <br>
    <em>Tabela leituras na camada final do dbt</em>
</p>

### Visualização

O **Power BI** se conecta à camada FINAL no Snowflake e mostra os indicadores dos sensores, a evolução das leituras no tempo e os alertas gerados pelas regras de decisão. Assim, o produtor consegue responder à pergunta central do projeto: como está a saúde da lavoura e o que fazer agora.

<p align="center">
    <img src="docs/powerbi.png" alt="Diagrama da arquitetura" width="50%">
    <br>
    <em>Power bi usando as tabelas da camada final</em>
</p>

## 🎥 Vídeo de demonstração

[Link para o vídeo](https://youtu.be/UhkiySBNCMc)
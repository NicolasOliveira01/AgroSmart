// ===== AgroSmart - Estação de campo (Fase 6) =====
// Lê o DHT22, o potenciômetro (umidade do solo) e o LDR (luminosidade), monta um JSON e publica no broker MQTT.
// Biblioteca do sensor: DHT sensor library (Adafruit) + Adafruit Unified Sensor

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <DHT.h>
#include <time.h>

// ---------- Configurações ----------
const char* WIFI_SSID     = "Wokwi-GUEST";
const char* WIFI_PASSWORD = "";
const char* MQTT_BROKER   = "broker.hivemq.com";
const int   MQTT_PORT     = 1883;
const char* MQTT_TOPIC    = "agrosmart-fiap/talhao01/leituras"; // troque por um nome único

const char* SENSOR_ID = "SENSOR-T1-01";
const char* TALHAO    = "TALHAO-01";

const int DHT_PIN  = 4;                    // fio verde (dados) está no GPIO 4
const int SOLO_PIN = 34;                   // SIG do potenciômetro (ADC1, funciona com Wi-Fi)
const int LDR_PIN  = 33;                   // AO do módulo LDR (ADC1, funciona com Wi-Fi)
const unsigned long INTERVALO_MS = 1000;   // intervalo entre leituras

DHT dht(DHT_PIN, DHT22);                   // pino e modelo definidos na declaração
WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);
unsigned long ultimoEnvio = 0;

// ---------- Wi-Fi ----------
void conectarWiFi() {
  Serial.print("Conectando ao Wi-Fi");
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);  // canal 6 acelera a conexão no Wokwi
  while (WiFi.status() != WL_CONNECTED) {
    delay(250);
    Serial.print(".");
  }
  Serial.println(" conectado!");
}

// ---------- Horário (NTP) ----------
void sincronizarHorario() {
  configTime(0, 0, "pool.ntp.org");         // UTC
  Serial.print("Sincronizando horário (NTP)");
  struct tm t;
  while (!getLocalTime(&t)) {
    delay(250);
    Serial.print(".");
  }
  Serial.println(" ok!");
}

String timestampUTC() {
  struct tm t;
  getLocalTime(&t);
  char buf[25];
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &t);
  return String(buf);
}

// ---------- MQTT ----------
void conectarMQTT() {
  while (!mqtt.connected()) {
    String clientId = "agrosmart-esp32-" + String(random(0xffff), HEX);
    Serial.print("Conectando ao broker MQTT... ");
    if (mqtt.connect(clientId.c_str())) {
      Serial.println("conectado!");
    } else {
      Serial.printf("falhou (rc=%d), tentando em 2s\n", mqtt.state());
      delay(2000);
    }
  }
}

// ---------- Leitura e envio ----------
void enviarLeitura() {
  float temperatura = dht.readTemperature();
  float umidade     = dht.readHumidity();

  // O DHT22 às vezes falha e devolve NaN: não enviamos leitura inválida
  if (isnan(temperatura) || isnan(umidade)) {
    Serial.println("Falha na leitura do DHT22, pulando envio.");
    return;
  }

  // Potenciômetro simulando o sensor de umidade do solo
  // ADC de 12 bits: 0 a 4095 -> convertido para 0 a 100%
  int brutoSolo = analogRead(SOLO_PIN);
  float umidadeSolo = brutoSolo * 100.0 / 4095.0;

  // LDR: no módulo, mais luz = menor tensão no AO, por isso o valor é invertido
  int brutoLuz = analogRead(LDR_PIN);
  float luminosidade = 100.0 - (brutoLuz * 100.0 / 4095.0);

  JsonDocument doc;
  doc["sensor_id"]   = SENSOR_ID;
  doc["talhao"]      = TALHAO;
  doc["timestamp"]   = timestampUTC();
  doc["temperatura"] = round(temperatura * 10) / 10.0;
  doc["umidade_ar"]  = round(umidade * 10) / 10.0;
  doc["umidade_solo"] = round(umidadeSolo * 10) / 10.0;
  doc["luminosidade"] = round(luminosidade * 10) / 10.0;

  char payload[256];
  serializeJson(doc, payload);

  bool ok = mqtt.publish(MQTT_TOPIC, payload);
  Serial.printf("%s -> %s\n", ok ? "Enviado" : "ERRO ao enviar", payload);
}

// ---------- Setup e loop ----------
void setup() {
  Serial.begin(115200);
  dht.begin();
  analogReadResolution(12);                // garante leitura de 0 a 4095
  conectarWiFi();
  sincronizarHorario();
  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  conectarMQTT();
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) conectarWiFi();
  if (!mqtt.connected()) conectarMQTT();
  mqtt.loop();   // mantém a conexão MQTT viva

  // millis() em vez de delay(): o loop continua rodando e o mqtt.loop() não para
  if (millis() - ultimoEnvio >= INTERVALO_MS) {
    ultimoEnvio = millis();
    enviarLeitura();
  }
}

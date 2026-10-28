// ESP32 + MFRC522 RFID reader -> sends scans to the queue server
// Libraries: MFRC522 (by GithubCommunity), built-in WiFi + HTTPClient
//
// Wiring (MFRC522 -> ESP32):
//   SDA -> GPIO 5   SCK -> GPIO 18   MOSI -> GPIO 23   MISO -> GPIO 19
//   RST -> GPIO 22  3.3V -> 3.3V     GND -> GND

#include <WiFi.h>
#include <HTTPClient.h>
#include <SPI.h>
#include <MFRC522.h>

const char* WIFI_SSID   = "YOUR_WIFI";
const char* WIFI_PASS   = "YOUR_PASSWORD";
const char* SERVER_URL  = "http://192.168.1.10:8000/api/scan"; // your PC's local IP
const char* DEVICE_KEY  = "change-me";                         // same as DEVICE_API_KEY in .env

#define SS_PIN  5
#define RST_PIN 22
#define BUZZER  4   // optional

MFRC522 rfid(SS_PIN, RST_PIN);

void setup() {
  Serial.begin(115200);
  pinMode(BUZZER, OUTPUT);
  SPI.begin();
  rfid.PCD_Init();

  WiFi.begin(WIFI_SSID, WIFI_PASS);
  Serial.print("Connecting WiFi");
  while (WiFi.status() != WL_CONNECTED) { delay(400); Serial.print("."); }
  Serial.println("\nConnected: " + WiFi.localIP().toString());
}

String readUid() {
  String uid = "";
  for (byte i = 0; i < rfid.uid.size; i++) {
    if (rfid.uid.uidByte[i] < 0x10) uid += "0";
    uid += String(rfid.uid.uidByte[i], HEX);
  }
  uid.toUpperCase();
  return uid;
}

void beep(int times) {
  for (int i = 0; i < times; i++) { digitalWrite(BUZZER, HIGH); delay(120); digitalWrite(BUZZER, LOW); delay(100); }
}

void loop() {
  if (!rfid.PICC_IsNewCardPresent() || !rfid.PICC_ReadCardSerial()) return;

  String uid = readUid();
  Serial.println("Card: " + uid);

  if (WiFi.status() == WL_CONNECTED) {
    HTTPClient http;
    http.begin(SERVER_URL);
    http.addHeader("Content-Type", "application/json");
    http.addHeader("x-api-key", DEVICE_KEY);

    // Option A: send card UID (server maps it to roll no via the "rfid" field)
    String body = "{\"uid\":\"" + uid + "\"}";
    // Option B: if your card/hardware already gives the roll no, send this instead:
    // String body = "{\"rollNo\":\"" + rollNo + "\"}";

    int code = http.POST(body);
    Serial.printf("Server: %d %s\n", code, http.getString().c_str());
    beep(code == 201 ? 1 : 3);   // 1 beep = added, 3 beeps = error/duplicate
    http.end();
  }

  rfid.PICC_HaltA();
  rfid.PCD_StopCrypto1();
  delay(1500); // avoid double reads of the same card
}

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <time.h>
#include <Wire.h>
#include <I2C_RTC.h>
#include <SPI.h>
#include <ArduinoJson.h>
#include <BME280I2C.h>
#include <AHT20.h>
#include "ScioSense_ENS160.h"  // ENS160 library
#include "SdFat.h"
#include <Adafruit_SGP30.h>
#include <PubSubClient.h>
#include <esp_task_wdt.h>
#include <bsec2.h>
#include <ESPping.h>

// -------------------------------------------------------------------------------------------------------------------------------
// MACRO DEFINITIONS
//--------------------------------------------------------------------------------------------------------------------------------

#define DISABLE_FS_H_WARNING  // Disable warning for type File not defined.
// SD_FAT_TYPE = 0 for SdFat/File as defined in SdFatConfig.h,
// 1 for FAT16/FAT32, 2 for exFAT, 3 for FAT16/FAT32 and exFAT.
#define SD_FAT_TYPE 3

// SDCARD_SS_PIN is defined for the built-in SD on some boards.
#ifndef SDCARD_SS_PIN
const uint8_t SD_CS_PIN = SS;
#else   // SDCARD_SS_PIN
// Assume built-in SD is used.
const uint8_t SD_CS_PIN = SDCARD_SS_PIN;
#endif  // SDCARD_SS_PIN

// Try max SPI clock for an SD. Reduce SPI_CLOCK if errors occur.
#define SPI_CLOCK SD_SCK_MHZ(10)

// Try to select the best SD card configuration.
#if defined(HAS_TEENSY_SDIO)
#define SD_CONFIG SdioConfig(FIFO_SDIO)
#elif defined(RP_CLK_GPIO) && defined(RP_CMD_GPIO) && defined(RP_DAT0_GPIO)
// See the Rp2040SdioSetup example for RP2040/RP2350 boards.
#define SD_CONFIG SdioConfig(RP_CLK_GPIO, RP_CMD_GPIO, RP_DAT0_GPIO)
#elif ENABLE_DEDICATED_SPI
#define SD_CONFIG SdSpiConfig(SD_CS_PIN, DEDICATED_SPI, SPI_CLOCK)
#else  // HAS_TEENSY_SDIO
#define SD_CONFIG SdSpiConfig(SD_CS_PIN, SHARED_SPI, SPI_CLOCK)
#endif  // HAS_TEENSY_SDIO

#define SDCARD_MANDATORY false

// -------------------------------------------------------------------------------------------------------------------------------
// TYPEDEFS
//--------------------------------------------------------------------------------------------------------------------------------

// temperature/pressure/altitude typedef

typedef struct BME680Sample_t {
  float iaq;
  float gasres;
  float temp;
  float pres;
  float hum;
  float eco2;
  float tvoc;
  uint8_t sampleCount;
};

typedef struct TphaMeas_t {
  float temperature;
  float pressure;
  float humidity;
  float altitude;
  float dew;
};

typedef struct AqtMeas_t {
  float aqi;
  float tvoc;
  float eco2;
  float gasRes;
};

typedef struct allMeas_t {
  TphaMeas_t tpha;
  AqtMeas_t aqt;
};

typedef enum SensorTypes_t {
  BME28_T,
  ENS160_T,
  AHT20_T,
  SGP30_T
};

// -------------------------------------------------------------------------------------------------------------------------------
// GLOBAL VARIABLES
//--------------------------------------------------------------------------------------------------------------------------------

// WIFI credentials
char *ssid = "pandurluknw";
char *password = "arm920TRam";
// global flag representing wifi status
boolean wifiConnected = false;
boolean mqttHostPresent = false;
// Mosquitto client
WiFiClientSecure mqttWifi;
PubSubClient mqttClient(mqttWifi);
boolean fetchMqttConfig = false;     // true if config.json needs to be fetched from mqtt
boolean configJsonReceived = false;  // true if configJson received on mqtt
String boardId;                      // holds an unique board ID. defined in config.json or generated at init
char *mqttUser = "esp_monitor";
char *mqttPass = "Admin99!";
char *rootCA = "-----BEGIN CERTIFICATE-----\nMIIGGzCCBAOgAwIBAgIUVGzvIeyTLXPnwJEMEujkXMfxOKYwDQYJKoZIhvcNAQEL\nBQAwgZQxCzAJBgNVBAYTAlJPMQswCQYDVQQIDAJOQTESMBAGA1UEBwwJQlVDSEFS\nRVNUMREwDwYDVQQKDAhFU1BfTVFUVDEUMBIGA1UECwwLRVNQX01PTklUT1IxFzAV\nBgNVBAMMDmJ1Y3NvbGNhLmdvLnJvMSIwIAYJKoZIhvcNAQkBFhNwYW5kdXJsdWtA\neWFob28uY29tMB4XDTI1MDMxMzExNDc0OFoXDTQ1MDMwODExNDc0OFowgZQxCzAJ\nBgNVBAYTAlJPMQswCQYDVQQIDAJOQTESMBAGA1UEBwwJQlVDSEFSRVNUMREwDwYD\nVQQKDAhFU1BfTVFUVDEUMBIGA1UECwwLRVNQX01PTklUT1IxFzAVBgNVBAMMDmJ1\nY3NvbGNhLmdvLnJvMSIwIAYJKoZIhvcNAQkBFhNwYW5kdXJsdWtAeWFob28uY29t\nMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAs6d7FU7T6sAjf8CkKjdC\nfWKlIBlQ1GMGiOg5jTAuud3R96/lhscPcCrV/j1DAAn+H8jh+QWO49YIQ721v+li\nD7Lz77xWK2FO7ekbxEIDXSnIDt2d0tF7pSONNR7MbXQPBNSqUqmRngZZPR1IIRbG\ngB6ie796ngVlp+UckJjIyF5clYZT4WJlnBF720Ylu2TmjNNaIF9iUC2iy59WgWS/\nnaYwRRsJgf/hDtcrdNGMb/xZ5Y4lmdZ0f10XA/C7Yh7FqR901qaPqtzIbY7ZHIZk\nUxQ4vqNfz5nrHvTnsbUIwI9fAAzl5fWEIRHLSqjiiqWGye8M2krCnMI+7F+pk022\np0H77h9j+dgI0+g9CK5rHE1FbXMKKhXxaTbCy/3ye0hiioDIC8Tare9IDFxZ1q3E\nrKyaQbWqee77+SUtVn1w+mXf6s+mnU13BWbUgXLqBXiaXIghgOmXaTuEJ8p2dp2U\nIqPFwxc9gbiWEEp0qt+KN2eSJflDBbTGeDlx5kDWVU3WpbXs3oDCUBQyBiLNZNxs\nBp9Wb66pThf0DCHEIQxgcEObRnq6wYC0wknIZdwdaldQ9VZajVQMlr86zV7nljnV\nquW8veddHVdX37phe2c9saEKo3FcfmZ6uvVgEzYjQ1dkENdVIqbIJGGkenXwupPL\nbI1wGtQb6+f0cxzO617x6QsCAwEAAaNjMGEwHQYDVR0OBBYEFJJpc84FCbKJZfJq\ndoF/RYoYEkapMB8GA1UdIwQYMBaAFJJpc84FCbKJZfJqdoF/RYoYEkapMA8GA1Ud\nEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMA0GCSqGSIb3DQEBCwUAA4ICAQBu\nx2PB/zKEIr0rjSlC6cIbQvGuPAz7yaVL3VM5JKmPOCKmIwrWke242iwjQYbacoTY\na4/S8O9h9SE3MCXbqJxsjrz4jKoB7DcFuuzGuSLOUWfFg4HxGtvb1qAGFPLHmHu/\nr+W3Qt9C9ZC4AY85CPjpwsJ1yfOXfffFdAm6BOE0Po+EQiJOZaPvWXvlWdyYjwWp\nL3MRAn1AdFFaGpLZ9fa8eEj26JO+R/rBBlH8YBGPBR/Uby2rwkORniD/Y7Uqslod\nykQv8YtsIbdpcds/0mcdJrToKMAe6AXf3G/vbP6+xRS3LNQj9KsJsb2QDaqi/YFk\nq57xip9cYEy27/aFCWCOCnFzbxSXzGo014qTrzEDAX47yVo+i1aQbLrPgcfAevpu\nCs92ZwmNAGZDhwtM2miDQNo4w+O+7mrgMHKiFYFhUm7CXS6ky4TwSqJgdEfY7Ntk\nwUOiOwGynbSEI5y/yOEh6xZqQfBpsb5cgBOai63i7aiig6RQzFDb1QvuasLJbU59\nwssyEf/sWqgQg8OEyNAt/3LLweJfiFL/n6Tjz36fJQo3LkImp7YncXGrmU5gjFr7\nEI1mUmz/H4ER81E8smo4huzbmBWduOwBIlmUf+R5xhatID/pwLRq3h3DdfGvDduG\nFOzi81qFT/KXLCSHb1ppbAfh+VDJdsqhcY11u+KB/Q==\n-----END CERTIFICATE-----\n";

// BME280 sensor (temperature, pressure, humidity)
BME280I2C::Settings settings1(
  BME280::OSR_X1,
  BME280::OSR_X1,
  BME280::OSR_X1,
  BME280::Mode_Forced,
  BME280::StandbyTime_1000ms,
  BME280::Filter_Off,
  BME280::SpiEnable_False,
  BME280I2C::I2CAddr_0x76  // I2C address. I2C specific.
);
BME280I2C::Settings settings2(
  BME280::OSR_X1,
  BME280::OSR_X1,
  BME280::OSR_X1,
  BME280::Mode_Forced,
  BME280::StandbyTime_1000ms,
  BME280::Filter_Off,
  BME280::SpiEnable_False,
  BME280I2C::I2CAddr_0x77  // I2C address. I2C specific.
);
BME280I2C bme280_1(settings1);
BME280I2C bme280_2(settings2);

// BME680 sensor
Bsec2 bme680_1;
Bsec2 bme680_2;
BME680Sample_t bme680Samples1;
BME680Sample_t bme680Samples2;

#define SAMPLE_RATE BSEC_SAMPLE_RATE_LP

// ENS160 & AHT21 sensor
ScioSense_ENS160 ens160_1(ENS160_I2CADDR_0);
ScioSense_ENS160 ens160_2(ENS160_I2CADDR_0);

// Date and time related data
long timezone = 0;
byte daysavetime = 0;
struct tm timeData;
static DS3231 RTC;  // external RTC

// AHT20 sensor
AHT20 aht20;

// SGP30 sensor
Adafruit_SGP30 sgp;

// JSON lib
#define MSG_BUFFER_SIZE (4096)
DynamicJsonDocument configJson(MSG_BUFFER_SIZE);

// SD-card config data
JsonObject sensors;
JsonObject dateTimeConfig;
JsonObject networking;
JsonObject locationInfo;
JsonObject logging;

float seaLevelComp = 0;

// SSD data
SdFs SD;
uint16_t samplesInd = 0;
uint16_t samplesBkInd = 0;

// Logging
boolean serialLogging = true;
boolean sdCardLogging = true;
char sdCardLog[4096];

// main loop timing
uint32_t loopCounter = 0;
#define SENSORS_SAMPLE_RATE 10

DynamicJsonDocument mqttDoc(MSG_BUFFER_SIZE);
String mqttCommand = "";

// -------------------------------------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------------------------------------


void LOG(String data) {
  if (serialLogging) {
    Serial.print(data.c_str());
  }
  if (sdCardLogging) {
    sprintf(sdCardLog, data.c_str());
  }
}

void LOGN(String data) {
  if (serialLogging) {
    Serial.println(data.c_str());
  }
  if (sdCardLogging) {
    sprintf(sdCardLog, data.c_str());
    sprintf(sdCardLog, "\n");
  }
}

void LOGF(String data, ...) {
  va_list argptr;
  va_start(argptr, data.c_str());
  if (serialLogging) {
    Serial.vprintf(data.c_str(), argptr);
  }
  if (sdCardLogging) {

    vsprintf(sdCardLog, data.c_str(), argptr);
    sprintf(sdCardLog, "\n");
  }
  va_end(argptr);
}

void handleError(String errorMessage) {
  LOGN(errorMessage);
  delay(1000);    // Afișează mesajul de eroare pentru 1 secunde
  esp_restart();  // Resetează dispozitivul ESP32
}


// Initialize serial interface for logging
void initSerial(uint32_t baud) {
  if (serialLogging) {
    Serial.begin(baud);
    delay(2000);  // wait for serial to be up
  }
  LOGN("Serial port enabled. Logging enabled");
}


void systemConfigInit() {
  if (!SDCARD_MANDATORY) {
    const char* config =  "{"
                    "  \"date-time\": {"
                    "    \"year\": 2025,"
                    "    \"month\": 2,"
                    "    \"day\": 14,"
                    "    \"hour\": 10,"
                    "    \"min\": 33,"
                    "    \"sec\": 22"
                    "  },"
                    "  \"logging\": {"
                    "    \"serialLogging\": true,"
                    "    \"sdCardLogging\": false"
                    "  },"
                    "  \"networking\": {"
                    "    \"wifi\": {"
                    "      \"ssid\": \"pandurluknw\","
                    "      \"password\": \"arm920TRam\""
                    "    },"
                    "    \"https-server\": {"
                    "      \"host-name:\": \"rpanduru.go.ro\","
                    "      \"port\": \"8443\","
                    "      \"username\": \"rpanduru\","
                    "      \"passwd\": \"testpass123\","
                    "      \"url-base\": \"/data/sensors\""
                    "    },"
                    "    \"mqtt\": {"
                    "      \"board-id\": \"monitor_0001\","
                    "      \"ip\": \"bucsolca.go.ro\","
                    "      \"port\": 8883,"
                    "      \"user\": \"esp_monitor\","
                    "      \"pass\": \"Admin99!\","
                    "      \"cacert\": \"-----BEGIN CERTIFICATE-----\\n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\\n-----END CERTIFICATE-----\\n\""
                    "    }"
                    "  },"
                    "  \"location\": {"
                    "    \"description\": \"Apartament Solca\","
                    "    \"city\": \"BUCURESTI FILARET\","
                    "    \"address\": \"Aleea Solca 3\","
                    "    \"sea-level\": 75,"
                    "    \"coordinates\": {"
                    "      \"latitude\": \"44.3993442\","
                    "      \"longitude\": \"26.0966582\","
                    "      \"maps\": \"https://maps.app.goo.gl/4Tk8Fv24hZu65vT1A\""
                    "    }"
                    "  },"
                    "  \"sensors\": {"
                    "    \"sensor-topology\": {"
                    "      \"BMP280\": 0,"
                    "      \"BME280\": 0,"
                    "      \"BME680\": \"2\","
                    "      \"ENS160\": 0,"
                    "      \"AHT20\": 0,"
                    "      \"SGP30\": 0"
                    "    }"
                    "  }"
                    "}";
    LOGN("Loading system config from memory: ");
    LOGN(String(config));
    DeserializationError error = deserializeJson(configJson, config);
    if (error) {
      handleError("Inconsistent System configuration in config.json. Please validate json format first (jsonlint.com)");
    } else {
      parseConfigJson(configJson);
    }
  }
}


void wdtInit() {
  esp_task_wdt_config_t wdt;
  wdt.timeout_ms = 20000;
  wdt.trigger_panic = true;
  wdt.idle_core_mask = 1;
  esp_task_wdt_deinit();
  esp_task_wdt_init(&wdt);
  esp_task_wdt_add(NULL);
}

// initialize Wifi
void wifiInit(const char *ssid, const char *password) {
  LOGN("Searching for WiFi network...");
  esp_task_wdt_reset();
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  LOG("Trying to connect to ");
  LOGN(ssid);
  uint8_t retry = 0;
  // try connecting to wifi 10 times. time-out between trials: 500ms
  while ((WiFi.status() != WL_CONNECTED) && (retry < 10)) {
    delay(500);
    LOG(".");
    esp_task_wdt_reset();
    retry++;
  }
  if (retry >= 10) {
    // could not connect to wifi
    LOGN("Wifi not connected. System working in offline mode");
    if (!SDCARD_MANDATORY) {
      handleError("ERROR: when SD-card is not mandatory, Wifi connection is mandatory. Please properly configure wifi connection. Resetting system...");
    }
    return;
  }
  LOG("Connected to wifi. IP address: ");
  LOGN(WiFi.localIP().toString());
  wifiConnected = true;
}


// Time and date init
void dateAndTimeInit(void) {
  esp_task_wdt_reset();
  if (wifiConnected) {
    LOGN("Contacting NTP Time Server...");
    configTime(3600 * timezone, daysavetime * 3600, "time.nist.gov", "0.pool.ntp.org", "1.pool.ntp.org");
    // delay(2000);
    timeData.tm_year = 0;
    getLocalTime(&timeData, 5000);
    LOGF("\nFetched time is : %d-%02d-%02d %02d:%02d:%02d\n", (timeData.tm_year) + 1900, (timeData.tm_mon) + 1, timeData.tm_mday, timeData.tm_hour, timeData.tm_min, timeData.tm_sec);
  } else {
    LOGN("Skipping NTP time reading since wifi is disconnected...");
  }
}


// initialize external RTC
void RTCInit() {
  LOGN("Initializing RTC...");
  esp_task_wdt_reset();
  if (RTC.begin()) {
    RTC.setHourMode(CLOCK_H24);

    // If wifi is connected, we have fresh NTP server data. update RTC with date/time
    if (wifiConnected) {
      LOGN("Updating RTC registers with proper time and date from NTP server...");
      LOGF("\Set time is : %d-%02d-%02d %02d:%02d:%02d\n", (timeData.tm_year) + 1900, (timeData.tm_mon) + 1, timeData.tm_mday, timeData.tm_hour, timeData.tm_min, timeData.tm_sec);
      do {
        RTC.setYear((uint16_t)timeData.tm_year + 1900);
        RTC.setMonth((uint8_t)timeData.tm_mon + 1);
        RTC.setDay((uint8_t)timeData.tm_mday);
        RTC.setHours((uint8_t)timeData.tm_hour);
        RTC.setMinutes((uint8_t)timeData.tm_min);
        RTC.setSeconds((uint8_t)timeData.tm_sec);
      } while (RTC.getYear() <= 2024);
    } else {
      // Check if RTC was previously set via NTP server
      int16_t yearDiff = dateTimeConfig["year"].as<uint16_t>() - RTC.getYear();
      if (yearDiff > 0) {
        LOGN("Wifi not connected and RTC was not previously set from NTP server. Using date-time from system configuration (config.json)");
        do {
          RTC.setYear(dateTimeConfig["year"].as<uint16_t>());
          RTC.setMonth(dateTimeConfig["month"].as<uint8_t>());
          RTC.setDay(dateTimeConfig["day"].as<uint8_t>());
          RTC.setHours(dateTimeConfig["hour"].as<uint8_t>());
          RTC.setMinutes(dateTimeConfig["min"].as<uint8_t>());
          RTC.setSeconds(dateTimeConfig["sec"].as<uint8_t>());
        } while (RTC.getYear() <= 2024);
      } else {
        LOGN("Wifi not connected but RTC was previously set. Skipping RTC update");
      }
    }
  } else {
    handleError("ERROR: RTC not found. RTC is mandatory to be able to log sample timestamps. Restarting the system...");
  }
}


// SD card initialization
void SDCardInit() {
  LOGN("Initializing SD card...");
  esp_task_wdt_reset();
  // Initialize the SD.
  if (!SD.begin(SD_CONFIG)) {
    if (SDCARD_MANDATORY) {
      handleError("ERROR: Card Mount Failed. Restarting the system...");
    } else {
      LOGN("SD-card not present, but the system is configured to work without a SD-card (dump data over mqtt)");
    }
  } else {
    LOGN("SD card initialized successfully");
  }
}


// parse config.json and update global parameters
void parseConfigJson(DynamicJsonDocument &cfg) {
  // System initialization parameters - wifi settings
  networking = cfg["networking"];
  String nw;
  if (networking) {
    ssid = (char *)networking["wifi"]["ssid"].as<const char *>();
    password = (char *)networking["wifi"]["password"].as<const char *>();
    mqttUser = (char *)networking["mqtt"]["user"].as<const char *>();
    mqttPass = (char *)networking["mqtt"]["pass"].as<const char *>();
    rootCA = (char *)networking["mqtt"]["cacert"].as<const char *>();
    boardId = (char *)networking["mqtt"]["board-id"].as<const char *>();
  }
  serializeJson(networking, nw);
  LOG("Networking configuration: ");
  LOGN(nw);

  // Get sensors config
  sensors = cfg["sensors"];
  String sn;
  serializeJson(sensors, sn);
  LOG("Sensors config: ");
  LOGN(sn);

  dateTimeConfig = cfg["date-time"];
  String dt;
  serializeJson(dateTimeConfig, dt);
  LOG("Date and time read from config: ");
  LOGN(dt);

  locationInfo = cfg["location"];
  seaLevelComp = locationInfo["sea-level"];
  LOG("Location information: ");
  String loc;
  serializeJson(locationInfo, loc);
  LOGN(loc);

  logging = cfg["logging"];
  serialLogging = logging["serialLogging"];
  sdCardLogging = logging["sdCardLogging"];
  LOG("Logging information: ");
  String log;
  serializeJson(logging, log);
  LOGN(log);
  LOGN("Successfully read system configuration from file");
}


// get the system configuration froom mosquitto protocol and save it to
void getSystemConfigFromMqtt() {
  LOGN("Received system configuration (config.json) from mqtt");
  parseConfigJson(configJson);
  LOGN("Writting system configuration to SD card");
  String config;
  serializeJsonPretty(configJson, config);
  if (SDCARD_MANDATORY) {
    writeFile("config.json", config.c_str());
  }
  configJsonReceived = true;
}


// Read system configuration from file
void readSystemConfig() {
  LOGN("Reading system configuration from SD card");
  esp_task_wdt_reset();
  if (!SDCARD_MANDATORY) {
    // System configuration loaded from memory
    return;
  }
  char config[4096];
  if (SD.exists("config.json")) {
    LOGN("System config file (config.json) found on SD card");
  } else {
    LOGN("config.json file is missing from SD-card. Initiate fetch config from MQTT");
    fetchMqttConfig = true;
    configJsonReceived = false;
    return;
  }
  readFile("config.json", config);
  DeserializationError error = deserializeJson(configJson, config);
  if (error) {
    handleError("Inconsistent System configuration in config.json. Please validate json format first (jsonlint.com)");
  } else {
    parseConfigJson(configJson);
  }
}



// ENS160 Init function
void ENS160Init(ScioSense_ENS160 &dev) {
  LOGN("Initializing ENS160 sensor...");
  esp_task_wdt_reset();
  dev.begin();
  if (dev.available()) {
    // Print ENS160 versions
    LOG("\tRev: ");
    Serial.print(dev.getMajorRev());
    LOG(".");
    Serial.print(dev.getMinorRev());
    LOG(".");
    Serial.println(dev.getBuild());

    LOG("\tStandard mode ");
    if (dev.setMode(ENS160_OPMODE_STD)) {
      LOGN("ENS160 sensor Initialization done");
    } else {
      handleError("ERROR: ENS160 Sensor initialization failed. Restarting the system...");
    }
  } else {
    handleError("ERROR: ENS160 Sensor initialization failed. Restarting the system...");
  }
}

// BME280 Init function
void BME280Init(BME280I2C &dev) {
  LOGN("Initializing BMP280/BME280 Sensor...");
  esp_task_wdt_reset();
  if (dev.begin()) {
    switch (dev.chipModel()) {
      case BME280::ChipModel_BME280:
        LOGN("Found BME280 sensor! Success.");
        break;
      case BME280::ChipModel_BMP280:
        LOGN("Found BMP280 sensor! No Humidity available.");
        break;
      default:
        LOGN("Found UNKNOWN sensor! Error!");
    }
  } else {
    handleError("ERROR: Could not initialize BMP280/BME280 sensor. Restarting system...");
  }
}


// BME680 init function
void BME680Init(Bsec2 &dev, uint8_t address) {
  uint8_t retry = 0;
  esp_task_wdt_reset();
  LOGN("Initializing BME680 sensor...");

  bsecSensor sensorList[] = {
    BSEC_OUTPUT_IAQ,
    BSEC_OUTPUT_RAW_PRESSURE,
    BSEC_OUTPUT_RAW_GAS,
    BSEC_OUTPUT_SENSOR_HEAT_COMPENSATED_TEMPERATURE,
    BSEC_OUTPUT_SENSOR_HEAT_COMPENSATED_HUMIDITY,
    BSEC_OUTPUT_CO2_EQUIVALENT,
    BSEC_OUTPUT_BREATH_VOC_EQUIVALENT
  };

  while (!dev.begin(address, Wire) && (retry < 16)) {
    retry++;
    checkBsecStatus(dev);
    esp_task_wdt_reset();
    delay(2000);
  }
  if (retry >= 16) {
    handleError("BME680 sensor configured but the sensor is not connected. Restarting system");
  } else {
    dev.setTemperatureOffset(TEMP_OFFSET_LP);
    if (!dev.updateSubscription(sensorList, ARRAY_LEN(sensorList), SAMPLE_RATE)) {
      checkBsecStatus(dev);
    }
    if (address == 0x76) {
      bme680Samples1.sampleCount = 0;
      bme680Samples1.eco2 = 0;
      bme680Samples1.gasres = 0;
      bme680Samples1.iaq = 0;
      bme680Samples1.tvoc = 0;
      bme680Samples1.temp = 0;
      bme680Samples1.hum = 0;
      dev.attachCallback(bme680Callback1);
    } else {
      bme680Samples2.sampleCount = 0;
      bme680Samples2.eco2 = 0;
      bme680Samples2.gasres = 0;
      bme680Samples2.iaq = 0;
      bme680Samples2.tvoc = 0;
      bme680Samples2.temp = 0;
      bme680Samples2.hum = 0;
      dev.attachCallback(bme680Callback2);
    }
  }
}

void AHT20Init() {
  LOGN("Initializing AHT20 sensor...");
  esp_task_wdt_reset();
  if (!aht20.begin()) {
    handleError("ERROR: Could not initialize AHT20 sensor. Restarting the system");
    return;
  }
  LOGN("AHT20 sensor initialized");
}


// SGP30 init
void SGP30Init() {
  LOGN("Initializing SGP20 sensor...");
  esp_task_wdt_reset();
  if (!sgp.begin()) {
    handleError("ERROR: Could not initialize SGP30 sensor. Check connections. Restarting system...");
  }
  sgp.IAQinit();
  LOGN("SGP30 successfully initialized");
}


// Mosquitto initialization
void mqttInit(PubSubClient &client, const char *server, uint16_t port) {
  // MQTT SETUP
  LOG("Initializing Mosquitto client to connect to ");
  esp_task_wdt_reset();
  LOG(server);
  LOG(" port ");
  LOGN(String(port));
  if (Ping.ping(server, 1)) {
    mqttHostPresent = true;
    LOGN("MQTT host found");
  } else {
    mqttHostPresent = false;
    LOGN("MQTT host not found");
  }
  mqttWifi.setTimeout(1000);
  mqttWifi.setCACert(rootCA);
  client.setServer(server, port);
  client.setCallback(mqttCallback);
  client.setBufferSize(MSG_BUFFER_SIZE);
  client.setKeepAlive(60000);
  LOGN("Initializing Mosquitto done");
}


void getMqttConfigFromSerial(String &server, uint16_t &port) {
  LOGN("Please input Mosquitto server IP address (X.X.X.X):");
  while (Serial.available() == 0) {
    delay(100);
    esp_task_wdt_reset();
  }
  server = Serial.readStringUntil('\n');
  LOGN("Please input Mosquitto port (default is 1883): ");
  while (Serial.available() == 0) {
    delay(100);
    esp_task_wdt_reset();
  }
  port = Serial.readStringUntil('\n').toInt();
}


// ESP32 board initialization function
void setup() {
  // wdtInit
  wdtInit();

  // serial logging
  initSerial(115200);

  // initalize system config from memory
  systemConfigInit();

  // send reset reason
  if (ESP_RST_TASK_WDT == esp_reset_reason()) {
    LOGN("Warning: the board was previously reset by watchdog");
  }

  // I2C init
  Wire.begin();

  // SD cart initialization
  SDCardInit();

  // System parameter initialization
  readSystemConfig();

  // if system config is NULL, retrieve config from mqtt
  if ((networking == NULL) || (sensors == NULL) || (dateTimeConfig == NULL) || (locationInfo == NULL)) {
    fetchMqttConfig = true;
  }

  // init WiFi
  wifiInit(ssid, password);

  // if system configuration needs to be fetched from MQTT, provide ip/port on Serial
  String server;
  uint16_t port = 8883;
  if (fetchMqttConfig) {
    getMqttConfigFromSerial(server, port);
    mqttInit(mqttClient, server.c_str(), port);

    // generate a board id based on local ip address
    boardId = String("monitor_") + String(WiFi.localIP().toString());

    while (!mqttReconnect(mqttClient)) {
      delay(500);
      esp_task_wdt_reset();
    }
    LOGN("Waiting for receiving config.json via mqtt");
    while (!configJsonReceived) {
      LOG(".");
      mqttClient.loop();
      delay(1000);
      esp_task_wdt_reset();
    }
    fetchMqttConfig = false;
    configJsonReceived = false;
  } else {
    // MQTT init from SD card config.json
    boardId = networking["mqtt"]["board-id"].as<String>();
    mqttInit(mqttClient, networking["mqtt"]["ip"].as<const char *>(), networking["mqtt"]["port"].as<uint16_t>());
  }

  // real time initialization
  dateAndTimeInit();

  // RTC initialization
  RTCInit();

  // Sensors init, based on sensors topology from config.json
  JsonObject sensorTopology = sensors["sensor-topology"];
  if ((sensorTopology["BMP280"].as<uint8_t>() > 0) || (sensorTopology["BME280"].as<uint8_t>() > 0)) {
    BME280Init(bme280_1);
  }
  if ((sensorTopology["BMP280"].as<uint8_t>() > 1) || (sensorTopology["BME280"].as<uint8_t>() > 1) || (sensorTopology["BMP280"].as<uint8_t>() + sensorTopology["BME280"].as<uint8_t>() >= 2)) {
    BME280Init(bme280_2);
  }
  if ((sensorTopology["BME680"].as<uint8_t>() > 0)) {
    BME680Init(bme680_1, 0x76);
  }
  if ((sensorTopology["BME680"].as<uint8_t>() > 1)) {
    BME680Init(bme680_2, 0x77);
  }
  if (sensorTopology["ENS160"].as<uint8_t>() > 0) {
    ENS160Init(ens160_1);
  }
  if (sensorTopology["ENS160"].as<uint8_t>() > 1) {
    ENS160Init(ens160_2);
  }
  if (sensorTopology["AHT20"].as<uint8_t>() > 0) {
    AHT20Init();
  }
  if (sensorTopology["SGP30"].as<uint8_t>() > 0) {
    SGP30Init();
  }
  LOGN("Initialization process finished.");
  esp_task_wdt_reset();

  if (sdCardLogging) {
    appendFile("debug.log", sdCardLog);
  }

  loopCounter = RTC.getEpoch(true);
}


void bme680Callback1(const bme68xData data, const bsecOutputs outputs, Bsec2 bsec) {
  if (!outputs.nOutputs) {
    return;
  }

  for (uint8_t i = 0; i < outputs.nOutputs; i++) {
    const bsecData output = outputs.output[i];
    switch (output.sensor_id) {
      case BSEC_OUTPUT_IAQ:
        bme680Samples1.iaq += output.signal;
        break;
      case BSEC_OUTPUT_RAW_GAS:
        bme680Samples1.gasres += output.signal;
        break;
      case BSEC_OUTPUT_RAW_PRESSURE:
        bme680Samples1.pres += output.signal;
        break;
      case BSEC_OUTPUT_SENSOR_HEAT_COMPENSATED_TEMPERATURE:
        bme680Samples1.temp += output.signal;
        break;
      case BSEC_OUTPUT_SENSOR_HEAT_COMPENSATED_HUMIDITY:
        bme680Samples1.hum += output.signal;
        break;
      case BSEC_OUTPUT_CO2_EQUIVALENT:
        bme680Samples1.eco2 += output.signal;
        break;
      case BSEC_OUTPUT_BREATH_VOC_EQUIVALENT:
        bme680Samples1.tvoc += output.signal;
        break;
      default:
        break;
    }
  }
  bme680Samples1.sampleCount++;
}

void bme680Callback2(const bme68xData data, const bsecOutputs outputs, Bsec2 bsec) {
  if (!outputs.nOutputs) {
    return;
  }

  for (uint8_t i = 0; i < outputs.nOutputs; i++) {
    const bsecData output = outputs.output[i];
    switch (output.sensor_id) {
      case BSEC_OUTPUT_IAQ:
        bme680Samples2.iaq += output.signal;
        break;
      case BSEC_OUTPUT_RAW_GAS:
        bme680Samples2.gasres += output.signal;
        break;
      case BSEC_OUTPUT_RAW_PRESSURE:
        bme680Samples2.pres += output.signal;
        break;
      case BSEC_OUTPUT_SENSOR_HEAT_COMPENSATED_TEMPERATURE:
        bme680Samples2.temp += output.signal;
        break;
      case BSEC_OUTPUT_SENSOR_HEAT_COMPENSATED_HUMIDITY:
        bme680Samples2.hum += output.signal;
        break;
      case BSEC_OUTPUT_CO2_EQUIVALENT:
        bme680Samples2.eco2 += output.signal;
        break;
      case BSEC_OUTPUT_BREATH_VOC_EQUIVALENT:
        bme680Samples2.tvoc += output.signal;
        break;
      default:
        break;
    }
  }
  bme680Samples2.sampleCount++;
}


void checkBsecStatus(Bsec2 bsec) {
  if (bsec.status < BSEC_OK) {
    LOGN("BSEC error code : " + String(bsec.status));
  } else if (bsec.status > BSEC_OK) {
    LOGN("BSEC warning code : " + String(bsec.status));
  }

  if (bsec.sensor.status < BME68X_OK) {
    LOGN("BME68X error code : " + String(bsec.sensor.status));
  } else if (bsec.sensor.status > BME68X_OK) {
    LOGN("BME68X warning code : " + String(bsec.sensor.status));
  }
}


TphaMeas_t newTpha() {
  TphaMeas_t data;
  data.temperature = 0;
  data.pressure = 0;
  data.humidity = 0;
  data.altitude = 0;
  data.dew = 0;
  return data;
}

AqtMeas_t newAqt() {
  AqtMeas_t data;
  data.aqi = 0;
  data.eco2 = 0;
  data.tvoc = 0;
  data.gasRes = 0;
  return data;
}


// BMP280 / BME280 sensors samples reading
TphaMeas_t getBME280Samples(BME280I2C dev) {
  LOGN("Reading BME280 sensor measurements...");
  TphaMeas_t res = newTpha();

  BME280::TempUnit tempUnit(BME280::TempUnit_Celsius);
  BME280::PresUnit presUnit(BME280::PresUnit_hPa);

  dev.read(res.pressure, res.temperature, res.humidity, tempUnit, presUnit);

  LOG("\tTemp: ");
  LOG(String(res.temperature));
  LOG("°" + String(tempUnit == BME280::TempUnit_Celsius ? 'C' : 'F'));
  LOG("\t\tHumidity: ");
  LOG(String(res.humidity));
  LOG("% RH");
  LOG("\t\tPressure: ");
  LOG(String(res.pressure));
  LOGN("hPa");
  return res;
}


// ENS160 sensors samples reading
AqtMeas_t getENS160Samples(ScioSense_ENS160 dev) {
  AqtMeas_t res = newAqt();
  if (dev.available()) {
    LOGN("Fetching air quality measurements from ENS160");
    dev.measure(true);
    delay(1000);
    res.aqi = dev.getAQI();
    res.tvoc = dev.getTVOC();
    res.eco2 = dev.geteCO2();
    LOG("\tAQI: ");
    LOG(String(res.aqi));
    LOG("\tTVOC: ");
    LOG(String(res.tvoc));
    LOG("ppb\teCO2: ");
    LOGN(String(res.eco2));
  }
  return res;
}


// AHT20 sensor data
TphaMeas_t getAHT20Samples(AHT20 dev) {
  TphaMeas_t res = newTpha();
  res.temperature = dev.getTemperature();
  res.humidity = dev.getHumidity();
  return res;
}


// SGP30 sensor data
AqtMeas_t getSGP20Samples(Adafruit_SGP30 dev) {
  AqtMeas_t res = newAqt();
  if (!dev.IAQmeasure()) {
    LOGN("Error getting SGP30 sensor measurements");
    return res;
  }
  res.eco2 = dev.eCO2;
  res.tvoc = dev.TVOC;
  return res;
}


// Get BME680 samples
allMeas_t getBME680Samples(BME680Sample_t &samples) {
  allMeas_t res;
  float seaLevel;
  res.aqt = newAqt();
  res.tpha = newTpha();

  res.tpha.temperature = samples.temp / samples.sampleCount;
  res.tpha.pressure = samples.pres / samples.sampleCount;
  res.tpha.humidity = samples.hum / samples.sampleCount;
  res.tpha.dew = res.tpha.temperature - ((100 - res.tpha.humidity) / 5.0);

  res.aqt.gasRes = samples.gasres / samples.sampleCount;
  res.aqt.aqi = samples.iaq / samples.sampleCount;
  res.aqt.eco2 = samples.eco2 / samples.sampleCount;
  res.aqt.tvoc = samples.tvoc / samples.sampleCount;

  samples.tvoc = 0;
  samples.eco2 = 0;
  samples.gasres = 0;
  samples.hum = 0;
  samples.iaq = 0;
  samples.pres = 0;
  samples.temp = 0;
  samples.sampleCount = 0;

  return res;
}


// Get all sensors data
void getAllSensorsData(JsonObject sensorTopology, JsonObject &indoor, JsonObject &outdoor) {
  // BME280 Sensors data
  esp_task_wdt_reset();
  if ((sensorTopology["BMP280"].as<uint8_t>() > 0) || (sensorTopology["BME280"].as<uint8_t>() > 0)) {
    TphaMeas_t bme280TPH = getBME280Samples(bme280_1);
    indoor["temp"] = bme280TPH.temperature;
    indoor["pres"] = bme280TPH.pressure;
    if (sensorTopology["BME280"] >= 1) {
      indoor["hum"] = bme280TPH.humidity;
    }
  }
  // BME280 Sensor data
  esp_task_wdt_reset();
  if ((sensorTopology["BMP280"].as<uint8_t>() > 1) || (sensorTopology["BME280"].as<uint8_t>() > 1) || (sensorTopology["BMP280"].as<uint8_t>() + sensorTopology["BME280"].as<uint8_t>() >= 2)) {
    TphaMeas_t bme280TPH = getBME280Samples(bme280_2);
    outdoor["temp"] = bme280TPH.temperature;
    outdoor["pres"] = bme280TPH.pressure;
    if (sensorTopology["BME280"] >= 1) {
      outdoor["hum"] = bme280TPH.humidity;
    }
  }

  // BME680 Sensor data
  esp_task_wdt_reset();
  if ((sensorTopology["BME680"].as<uint8_t>() > 0)) {
    allMeas_t data = getBME680Samples(bme680Samples1);
    indoor["temp"] = data.tpha.temperature;
    indoor["pres"] = data.tpha.pressure;
    indoor["hum"] = data.tpha.humidity;
    indoor["dew"] = data.tpha.dew;
    indoor["aqi"] = data.aqt.aqi;
    indoor["eco2"] = data.aqt.eco2;
    indoor["tvoc"] = data.aqt.tvoc;
    indoor["gasres"] = data.aqt.gasRes;
  }
  esp_task_wdt_reset();
  if ((sensorTopology["BME680"].as<uint8_t>() > 1)) {
    allMeas_t data = getBME680Samples(bme680Samples2);
    outdoor["temp"] = data.tpha.temperature;
    outdoor["pres"] = data.tpha.pressure;
    outdoor["hum"] = data.tpha.humidity;
    outdoor["dew"] = data.tpha.dew;
    outdoor["aqi"] = data.aqt.aqi;
    outdoor["eco2"] = data.aqt.eco2;
    outdoor["tvoc"] = data.aqt.tvoc;
    outdoor["gasres"] = data.aqt.gasRes;
  }
  esp_task_wdt_reset();
  // AHT280 Sensor data
  if (sensorTopology["AHT20"].as<uint8_t>() > 0) {
    TphaMeas_t ahtM = getAHT20Samples(aht20);
    indoor["hum"] = ahtM.humidity;
  }
  esp_task_wdt_reset();
  // ENS160 sensor data
  if (sensorTopology["ENS160"].as<uint8_t>() > 0) {
    AqtMeas_t aqtM = getENS160Samples(ens160_1);
    indoor["aqi"] = aqtM.aqi;
    indoor["eco2"] = aqtM.eco2;
    indoor["tvoc"] = aqtM.tvoc;
  }
  esp_task_wdt_reset();
  // ENS160 sensor data
  if (sensorTopology["ENS160"].as<uint8_t>() > 1) {
    AqtMeas_t aqtM = getENS160Samples(ens160_2);
    outdoor["aqi"] = aqtM.aqi;
    outdoor["eco2"] = aqtM.eco2;
    outdoor["tvoc"] = aqtM.tvoc;
  }
  esp_task_wdt_reset();
  // SGP30 sensor data
  if (sensorTopology["SGP30"].as<uint8_t>() > 0) {
    AqtMeas_t aqtM = getSGP20Samples(sgp);
    indoor["eCO2"] = aqtM.eco2;
    indoor["tvoc"] = aqtM.tvoc;
  }
}


// wifi reconnect function
void wifiReconnect(void) {
  if (!wifiConnected) {
    // try a new wifi connection
    esp_task_wdt_reset();
    if (WiFi.waitForConnectResult(1000) == WL_CONNECTED) {
      LOGN(String("Reconnected to wifi. IP: ") + WiFi.localIP().toString());
      wifiConnected = true;
    }
  } else {
    // Wifi is connected
    // Check if wifi connection is still present
    if (WiFi.waitForConnectResult() != WL_CONNECTED) {
      wifiConnected = false;
      LOGN("Disconnected from wifi. Working in offline mode.");
    }
  }
}


// SD-card related functions
uint32_t readFile(const char *path, char *data) {
  LOGF("Reading file: %s\n", path);
  uint8_t retry = 0;
  uint32_t len = 0;

  File file;
  do {
    esp_task_wdt_reset();
    file = SD.open(path, FILE_READ);
    retry++;
  } while (!file && (retry < 32));

  if (retry < 32) {
    while (file.available()) {
      data[len] = file.read();
      len++;
    }
    file.close();
    LOGN("Done reading file");
  } else {
    handleError("Could not open the file for read. restarting system");
  }
  return len;
}

void writeFile(const char *path, const char *message) {
  LOGF("Writting to file: %s\n", path);
  uint8_t retry = 0;

  File file;
  do {
    esp_task_wdt_reset();
    file = SD.open(path, (O_WRONLY | O_CREAT));
    retry++;
  } while (!file && (retry < 32));

  if (retry < 32) {
    retry = 0;
    while (!file.print(message) && (retry < 32)) {
      esp_task_wdt_reset();
      retry++;
    }
    if (retry >= 32) {
      handleError("Write message failed");
    } else {
      LOGN("Done writting file");
    }
    file.close();
  } else {
    handleError("Could not open the file for write. resetting system");
  }
}

void appendFile(const char *path, const char *message) {
  LOGF("Appending to file: %s", path);
  LOGN("");
  uint8_t retry = 0;

  File file;
  do {
    esp_task_wdt_reset();
    file = SD.open(path, FILE_WRITE);
    retry++;
  } while (!file && (retry < 32));

  if (retry < 32) {
    retry = 0;
    while (!file.print(message) && (retry < 32)) {
      esp_task_wdt_reset();
      retry++;
    }
    if (retry >= 32) {
      handleError("Append message failed. Restarting system");
    } else {
      LOGN("Done appending file");
    }
    file.close();
  } else {
    handleError("Could not open the file for append. restarting system");
  }
}


// Get time and date from RTC
struct tm getRTCDateTime(DS3231 &dev) {
  struct tm res;
  res.tm_year = RTC.getYear();
  res.tm_mon = RTC.getMonth();
  res.tm_mday = RTC.getDay();
  res.tm_hour = RTC.getHours();
  res.tm_min = RTC.getMinutes();
  res.tm_sec = RTC.getSeconds();
  return res;
}


// Format timestamp for logging
String timeStamp() {
  // Get time data
  struct tm time = getRTCDateTime(RTC);
  char ret[32];
  sprintf(ret, "%d-%02d-%02dT%02d:%02d:%02dZ", time.tm_year, time.tm_mon, time.tm_mday, time.tm_hour, time.tm_min, time.tm_sec);
  return String(ret);
}


// Serialize sensors data for lgging
String serializeSensorsData(JsonObject indoor, JsonObject outdoor) {
  JsonDocument samples;
  if (indoor.size() > 0) {
    samples["indoor"] = indoor;
  }
  if (outdoor.size() > 0) {
    samples["outdoor"] = outdoor;
  }
  String serialized;
  serializeJson(samples, serialized);
  return serialized;
}

// Mosquitto reconnect
boolean mqttReconnect(PubSubClient &client) {
  boolean connected = false;
  // Loop until we're reconnected
  client.setSocketTimeout(1000);
  if (wifiConnected && mqttHostPresent && !client.connected()) {
    // LOGN("Attempting MQTT connection...");
    // Attempt to connect
    esp_task_wdt_reset();
    if (client.connect(boardId.c_str(), mqttUser, mqttPass)) {
      // Serial.println("connected");
      client.subscribe("monitors/in");
      LOGN("Successfully connected to MQTT");
      connected = true;
    } else {
      // LOGN("Mosquitto server is offline");
    }
  } else if (client.connected()) {
    connected = true;
  }
  return connected;
}

void populateMeasurements(JsonArray &indoor, JsonArray &outdoor) {
  if (sensors["sensor-topology"]["BMP280"].as<uint8_t>() > 0) {
    indoor.add("temp");
    indoor.add("pres");
  }
  if (sensors["sensor-topology"]["BMP280"].as<uint8_t>() > 1) {
    outdoor.add("temp");
    outdoor.add("pres");
  }
  if (sensors["sensor-topology"]["BME280"].as<uint8_t>() > 0) {
    indoor.add("temp");
    indoor.add("pres");
    indoor.add("hum");
  }
  if ((sensors["sensor-topology"]["BME280"].as<uint8_t>() > 1) || ((sensors["sensor-topology"]["BMP280"].as<uint8_t>() > 0) && sensors["sensor-topology"]["BME280"].as<uint8_t>() > 0)) {
    outdoor.add("temp");
    outdoor.add("pres");
    outdoor.add("hum");
  }
  if (sensors["sensor-topology"]["BME680"].as<uint8_t>() > 0) {
    indoor.add("temp");
    indoor.add("pres");
    indoor.add("hum");
    indoor.add("dew");
    indoor.add("aqi");
    indoor.add("eco2");
    indoor.add("tvoc");
    indoor.add("gasres");
  }
  if (sensors["sensor-topology"]["BME680"].as<uint8_t>() > 1) {
    outdoor.add("temp");
    outdoor.add("pres");
    outdoor.add("hum");
    outdoor.add("dew");
    outdoor.add("aqi");
    outdoor.add("eco2");
    outdoor.add("tvoc");
    outdoor.add("gasres");
  }
  if (sensors["sensor-topology"]["ENS160"].as<uint8_t>() > 0) {
    indoor.add("aqi");
    indoor.add("eco2");
    indoor.add("tvoc");
  }
  if (sensors["sensor-topology"]["ENS160"].as<uint8_t>() > 1) {
    outdoor.add("aqi");
    outdoor.add("eco2");
    outdoor.add("tvoc");
  }
  if (sensors["sensor-topology"]["AHT20"].as<uint8_t>() > 0) {
    indoor.add("temp");
    indoor.add("hum");
  }
  if (sensors["sensor-topology"]["AHT20"].as<uint8_t>() > 1) {
    outdoor.add("temp");
    outdoor.add("hum");
  }
  if (sensors["sensor-topology"]["SGP30"].as<uint8_t>() > 0) {
    indoor.add("eco2");
    indoor.add("tvoc");
  }
  if (sensors["sensor-topology"]["SGP30"].as<uint8_t>() > 0) {
    outdoor.add("eco2");
    outdoor.add("tvoc");
  }
}

// Handle system commands received on mqtt
void handleSystemCommand(String cmd, DynamicJsonDocument doc) {
  esp_task_wdt_reset();
  // system_config command: fetch config.json content via MQTT and save it to SD-card
  if (String("monitor_config").equals(cmd)) {
    LOGN("Handling monitor_config command...");
    String config;
    serializeJson(doc["payload"], config);
    deserializeJson(configJson, config);
    getSystemConfigFromMqtt();
    if ((networking == NULL) || (sensors == NULL) || (dateTimeConfig == NULL) || (locationInfo == NULL)) {
      handleError("There was an issue getting system configuration on MQTT. resetting the system");
    }
  }

  // get_location command: returns location information of this monitor
  if (String("monitor_info").equals(cmd)) {
    LOGN("Handling monitor_info command");
    DynamicJsonDocument info(1024);
    info["board-id"] = boardId;
    info["location"] = locationInfo;
    JsonArray indoor = info.createNestedArray("indoor");
    JsonArray outdoor = info.createNestedArray("outdoor");
    populateMeasurements(indoor, outdoor);
    String payload;
    serializeJson(info, payload);
    LOGN(String("payload to be sent: ") + payload);
    LOGF("Payload size: %d", payload.length());
    mqttClient.publish("monitors/info", payload.c_str());
  }

  // get_sdsamples: reads samples from SD-card and send them on MQTT
  if (String("monitor_log").equals(cmd)) {
    char data[512];
    File file;
    uint8_t retry = 0;
    do {
      file = SD.open("samples.log", FILE_READ);
      retry++;
    } while (!file && (retry < 32));

    if (retry < 32) {
      int len = 0;
      while (file.available()) {
        esp_task_wdt_reset();
        data[len] = file.read();
        if (data[len] == '\n') {
          char data2[512];
          for (int i = 0; i < 512; i++) {
            data2[i] = 0;
          }
          for (int i = 0; i < len; i++) {
            data2[i] = data[i];
          }
          LOGN(String("read from file: ") + String(data2));
          mqttClient.publish("monitors/out", data2);
          for (int i = 0; i < len; i++) {
            data[i] = 0;
          }
          len = 0;
        } else {
          len++;
        }
      }
      file.close();
    }
  }

  // get_sdsamples: reads samples from SD-card and send them on MQTT
  if (String("erase_log").equals(cmd)) {
    LOGN("Handling erase_sensor_logs command");
    SD.remove("samples.log");
  }
}


// Mosquitto client callbaclk read function
void mqttCallback(char *topic, byte *payload, unsigned int length) {
  LOGN(String("MQTT: Received new message on ") + String(topic));
  DynamicJsonDocument doc(length);
  DeserializationError error = deserializeJson(doc, payload);
  if (error) {
    LOGN("Invalid JSON format received on MQTT. Dropping the message");
  } else {
    LOG("MQTT message is for board id: ");
    LOGN(doc["board-id"].as<String>());
    LOG("Self board id: ");
    LOGN(boardId);
    if (boardId.equals(doc["board-id"].as<String>())) {
      LOGN(String("Received command: ") + doc["command"].as<String>());
      mqttDoc = doc;
      mqttCommand = doc["command"].as<String>();
    } else {
      LOGN("Received valid JSON object on MQTT, but it is not addressed to this system. Dropping message");
    }
  }
}

// log data to sd-card
void logDataToSDCard(String file, String id, String ts, String samples) {
  if (SDCARD_MANDATORY) {
    StaticJsonDocument<1024> data;
    StaticJsonDocument<1024> smp;
    esp_task_wdt_reset();
    deserializeJson(smp, samples);
    data["board-id"] = id;
    data["timestamp"] = ts;
    data["samples"] = smp;
    String payload;
    serializeJson(data, payload);
    LOGN("Logging following data to SD card: ");
    LOGN(payload);

    appendFile(file.c_str(), (payload + String("\n")).c_str());
  }
}

// log to mqtt
void logDataToMqtt(PubSubClient &client, String id, String ts, String samples) {
  StaticJsonDocument<1024> data;
  StaticJsonDocument<1024> smp;
  esp_task_wdt_reset();
  deserializeJson(smp, samples);
  data["board-id"] = id;
  data["timestamp"] = ts;
  data["samples"] = smp;
  String payload;
  serializeJson(data, payload);
  client.publish("monitors/out", payload.c_str());
  LOGN("Sent sensor data over MQTT");
}


// Board main loop forever
void loop() {
  // watchdog checks
  esp_task_wdt_reset();

  // Reconnect wifi if possible
  wifiReconnect();
  // mosquitto reconnect
  boolean mqttConnected = mqttReconnect(mqttClient);

  if ((RTC.getEpoch() - loopCounter) >= SENSORS_SAMPLE_RATE) {
    LOGN("Fetching sensors readings...");
    StaticJsonDocument<1024> sensorData;
    JsonObject indoor = sensorData.createNestedObject("indoor");
    JsonObject outdoor = sensorData.createNestedObject("outdoor");
    JsonObject sensorTopology = sensors["sensor-topology"];

    String sn;
    serializeJson(sensors, sn);
    getAllSensorsData(sensorTopology, indoor, outdoor);


    // Build the samples data
    String samples = serializeSensorsData(indoor, outdoor);

    // Log sensor data to SD card
    String ts = timeStamp();
    logDataToSDCard("samples.log", boardId, ts, samples);
    logDataToSDCard("samples_bk.log", boardId, ts, samples);

    // log samples to mqtt()
    if (mqttConnected) {
      logDataToMqtt(mqttClient, boardId, timeStamp(), samples);
    }

    // log data to SD-card
    if (sdCardLogging) {
      esp_task_wdt_reset();
      appendFile("debug", sdCardLog);
    }

    loopCounter = RTC.getEpoch(true);
  }

  // MQTT client loop to get/push messages
  if (mqttConnected) {
    esp_task_wdt_reset();
    mqttClient.loop();
    if (!mqttCommand.equals("")) {
      handleSystemCommand(mqttCommand, mqttDoc);
      mqttCommand = "";
    }
  }

  // run BME680 sensors
  if (sensors["sensor-topology"]["BME680"].as<uint8_t>() > 0) {
    if (!bme680_1.run()) {
      checkBsecStatus(bme680_1);
    }
  }
  if (sensors["sensor-topology"]["BME680"].as<uint8_t>() > 1) {
    if (!bme680_2.run()) {
      checkBsecStatus(bme680_2);
    }
  }

  // Wait 10 seconds before fetching the next samples
  delay(100);
}

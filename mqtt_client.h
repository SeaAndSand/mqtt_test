#ifndef MQTT_CLIENT_H
#define MQTT_CLIENT_H

#include "MQTTAsync.h"
#include <pthread.h>

#define MAX_TOPICS 32

typedef int (*MQTTAsync_messageArrived_Handle)(void* context, char* topicName, int topicLen, MQTTAsync_message* message);
typedef void (*MQTTAsync_connectionStatusChanged)(void* context, int isConnected);
typedef int (*MQTTAsync_messageArrivedTotal)(void* context, char* topicName, int topicLen, MQTTAsync_message* message);

typedef struct {
    char* topic;
    int qos;
    MQTTAsync_messageArrived_Handle messageHandler;
    time_t lastForwardTime;
    int interval;
    int enableRateLimit;
} Topic;

typedef struct {
    int quit;
    pthread_t thread;
    int retry;
    int connected;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} ClientReconnect;

typedef struct {
    int enable;
    pthread_t thread;
    int count;
    int quit;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    char *heartbeat_topic;
    char *heartbeat_reply_topic;
} Heartbeat;
typedef struct {
    MQTTAsync client;
    char *url;
    char *name;
    Topic topics[MAX_TOPICS];
    int topicCount;
    MQTTAsync_connectionStatusChanged connectionStatusChangedCallback;
    MQTTAsync_messageArrivedTotal totalMessageHandler;
    ClientReconnect reconnect_handle;
    Heartbeat heartbeat;
    void *arrived_message;
    int isConnected;
} MqttClient;

MqttClient* mqttClient_init(const char* serverURI, const char* clientId, MQTTAsync_connectionStatusChanged connectionStatusChangedCallback, MQTTAsync_messageArrivedTotal totalMessageHandler, void *arrived_message, int enable_heartbeat);
void mqttClient_deinit(MqttClient* client);
void mqttClient_subscribe(MqttClient* client, const char* topic, int qos, MQTTAsync_messageArrived_Handle messageHandler, int interval, int enableRateLimit);
void mqttClient_publish(MqttClient* client, const char* topic, const char* payload, int payloadlen, int qos, int retained);
void mqttClient_connect(MqttClient* client);
// void mqttClient_disconnect(MqttClient* client);
void mqttClient_modify_interval(MqttClient *client, int interval);
int mqttClient_get_connected_status(MqttClient *client);

#endif // MQTT_CLIENT_H
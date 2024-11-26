#ifndef MQTT_CLIENT_H
#define MQTT_CLIENT_H

#include "MQTTAsync.h"
#include <pthread.h>

#define MAX_TOPICS 10

typedef int (*MQTTAsync_messageArrived_Handle)(void* context, char* topicName, int topicLen, MQTTAsync_message* message);
typedef void (*MQTTAsync_connectionStatusChanged)(void* context, int isConnected);
typedef int (*MQTTAsync_messageArrivedTotal)(void* context, char* topicName, int topicLen, MQTTAsync_message* message);

typedef struct {
    char* topic;
    int qos;
    MQTTAsync_messageArrived_Handle messageHandler;
} Topic;

typedef struct {
    int quit;
    pthread_t thread;
    int retry;
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
    pthread_cond_t cond;
    pthread_mutex_t lock;
    int status; // 0：未断开，1：断开中，2：断开成功，3：断开失败
} DisconnectHandle;

typedef struct {
    MQTTAsync client;
    char *url;
    char *name;
    Topic topics[MAX_TOPICS];
    int topicCount;
    int is_connected;
    MQTTAsync_connectionStatusChanged connectionStatusChangedCallback;
    MQTTAsync_messageArrivedTotal totalMessageHandler;
    ClientReconnect reconnect_handle;
    Heartbeat heartbeat;
    DisconnectHandle disconnect_handle;
    void *arrived_message;
} MqttClient;

MqttClient* mqttClient_init(const char* serverURI, const char* clientId, MQTTAsync_connectionStatusChanged connectionStatusChangedCallback, MQTTAsync_messageArrivedTotal totalMessageHandler, void *arrived_message, int enable_heartbeat);
void mqttClient_deinit(MqttClient* client);
void mqttClient_subscribe(MqttClient* client, const char* topic, int qos, MQTTAsync_messageArrived_Handle messageHandler);
void mqttClient_publish(MqttClient* client, const char* topic, const char* payload, int payloadlen, int qos, int retained);
void mqttClient_connect(MqttClient* client);
void mqttClient_disconnect(MqttClient* client);

#endif // MQTT_CLIENT_H
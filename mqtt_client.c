#include "mqtt_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include "slog.h"

#define KEEPALIVEINTERVAL (10)

static int topic_matches(const char *subscribed_topic, const char *received_topic) 
{
    int i = 0;
    int j = 0;
    int match = 1;

    while (subscribed_topic[i] && received_topic[j]) {
        if (subscribed_topic[i] == '+') {
            // Skip until next '/' in received_topic
            while (received_topic[j] && received_topic[j] != '/') {
                j++;
            }
            i++;
        } else if (subscribed_topic[i] == '#') {
            // '#' must be the last character of subscribed_topic
            if (subscribed_topic[i + 1] == '\0') {
                return 1; // '#' matches everything after this level
            } else {
                match = 0;
                break;
            }
        } else {
            if (subscribed_topic[i] != received_topic[j]) {
                match = 0;
                break;
            }
            i++;
            j++;
        }
    }

    // Check if both strings have reached the end
    if (subscribed_topic[i] != '\0' || received_topic[j] != '\0') {
        match = 0;
    }

    return match;
}

// 重连线程，负责失败重连
void* reconnect_thread(void* arg) {
    MqttClient* mqttClient = (MqttClient*)arg;

    while (1) {
        pthread_mutex_lock(&(mqttClient->reconnect_handle.lock));

        while (!mqttClient->reconnect_handle.retry && !mqttClient->reconnect_handle.quit) {
            pthread_cond_wait(&(mqttClient->reconnect_handle.cond), &(mqttClient->reconnect_handle.lock));
        }

        if (mqttClient->reconnect_handle.quit) {
            pthread_mutex_unlock(&(mqttClient->reconnect_handle.lock));
            break;
        }

        mqttClient->reconnect_handle.retry = 0;
        pthread_mutex_unlock(&(mqttClient->reconnect_handle.lock));

        slog_info("[%s] Retrying connection...\n", mqttClient->name);

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 20; // 等待20秒

        pthread_mutex_lock(&(mqttClient->reconnect_handle.lock));
        while (!mqttClient->reconnect_handle.quit) {
            int rc = pthread_cond_timedwait(&(mqttClient->reconnect_handle.cond), &(mqttClient->reconnect_handle.lock), &ts);
            if (rc == ETIMEDOUT) {
                break; // 超时，退出等待
            }
        }
        if (mqttClient->reconnect_handle.quit) {
            pthread_mutex_unlock(&(mqttClient->reconnect_handle.lock));
            break;
        }
        pthread_mutex_unlock(&(mqttClient->reconnect_handle.lock));

        // 重连
        mqttClient_connect(mqttClient);
    }

    return NULL;
}

void reconnect_thread_quit(MqttClient *client) {
    pthread_mutex_lock(&(client->reconnect_handle.lock));
    client->reconnect_handle.quit = 1;
    pthread_cond_signal(&(client->reconnect_handle.cond));
    pthread_mutex_unlock(&(client->reconnect_handle.lock));
    pthread_join(client->reconnect_handle.thread, NULL);
    pthread_mutex_destroy(&(client->reconnect_handle.lock));
    pthread_cond_destroy(&(client->reconnect_handle.cond));
}

void reconnect_thread_wakeup(MqttClient *client) {
    pthread_mutex_lock(&(client->reconnect_handle.lock));
    client->reconnect_handle.retry = 1;
    pthread_cond_signal(&(client->reconnect_handle.cond));
    pthread_mutex_unlock(&(client->reconnect_handle.lock));
}

// 连接丢失回调函数
void onConnectionLost(void *context, char *cause) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] Connection lost: %s\n", client->name, cause);
    client->is_connected = 0;
    if (client->connectionStatusChangedCallback) {
        client->connectionStatusChangedCallback(client, 0);
    }

    mqttClient_connect(client);
}

// 连接成功回调函数
void onConnectSuccess(void* context, MQTTAsync_successData5* response) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] Connected successfully\n", client->name);
    client->is_connected = 1;
    for (int i = 0; i < client->topicCount; i++) {
        MQTTAsync_subscribe(client->client, client->topics[i].topic, client->topics[i].qos, NULL);
    }

    if (client->connectionStatusChangedCallback) {
        client->connectionStatusChangedCallback(client, 1);
    }
}

// 连接失败回调函数
void onConnectFailure(void* context, MQTTAsync_failureData5* response) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] Connect failed, rc %d\n", client->name, response ? response->code : 0);
    client->is_connected = 0;
    if (client->connectionStatusChangedCallback) {
        client->connectionStatusChangedCallback(client, 0);
    }
    reconnect_thread_wakeup(client);
}

// 消息到达回调函数
int onMessageArrived(void* context, char* topicName, int topicLen, MQTTAsync_message* message) {
    MqttClient* client = (MqttClient*)context;
    // 总处理方法
    if (client->totalMessageHandler && client->totalMessageHandler(context, topicName, topicLen, message) < 0) {
        slog_info("[%s] Total message handler failed\n", client->name);
        MQTTAsync_freeMessage(&message);
        MQTTAsync_free(topicName);
        return 1;
    }

    // 具体处理方法
    for (int i = 0; i < client->topicCount; i++) {
        if (topic_matches(client->topics[i].topic, topicName)) {
            client->topics[i].messageHandler(context, topicName, topicLen, message);
        }
    }

    MQTTAsync_freeMessage(&message);
    MQTTAsync_free(topicName);
    return 1;
}

// 发送成功回调函数
void onSendSuccess(void* context, MQTTAsync_successData5* response) {
    // MqttClient *client = (MqttClient *)context;
    // printf("[%s] Message sent successfully\n", client->name);
}

// 发送失败回调函数
void onSendFailure(void* context, MQTTAsync_failureData5* response) {
    MqttClient *client = (MqttClient *)context;
    slog_info("[%s] Failed to send message\n", client->name);
}

void myTraceCallback(enum MQTTASYNC_TRACE_LEVELS level, char* message) {
    slog_info("Trace : %s\n", message);
}

// 客户端初始化
MqttClient* mqttClient_init(const char* serverURI, const char* clientId, MQTTAsync_connectionStatusChanged connectionStatusChangedCallback, MQTTAsync_messageArrivedTotal totalMessageHandler) {
    slog_info("serverURI = %s, clientId = %s\n", serverURI, clientId);
    MqttClient* client = malloc(sizeof(MqttClient));
    if (client == NULL) {
        slog_info("Failed to allocate memory for MqttClient\n");
        return NULL;
    }

    memset(client, 0, sizeof(MqttClient));
    client->name = strdup(clientId);
    client->is_connected = 0;
    client->connectionStatusChangedCallback = connectionStatusChangedCallback;
    client->totalMessageHandler = totalMessageHandler;

    client->reconnect_handle.quit = 0;
    client->reconnect_handle.retry = 0;
    pthread_mutex_init(&client->reconnect_handle.lock, NULL);
    pthread_cond_init(&client->reconnect_handle.cond, NULL);

    pthread_create(&client->reconnect_handle.thread, NULL, reconnect_thread, (void *)client);

    MQTTAsync_createOptions create_opts = MQTTAsync_createOptions_initializer5;
    create_opts.deleteOldestMessages = 1;
    create_opts.maxBufferedMessages = 1000;
    if (MQTTAsync_createWithOptions(&client->client, serverURI, clientId, MQTTCLIENT_PERSISTENCE_NONE, NULL, &create_opts) != MQTTASYNC_SUCCESS) {
        slog_info("Failed to create MQTTAsync client\n");
        reconnect_thread_quit(client);
        free(client->name);
        free(client);
        return NULL;
    }

    // MQTTAsync_setTraceLevel(MQTTASYNC_TRACE_MAXIMUM);
    // MQTTAsync_setTraceCallback(myTraceCallback);

    MQTTAsync_setCallbacks(client->client, client, onConnectionLost, onMessageArrived, NULL);

    MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer5;
    conn_opts.keepAliveInterval = KEEPALIVEINTERVAL;
    conn_opts.cleanstart = 1;
    conn_opts.onSuccess5 = onConnectSuccess;
    conn_opts.onFailure5 = onConnectFailure;
    conn_opts.automaticReconnect = 0;
    // 回调使用
    conn_opts.context = client;

    int ret;
    if ((ret = MQTTAsync_connect(client->client, &conn_opts)) != MQTTASYNC_SUCCESS) {
        slog_info("Failed to connect to MQTT broker, ret = %d, %s\n", ret, MQTTAsync_strerror(ret));
        reconnect_thread_quit(client);
        MQTTAsync_destroy(&client->client);
        free(client->name);
        free(client);
        return NULL;
    }

    return client;
}

// 客户端反初始化
void mqttClient_deinit(MqttClient* client) {
    if (client == NULL) {
        return;
    }

    if (client->is_connected) {
        mqttClient_disconnect(client);
    }

    MQTTAsync_destroy(&client->client);

    for (int i = 0; i < client->topicCount; i++) {
        free(client->topics[i].topic);
    }
    reconnect_thread_quit(client);
    free(client->name);
    free(client);
}

// 连接到服务器
void mqttClient_connect(MqttClient* client) {
    if (client->is_connected) {
        return;
    }

    MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer5;
    conn_opts.keepAliveInterval = KEEPALIVEINTERVAL;
    conn_opts.cleanstart = 1;
    conn_opts.onSuccess5 = onConnectSuccess;
    conn_opts.onFailure5 = onConnectFailure;
    conn_opts.context = client;

    if (MQTTAsync_connect(client->client, &conn_opts) != MQTTASYNC_SUCCESS) {
        slog_info("Failed to connect to MQTT broker\n");
    }
}

// 注册主题订阅和消息处理
void mqttClient_subscribe(MqttClient* client, const char* topic, int qos, MQTTAsync_messageArrived_Handle messageHandler) {
    if (client->topicCount >= MAX_TOPICS) {
        slog_info("Maximum number of topics reached\n");
        return;
    }

    client->topics[client->topicCount].topic = strdup(topic);
    if (client->topics[client->topicCount].topic == NULL) {
        slog_info("Failed to allocate memory for topic\n");
        return;
    }

    client->topics[client->topicCount].qos = qos;
    client->topics[client->topicCount].messageHandler = messageHandler;
    client->topicCount++;

    if (client->is_connected) {
        MQTTAsync_subscribe(client->client, topic, qos, NULL);
    }
}

// 发送
void mqttClient_publish(MqttClient* client, const char* topic, const char* payload, int payloadlen, int qos, int retained) {
    if (!client->is_connected) {
        slog_info("Not connected to the broker\n");
        return;
    }

    MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
    MQTTAsync_message pubmsg = MQTTAsync_message_initializer;

    pubmsg.payload = (char *)payload;
    pubmsg.payloadlen = payloadlen;
    pubmsg.qos = qos;
    pubmsg.retained = retained;
    opts.onSuccess5 = onSendSuccess;
    opts.onFailure5 = onSendFailure;
    opts.context = client;

    int rc;
    if ((rc = MQTTAsync_sendMessage(client->client, topic, &pubmsg, &opts)) != MQTTASYNC_SUCCESS) {
        slog_info("Failed to send message, rc = %d, %s\n", rc, MQTTAsync_strerror(rc));
        if (rc == MQTTASYNC_MAX_BUFFERED_MESSAGES) {
            // mqttClient_disconnect(client);
            // if (client->connectionStatusChangedCallback) {
            //     client->connectionStatusChangedCallback(client, 0);
            // }
            // mqttClient_connect(client);
            // printf("主动断开连接\n");
        }
    }
}

// 断开连接
void mqttClient_disconnect(MqttClient* client) {
    if (!client->is_connected) {
        return;
    }

    MQTTAsync_disconnectOptions disc_opts = MQTTAsync_disconnectOptions_initializer5;
    if (MQTTAsync_disconnect(client->client, &disc_opts) != MQTTASYNC_SUCCESS) {
        slog_info("Failed to disconnect from MQTT broker\n");
    }
    client->is_connected = 0;
}
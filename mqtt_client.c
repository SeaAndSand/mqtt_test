#include "mqtt_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <arpa/inet.h>
#include "slog.h"
#include "uniq_serial_nums.h"
#include "system_support.h"

#define KEEPALIVEINTERVAL (10)
#define HEARTBEAT_TOPIC "heartbeat"
#define HEARTBEAT_REPLY_TOPIC "heartbeat_reply"
#define HEARTBEAT_INTERVAL (10)
#define HEARTBEAT_TIMEOUT (60)

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
            slog_info("[%s] [%s] reconnect wait...\n", mqttClient->url, mqttClient->name);
            pthread_cond_wait(&(mqttClient->reconnect_handle.cond), &(mqttClient->reconnect_handle.lock));
        }

        slog_info("[%s] [%s] reconnect wake...\n", mqttClient->url, mqttClient->name);

        if (mqttClient->reconnect_handle.quit) {
            pthread_mutex_unlock(&(mqttClient->reconnect_handle.lock));
            break;
        }

        mqttClient->reconnect_handle.retry = 0;
        pthread_mutex_unlock(&(mqttClient->reconnect_handle.lock));

        slog_info("[%s] [%s] Retrying connection...\n", mqttClient->url, mqttClient->name);

        mqttClient_connect(mqttClient);
    }

    return NULL;
}

void reconnect_thread_quit(MqttClient *client) {
    pthread_mutex_lock(&(client->reconnect_handle.lock));
    client->reconnect_handle.quit = 1;
    pthread_cond_signal(&(client->reconnect_handle.cond));
    pthread_mutex_unlock(&(client->reconnect_handle.lock));
    join_flexible_thread(client->reconnect_handle.thread);
    pthread_mutex_destroy(&(client->reconnect_handle.lock));
    pthread_cond_destroy(&(client->reconnect_handle.cond));
}

void reconnect_thread_wakeup(MqttClient *client) {
    pthread_mutex_lock(&(client->reconnect_handle.lock));
    client->reconnect_handle.retry = 1;
    pthread_cond_signal(&(client->reconnect_handle.cond));
    pthread_mutex_unlock(&(client->reconnect_handle.lock));
}

void* heartbeat_thread(void* arg) {
    MqttClient* client = (MqttClient*)arg;

    while (1) {
        pthread_mutex_lock(&(client->heartbeat.lock));

        while (!mqttClient_get_connected_status(client) && !client->heartbeat.quit) {
            pthread_cond_wait(&(client->heartbeat.cond), &(client->heartbeat.lock));
        }

        if (client->heartbeat.quit) {
            pthread_mutex_unlock(&(client->heartbeat.lock));
            break;
        }

        pthread_mutex_unlock(&(client->heartbeat.lock));

        char data[32] = {0};
        sprintf(data, "%d", client->heartbeat.count);
        slog_info("[%s] [%s] 发送心跳, heartbeat_count = %d\n", client->url, client->name, client->heartbeat.count);
        mqttClient_publish(client, client->heartbeat.heartbeat_topic, data, strlen(data), 0, 0);

        struct timespec ts_reply;
        clock_gettime(CLOCK_REALTIME, &ts_reply);
        ts_reply.tv_sec += HEARTBEAT_INTERVAL;
        pthread_mutex_lock(&(client->heartbeat.lock));
        int rc = pthread_cond_timedwait(&(client->heartbeat.cond), &(client->heartbeat.lock), &ts_reply);
        if (client->heartbeat.quit) {
            pthread_mutex_unlock(&(client->heartbeat.lock));
            break;
        }

        if (rc != ETIMEDOUT) {
            clock_gettime(CLOCK_REALTIME, &ts_reply);
            ts_reply.tv_sec += HEARTBEAT_INTERVAL;
            pthread_cond_timedwait(&(client->heartbeat.cond), &(client->heartbeat.lock), &ts_reply);
            if (client->heartbeat.quit) {
                pthread_mutex_unlock(&(client->heartbeat.lock));
                break;
            }
        }
        pthread_mutex_unlock(&(client->heartbeat.lock));
    }
    return NULL;
}

void heartbeat_thread_quit(MqttClient *client) {
    pthread_mutex_lock(&(client->heartbeat.lock));
    client->heartbeat.quit = 1;
    pthread_cond_signal(&(client->heartbeat.cond));
    pthread_mutex_unlock(&(client->heartbeat.lock));
    join_flexible_thread(client->heartbeat.thread);
    pthread_mutex_destroy(&(client->heartbeat.lock));
    pthread_cond_destroy(&(client->heartbeat.cond));
    free(client->heartbeat.heartbeat_topic);
    free(client->heartbeat.heartbeat_reply_topic);
}

void heartbeat_thread_wakeup(MqttClient *client) {
    pthread_mutex_lock(&(client->heartbeat.lock));
    pthread_cond_signal(&(client->heartbeat.cond));
    pthread_mutex_unlock(&(client->heartbeat.lock));
}

// 连接丢失回调函数
void onConnectionLost(void *context, char *cause) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] [%s] Connection lost: %s\n", client->url, client->name, cause);
    client->isConnected = 0;
	if (client->connectionStatusChangedCallback) {
		client->connectionStatusChangedCallback(client, 0);
	}
	if (client->reconnect_handle.connected == 0) {
        reconnect_thread_wakeup(client);
	}
}

void onConnectSuccess(void* context, MQTTAsync_successData5* response) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] [%s] Connected successfully\n", client->url, client->name);
    client->isConnected = 1;
    if (client->connectionStatusChangedCallback) {
        client->connectionStatusChangedCallback(client, 1);
    }

	if (client->reconnect_handle.connected == 0){
		client->reconnect_handle.connected = 1;
	}

	MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
	opts.subscribeOptions.retainAsPublished = 1;
	opts.subscribeOptions.noLocal = 1;
	for (int i = 0; i < client->topicCount; i++) {
		MQTTAsync_subscribe(client->client, client->topics[i].topic, client->topics[i].qos, &opts);
	}
    // 注册心跳主题
    if (client->heartbeat.enable) {
        MQTTAsync_subscribe(client->client, client->heartbeat.heartbeat_reply_topic, 0, &opts);
    }

    if (client->heartbeat.enable) {
        heartbeat_thread_wakeup(client);
    }
}


void onReConnectSuccess(void* context, char* cause) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] [%s] ReConnected successfully\n", client->url, client->name);
    client->isConnected = 1;
    if (client->connectionStatusChangedCallback) {
        client->connectionStatusChangedCallback(client, 1);
    }

	if (client->reconnect_handle.connected == 0){
		client->reconnect_handle.connected = 1;
	}

	MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
	opts.subscribeOptions.retainAsPublished = 1;
	opts.subscribeOptions.noLocal = 1;
	for (int i = 0; i < client->topicCount; i++) {
		MQTTAsync_subscribe(client->client, client->topics[i].topic, client->topics[i].qos, &opts);
	}
    // 注册心跳主题
    if (client->heartbeat.enable) {
        MQTTAsync_subscribe(client->client, client->heartbeat.heartbeat_reply_topic, 0, &opts);
    }

    if (client->heartbeat.enable) {
        heartbeat_thread_wakeup(client);
    }
}

// 连接失败回调函数
void onConnectFailure(void* context, MQTTAsync_failureData5* response) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] [%s] Connect failed, rc %d\n", client->url, client->name, response ? response->code : 0);
    client->isConnected = 0;
    if (client->connectionStatusChangedCallback) {
        client->connectionStatusChangedCallback(client, 0);
    }
	if (client->reconnect_handle.connected == 0) {
		reconnect_thread_wakeup(client);
	}
}

// 消息到达回调函数
int onMessageArrived(void* context, char* topicName, int topicLen, MQTTAsync_message* message) {
    MqttClient* client = (MqttClient*)context;
    // 心跳主题判断
    if (client->heartbeat.enable && client->heartbeat.heartbeat_reply_topic && topicName && strcmp(topicName, client->heartbeat.heartbeat_reply_topic) == 0) {
        char data[32] = {0};
        memcpy(data, message->payload, message->payloadlen);
        int received_count = atoi(data);
        slog_info("[%s] [%s] 收到回复，received_count = %d\n", client->url, client->name, received_count);
        if (received_count == client->heartbeat.count) {
            client->heartbeat.count = (client->heartbeat.count % 65535) + 1;
            heartbeat_thread_wakeup(client);
        }
        MQTTAsync_freeMessage(&message);
        MQTTAsync_free(topicName);
        return 1;
    }

    // 总处理方法
    if (client->totalMessageHandler && client->totalMessageHandler(context, topicName, topicLen, message) < 0) {
        slog_info("[%s] [%s] Total message handler failed\n", client->url, client->name);
        MQTTAsync_freeMessage(&message);
        MQTTAsync_free(topicName);
        return 1;
    }

    for (int i = 0; i < client->topicCount; i++) {
        if (topic_matches(client->topics[i].topic, topicName)) {
            if (client->topics[i].enableRateLimit) {
                // 如果启用了控制转发速度，检查时间间隔
                time_t now = time(NULL);
                if (now - client->topics[i].lastForwardTime < client->topics[i].interval) {
                    //slog_info("[%s] [%s] Rate limit exceeded, ignoring message, diff = %d, interval = %d\n", client->url, client->name, now - client->topics[i].lastForwardTime, client->topics[i].interval);
                    MQTTAsync_freeMessage(&message);
                    MQTTAsync_free(topicName);
                    return 1;
                }
                client->topics[i].lastForwardTime = now;
            }
            client->topics[i].messageHandler(context, topicName, topicLen, message);
        }
    }
    MQTTAsync_freeMessage(&message);
    MQTTAsync_free(topicName);
    return 1;
}

// 发送成功回调函数
void onSendSuccess(void* context, MQTTAsync_successData5* response) {
}

// 发送失败回调函数
void onSendFailure(void* context, MQTTAsync_failureData5* response) {
    MqttClient *client = (MqttClient *)context;
    slog_info("[%s] [%s] Failed to send message\n", client->url, client->name);
}

void myTraceCallback(enum MQTTASYNC_TRACE_LEVELS level, char* message) {
    slog_info("Trace[%d]: %s\n", level, message);
}

// 客户端初始化（不包含连接操作）
MqttClient* mqttClient_init_no_connect(const char* serverURI, const char* clientId, MQTTAsync_connectionStatusChanged connectionStatusChangedCallback, MQTTAsync_messageArrivedTotal totalMessageHandler, void *arrived_message, int enable_heartbeat) {
    slog_info("serverURI = %s, clientId = %s\n", serverURI, clientId);
    MqttClient* client = malloc(sizeof(MqttClient));
    if (client == NULL) {
        slog_info("Failed to allocate memory for MqttClient\n");
        return NULL;
    }

    memset(client, 0, sizeof(MqttClient));
    client->url = strdup(serverURI);
    client->name = strdup(clientId);
    client->connectionStatusChangedCallback = connectionStatusChangedCallback;
    client->totalMessageHandler = totalMessageHandler;
    client->arrived_message = arrived_message;

    client->heartbeat.enable = enable_heartbeat;
    if (enable_heartbeat) {
        char heartbeat_topic[128];
        memset(heartbeat_topic, 0, sizeof(heartbeat_topic));
        snprintf(heartbeat_topic, sizeof(heartbeat_topic), "/logger/%s/%s", generate_station_id((char[64]){0}, MODE_DEFAULT), HEARTBEAT_TOPIC);
        client->heartbeat.heartbeat_topic = strdup(heartbeat_topic);
        memset(heartbeat_topic, 0, sizeof(heartbeat_topic));
        snprintf(heartbeat_topic, sizeof(heartbeat_topic), "/logger/%s/%s", generate_station_id((char[64]){0}, MODE_DEFAULT), HEARTBEAT_REPLY_TOPIC);
        client->heartbeat.heartbeat_reply_topic = strdup(heartbeat_topic);
        client->heartbeat.count = 1;
        client->heartbeat.quit = 0;
        pthread_mutex_init(&client->heartbeat.lock, NULL);
        pthread_cond_init(&client->heartbeat.cond, NULL);
    }

    client->reconnect_handle.quit = 0;
    client->reconnect_handle.retry = 0;
    client->reconnect_handle.connected = 0;
    pthread_mutex_init(&client->reconnect_handle.lock, NULL);
    pthread_cond_init(&client->reconnect_handle.cond, NULL);

    // pthread_create(&client->reconnect_handle.thread, NULL, reconnect_thread, (void *)client);
    client->reconnect_handle.thread = create_flexible_thread(reconnect_thread, (void *)client);

    if (enable_heartbeat) {
        // pthread_create(&client->heartbeat.thread, NULL, heartbeat_thread, (void *)client);
        client->heartbeat.thread = create_flexible_thread(heartbeat_thread, (void *)client);
    }

    MQTTAsync_createOptions create_opts = MQTTAsync_createOptions_initializer5;
    create_opts.deleteOldestMessages = 1;
    create_opts.maxBufferedMessages = 1000;
    if (MQTTAsync_createWithOptions(&client->client, serverURI, clientId, MQTTCLIENT_PERSISTENCE_NONE, NULL, &create_opts) != MQTTASYNC_SUCCESS) {
        slog_info("[%s] [%s] Failed to create MQTTAsync client\n", client->url, client->name);
        reconnect_thread_quit(client);
        if (enable_heartbeat) {
            heartbeat_thread_quit(client);
        }
        free(client->name);
        free(client->url);
        free(client);
        return NULL;
    }

    // MQTTAsync_setTraceLevel(MQTTASYNC_TRACE_MAXIMUM);
    // MQTTAsync_setTraceCallback(myTraceCallback);
    MQTTAsync_setCallbacks(client->client, client, onConnectionLost, onMessageArrived, NULL);

    return client;
}

// 客户端初始化
MqttClient* mqttClient_init(const char* serverURI, const char* clientId, MQTTAsync_connectionStatusChanged connectionStatusChangedCallback, MQTTAsync_messageArrivedTotal totalMessageHandler, void *arrived_message, int enable_heartbeat) {
    MqttClient* client = mqttClient_init_no_connect(serverURI, clientId, connectionStatusChangedCallback, totalMessageHandler, arrived_message, enable_heartbeat);
    if (client == NULL) {
        return NULL;
    }

    mqttClient_connect(client);

    return client;
}

// 客户端反初始化
void mqttClient_deinit(MqttClient* client) {
    if (client == NULL) {
        return;
    }
    reconnect_thread_quit(client);
    if (client->heartbeat.enable) {
        heartbeat_thread_quit(client);
    }
    for (int i = 0; i < client->topicCount; i++) {
        free(client->topics[i].topic);
    }
    MQTTAsync_destroy(&client->client);
    free(client->name);
    free(client->url);
    free(client);
}

// 连接到服务器
void mqttClient_connect(MqttClient* client) {
    slog_info("[%s] [%s] Connecting to MQTT broker\n", client->url, client->name);

    MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer5;
    conn_opts.keepAliveInterval = KEEPALIVEINTERVAL;
    conn_opts.cleanstart = 1;
    conn_opts.onSuccess5 = NULL;
    conn_opts.onFailure5 = onConnectFailure;
    conn_opts.minRetryInterval = 1;
    conn_opts.maxRetryInterval = 30;
    conn_opts.automaticReconnect = 1; // 打开自动重连
    conn_opts.context = client;

    MQTTAsync_setConnected(client->client, client, onReConnectSuccess);

    if (MQTTAsync_connect(client->client, &conn_opts) != MQTTASYNC_SUCCESS) {
        slog_info("[%s] [%s] Failed to connect to MQTT broker\n", client->url, client->name);
    }
}

// 注册主题订阅和消息处理
void mqttClient_subscribe(MqttClient* client, const char* topic, int qos, MQTTAsync_messageArrived_Handle messageHandler, int interval, int enableRateLimit) {
    if (client->topicCount >= MAX_TOPICS) {
        slog_info("[%s] [%s] Maximum number of topics reached\n", client->url, client->name);
        return;
    }

    client->topics[client->topicCount].topic = strdup(topic);
    if (client->topics[client->topicCount].topic == NULL) {
        slog_info("[%s] [%s] Failed to allocate memory for topic\n", client->url, client->name);
        return;
    }

    client->topics[client->topicCount].qos = qos;
    client->topics[client->topicCount].messageHandler = messageHandler;
    client->topics[client->topicCount].lastForwardTime = 0;
    client->topics[client->topicCount].interval = interval;
    client->topics[client->topicCount].enableRateLimit = enableRateLimit;
    client->topicCount++;

    if (mqttClient_get_connected_status(client)) {
        MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
        opts.subscribeOptions.retainAsPublished = 1;
        opts.subscribeOptions.noLocal = 1;
        MQTTAsync_subscribe(client->client, topic, qos, &opts);
    }
}

// 发送
void mqttClient_publish(MqttClient* client, const char* topic, const char* payload, int payloadlen, int qos, int retained) {
    if (!mqttClient_get_connected_status(client)) {
        // slog_info("[%s] [%s] Not connected to the broker\n", client->url, client->name);
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
        slog_info("[%s] [%s] Failed to send message, rc = %d, %s\n",  client->url, client->name, rc, MQTTAsync_strerror(rc));
	}
}

void mqttClient_modify_interval(MqttClient *client, int interval) {
    for (int i = 0; i < client->topicCount; i++) {
        if (client->topics[i].enableRateLimit) {
            client->topics[i].interval = interval;
        }
    }
}

int mqttClient_get_connected_status(MqttClient *client) {
    return client->isConnected;
}
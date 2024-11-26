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
            pthread_cond_wait(&(mqttClient->reconnect_handle.cond), &(mqttClient->reconnect_handle.lock));
        }

        if (mqttClient->reconnect_handle.quit) {
            pthread_mutex_unlock(&(mqttClient->reconnect_handle.lock));
            break;
        }

        mqttClient->reconnect_handle.retry = 0;
        pthread_mutex_unlock(&(mqttClient->reconnect_handle.lock));

        slog_info("[%s] [%s] Retrying connection...\n", mqttClient->url, mqttClient->name);

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 20; // 等待20秒

        pthread_mutex_lock(&(mqttClient->reconnect_handle.lock));
        while (!mqttClient->reconnect_handle.quit) {
            int rc = pthread_cond_timedwait(&(mqttClient->reconnect_handle.cond), &(mqttClient->reconnect_handle.lock), &ts);
            slog_info("[%s] [%s] timedwait = %d\n", mqttClient->url, mqttClient->name, rc);
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

void* heartbeat_thread(void* arg) {
    MqttClient* client = (MqttClient*)arg;
    int heartbeat_missed = 0;

    while (1) {
        pthread_mutex_lock(&(client->heartbeat.lock));

        while (!client->is_connected && !client->heartbeat.quit) {
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
        // 如果唤醒之后，连接状态是断开，说明是断开回调先触发
        if (!client->is_connected) {
            pthread_mutex_unlock(&(client->heartbeat.lock));
            continue;
        }

        if (rc == ETIMEDOUT) {
            heartbeat_missed += HEARTBEAT_INTERVAL;
            if (heartbeat_missed >= HEARTBEAT_TIMEOUT) {
                slog_info("[%s] [%s] Heartbeat timeout, disconnecting...\n", client->url, client->name);
                if (client->is_connected) {
                    // client->is_connected = 0;
                    mqttClient_disconnect(client);
                    // 无论成功还是失败，都清空计数
                    heartbeat_missed = 0;
                    if (client->is_connected == 0) {
                        if (client->connectionStatusChangedCallback) {
                            client->connectionStatusChangedCallback(client, 0);
                        }
                        reconnect_thread_wakeup(client);
                    }
                }
            }
        } else {
            heartbeat_missed = 0;
            struct timespec ts_next;
            clock_gettime(CLOCK_REALTIME, &ts_next);
            ts_next.tv_sec += HEARTBEAT_INTERVAL;
            rc = pthread_cond_timedwait(&(client->heartbeat.cond), &(client->heartbeat.lock), &ts_next);
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
    pthread_join(client->heartbeat.thread, NULL);
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
    if (client->is_connected) {
        client->is_connected = 0;
        if (client->disconnect_handle.status == 1) {
            // 如果在断开过程中连接丢失，不唤醒重连线程
            return;
        }

        if (client->connectionStatusChangedCallback) {
            client->connectionStatusChangedCallback(client, 0);
        }
        reconnect_thread_wakeup(client);
    }
}

// 连接成功回调函数
void onConnectSuccess(void* context, MQTTAsync_successData5* response) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] [%s] Connected successfully\n", client->url, client->name);
    client->is_connected = 1;

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
    if (client->connectionStatusChangedCallback) {
        client->connectionStatusChangedCallback(client, 1);
    }

    if (client->heartbeat.enable) {
        heartbeat_thread_wakeup(client);
    }
}

// 连接失败回调函数
void onConnectFailure(void* context, MQTTAsync_failureData5* response) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] [%s] Connect failed, rc %d\n", client->url, client->name, response ? response->code : 0);
    client->is_connected = 0;
    if (client->connectionStatusChangedCallback) {
        client->connectionStatusChangedCallback(client, 0);
    }
    reconnect_thread_wakeup(client);
}

// 消息到达回调函数
int onMessageArrived(void* context, char* topicName, int topicLen, MQTTAsync_message* message) {
    MqttClient* client = (MqttClient*)context;

    // 忽略在断开状态或者断开过程中的消息
    if (!client->is_connected || client->disconnect_handle.status == 1) {
        slog_info("[%s] [%s] Ignoring message, not connected\n", client->url, client->name);
        MQTTAsync_freeMessage(&message);
        MQTTAsync_free(topicName);
        return 1;
    }

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
    slog_info("[%s] [%s] Failed to send message\n", client->url, client->name);
}

void myTraceCallback(enum MQTTASYNC_TRACE_LEVELS level, char* message) {
    slog_info("Trace : %s\n", message);
}

// 客户端初始化
MqttClient* mqttClient_init(const char* serverURI, const char* clientId, MQTTAsync_connectionStatusChanged connectionStatusChangedCallback, MQTTAsync_messageArrivedTotal totalMessageHandler, void *arrived_message, int enable_heartbeat) {
    slog_info("serverURI = %s, clientId = %s\n", serverURI, clientId);
    MqttClient* client = malloc(sizeof(MqttClient));
    if (client == NULL) {
        slog_info("Failed to allocate memory for MqttClient\n");
        return NULL;
    }

    memset(client, 0, sizeof(MqttClient));
    client->url = strdup(serverURI);
    client->name = strdup(clientId);
    client->is_connected = 0;
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
    pthread_mutex_init(&client->reconnect_handle.lock, NULL);
    pthread_cond_init(&client->reconnect_handle.cond, NULL);

    pthread_create(&client->reconnect_handle.thread, NULL, reconnect_thread, (void *)client);

    if (enable_heartbeat) {
        pthread_create(&client->heartbeat.thread, NULL, heartbeat_thread, (void *)client);
    }

    pthread_cond_init(&client->disconnect_handle.cond, NULL);
    pthread_mutex_init(&client->disconnect_handle.lock, NULL);
    client->disconnect_handle.status = 0;

    MQTTAsync_createOptions create_opts = MQTTAsync_createOptions_initializer5;
    create_opts.deleteOldestMessages = 1;
    create_opts.maxBufferedMessages = 1000;
    if (MQTTAsync_createWithOptions(&client->client, serverURI, clientId, MQTTCLIENT_PERSISTENCE_NONE, NULL, &create_opts) != MQTTASYNC_SUCCESS) {
        slog_info("[%s] [%s] Failed to create MQTTAsync client\n", client->url, client->name);
        reconnect_thread_quit(client);
        if (enable_heartbeat) {
            heartbeat_thread_quit(client);
        }
        pthread_mutex_destroy(&client->disconnect_handle.lock);
        pthread_cond_destroy(&client->disconnect_handle.cond);
        free(client->name);
        free(client->url);
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
        slog_info("[%s] [%s] Failed to connect to MQTT broker, ret = %d, %s\n", client->url, client->name, ret, MQTTAsync_strerror(ret));
        reconnect_thread_quit(client);
        if (enable_heartbeat) {
            heartbeat_thread_quit(client);
        }
        MQTTAsync_destroy(&client->client);
        pthread_mutex_destroy(&client->disconnect_handle.lock);
        pthread_cond_destroy(&client->disconnect_handle.cond);
        free(client->name);
        free(client->url);
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

    if (client->heartbeat.enable) {
        heartbeat_thread_quit(client);
    }

    pthread_mutex_destroy(&client->disconnect_handle.lock);
    pthread_cond_destroy(&client->disconnect_handle.cond);

    free(client->name);
    free(client->url);
    free(client);
}

// 连接到服务器
void mqttClient_connect(MqttClient* client) {
    slog_info("[%s] [%s] Connecting to MQTT broker\n", client->url, client->name);

    if (client->is_connected) {
        return;
    }

    MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer5;
    conn_opts.keepAliveInterval = KEEPALIVEINTERVAL;
    conn_opts.cleanstart = 1;
    conn_opts.onSuccess5 = onConnectSuccess;
    conn_opts.onFailure5 = onConnectFailure;
    conn_opts.context = client;

    slog_info("[%s] [%s] connectTimeout = %d\n", client->url, client->name, conn_opts.connectTimeout);

    if (MQTTAsync_connect(client->client, &conn_opts) != MQTTASYNC_SUCCESS) {
        slog_info("[%s] [%s] Failed to connect to MQTT broker\n", client->url, client->name);
    }
}

// 注册主题订阅和消息处理
void mqttClient_subscribe(MqttClient* client, const char* topic, int qos, MQTTAsync_messageArrived_Handle messageHandler) {
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
    client->topicCount++;

    if (client->is_connected) {
        MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
        opts.subscribeOptions.retainAsPublished = 1;
        opts.subscribeOptions.noLocal = 1;
        MQTTAsync_subscribe(client->client, topic, qos, &opts);
    }
}

// 发送
void mqttClient_publish(MqttClient* client, const char* topic, const char* payload, int payloadlen, int qos, int retained) {
    if (!client->is_connected) {
        slog_info("[%s] [%s] Not connected to the broker\n", client->url, client->name);
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

void onDisconnectSuccess(void* context, MQTTAsync_successData5* response) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] [%s] Disconnected successfully\n", client->url, client->name);

    pthread_mutex_lock(&client->disconnect_handle.lock);
    client->disconnect_handle.status = 2; // 断开成功
    pthread_cond_signal(&client->disconnect_handle.cond);
    pthread_mutex_unlock(&client->disconnect_handle.lock);
}

void onDisconnectFailure(void* context, MQTTAsync_failureData5* response) {
    MqttClient* client = (MqttClient*)context;
    slog_info("[%s] [%s] Disconnect failed, rc %d\n", client->url, client->name, response ? response->code : 0);

    pthread_mutex_lock(&client->disconnect_handle.lock);
    client->disconnect_handle.status = 3; // 断开失败
    pthread_cond_signal(&client->disconnect_handle.cond);
    pthread_mutex_unlock(&client->disconnect_handle.lock);
}


// 断开连接
void mqttClient_disconnect(MqttClient* client) {
    if (!client->is_connected) {
        return;
    }

    MQTTAsync_disconnectOptions disc_opts = MQTTAsync_disconnectOptions_initializer5;
    disc_opts.onSuccess5 = onDisconnectSuccess;
    disc_opts.onFailure5 = onDisconnectFailure;
    disc_opts.context = client;

    pthread_mutex_lock(&client->disconnect_handle.lock);
    client->disconnect_handle.status = 1; // 断开中
    if (MQTTAsync_disconnect(client->client, &disc_opts) != MQTTASYNC_SUCCESS) {
        slog_info("[%s] [%s] Failed to disconnect from MQTT broker\n", client->url, client->name);
        client->disconnect_handle.status = 3; // 断开失败
        pthread_mutex_unlock(&client->disconnect_handle.lock);
        return;
    }

    // 等待断开成功或失败
    while (client->disconnect_handle.status == 1) {
        pthread_cond_wait(&client->disconnect_handle.cond, &client->disconnect_handle.lock);
    }
    pthread_mutex_unlock(&client->disconnect_handle.lock);

    if (client->disconnect_handle.status == 3) {
        slog_info("[%s] [%s] Disconnect failed\n", client->url, client->name);
        return;
    }

    client->is_connected = 0;
}
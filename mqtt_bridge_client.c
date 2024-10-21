#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <sys/epoll.h>
#include <errno.h>
#include "mqtt_client.h"
#include "uniq_serial_nums.h"
#include "cache.h"
#include "json.h"
#include "comm_types.h"

#define localServerURI "tcp://localhost:1883"
#define cloudServerURI "tcp://192.168.21.252:1883"

#define localClientId "mqtt_client_local_ems"

// 本地客户端
MqttClient *localClient = NULL;
// 云平台客户端
MqttClient *cloudClient = NULL;

static Cache g_mqtt_init_data_cache;

// 本地客户端消息总检查
int local_message_check(void* context, char* topicName, int topicLen, MQTTAsync_message* message)
{
    char buffer[10240] = {0};
    memcpy(buffer, message->payload, message->payloadlen);

    json_object *json_obj = json_tokener_parse(buffer);
    if (json_obj == NULL)
    {
        return -1;
    }

    json_object *head_obj = json_object_object_get(json_obj, "head");
    if (head_obj == NULL)
    {
        json_object_put(json_obj);
        return -1;
    }

    json_object *source_obj = json_object_object_get(head_obj, "source");
    if (source_obj == NULL)
    {
        json_object_put(json_obj);
        return -1;
    }

    json_object *dest_obj = json_object_object_get(head_obj, "dest");
    if (dest_obj == NULL)
    {
        json_object_put(json_obj);
        return -1;
    }

    int source = json_object_get_int(source_obj);
    int dest = json_object_get_int(dest_obj);

    // local_ems是ECU-->云平台，所以source肯定是0，dest是2或者3
    if (source != 0 || (GET(dest, 1) == 0))
    {
        json_object_put(json_obj);
        return -1;
    }

    json_object_put(json_obj);
    return 0;
}

int cloud_message_check(void* context, char* topicName, int topicLen, MQTTAsync_message* message)
{
    char buffer[10240] = {0};
    memcpy(buffer, message->payload, message->payloadlen);

    json_object *json_obj = json_tokener_parse(buffer);
    if (json_obj == NULL)
    {
        json_object_put(json_obj);
        return -1;
    }

    json_object *head_obj = json_object_object_get(json_obj, "head");
    if (head_obj == NULL)
    {
        json_object_put(json_obj);
        return -1;
    }

    json_object *source_obj = json_object_object_get(head_obj, "source");
    if (source_obj == NULL)
    {
        json_object_put(json_obj);
        return -1;
    }

    // upload是云平台-->ECU，来源肯定是云平台，判断来源即可
    int source = json_object_get_int(source_obj);
    if (GET(source, 1) == 0)
    {
        json_object_put(json_obj);
        return -1;
    }

    json_object_put(json_obj);

    return 0;
}

// 本地服务器的具体消息处理函数，注册给本地服务器客户端
// 经过total处理的消息，直接转发即可；如果是初始数据，且云平台没链接上，则缓存
int local_message_handler(void* context, char* topicName, int topicLen, MQTTAsync_message* message)
{
    // MqttClient *client = (MqttClient *)context;
    // printf("Local message arrived\n");
    // printf("     topic: %s\n", topicName);
    // printf("   message: %.*s\n", message->payloadlen, (char *)message->payload);
    if (cloudClient->is_connected == 0)
    {
        char topic[256] = {0};
        sprintf(topic, "/logger/%s/realData/0", generate_station_id((char[64]){0}, MODE_DEFAULT));
        if (strcmp(topicName, topic) == 0)
        {
            // 如果是初始信息，且服务器没链接上，则缓存，链接上之后在直接进行发送
            write_cache(&g_mqtt_init_data_cache, message->payload, message->payloadlen);
        }
        return 0;
    }
    // 转发到云平台
    mqttClient_publish(cloudClient, topicName, (char *)message->payload, message->payloadlen, message->qos, message->retained);
    return 0;
}

// 云平台的消息处理函数，注册给云平台
// 云平台的消息，直接转发到本地服务器
int cloud_message_handler(void* context, char* topicName, int topicLen, MQTTAsync_message* message)
{
    // MqttClient *client = (MqttClient *)context;
    // printf("Cloud message arrived\n");
    // printf("     topic: %s\n", topicName);
    // printf("   message: %.*s\n", message->payloadlen, (char *)message->payload);

    // 转发到本地服务器
    mqttClient_publish(localClient, topicName, (char *)message->payload, message->payloadlen, message->qos, message->retained);
    return 0;
}

// 连接状态变化回调函数，此函数注册给云平台客户端；
// 当断开触发式，向本地客户端发送和云平台的连接状态；
// 当连接触发式，读取缓存，如果有数据，则向云平台发送缓存的数据
void connection_status_changed(void *context, int isConnected)
{
    if (isConnected)
    {
        mqttClient_publish(localClient, "cloud_server_connection_status", "\x01\x00", 2, 2, 1);
        // 发送缓存的初始信息
        void *message = NULL;
        size_t size = 0;
        char topic[128];
        while ((message = read_cache(&g_mqtt_init_data_cache, &size)) != NULL)
        {
            memset(topic, 0, sizeof(topic));
            sprintf(topic, "/logger/%s/realData/0", generate_station_id((char[64]){0}, MODE_DEFAULT));
            mqttClient_publish(cloudClient, topic, (char *)message, size, 2, 1);
            free(message);
        }
    }
    else
    {
        mqttClient_publish(localClient, "cloud_server_connection_status", "\x00\x00", 2, 2, 1);
    }
}

void mqtt_bridge_client_init(void)
{
    sigset_t signal_mask;                                    // 创建一个信号
    sigemptyset(&signal_mask);                               // 将信号集初始化并情况
    sigaddset(&signal_mask, SIGPIPE);                        // 将SIGPIPE信号加入信号集中
    int rc = pthread_sigmask(SIG_BLOCK, &signal_mask, NULL); // 设置信号屏蔽码
    if (rc != 0)
    {
        printf("block sigpipe error\n");
    }


    localClient = mqttClient_init(localServerURI, localClientId, NULL, local_message_check);
    if (localClient == NULL)
    {
        fprintf(stderr, "Failed to initialize local MQTT client\n");
        return;
    }

    cloudClient = mqttClient_init(cloudServerURI, generate_client_name((char[23]){0}), connection_status_changed, cloud_message_check);
    if (cloudClient == NULL)
    {
        fprintf(stderr, "Failed to initialize cloud MQTT client\n");
        mqttClient_deinit(localClient);
        return;
    }

    mqttClient_subscribe(localClient, "/logger/#", 2, local_message_handler);

    mqttClient_subscribe(cloudClient, "/logger/+/ctrlData", 2, cloud_message_handler);
}

void mqtt_bridge_client_deinit(void)
{
    mqttClient_deinit(localClient);
    mqttClient_deinit(cloudClient);
    free_cache(&g_mqtt_init_data_cache);
}
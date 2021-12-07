<?php
/**
 * Created by PhpStorm.
 * User: ctz
 * Date: 2021/12/3
 * Time: 15:42
 */
require_once __DIR__ . '/../vendor/autoload.php';
use Pack\Tool\MqTool\Puslar\SwoolePulsarClient;
SwoolePulsarClient::getInstance()
    ->setConf([
        "host" => "111.111.0.60",
        "port"=>  "8080",
        "tenant"=>"socket",
        "namespace"=>"socket_namespace",
        "topic"=>"socket_topic",
        "subname"=>"ctz_test",
        "timer_tick"=>60,//消费任务 60秒 ping一下
    ])->consumerMsg(function ($msgReceive){
        echo base64_decode($msgReceive['payload']).PHP_EOL;
        return true;
    });
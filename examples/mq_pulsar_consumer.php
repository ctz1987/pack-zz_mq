<?php
/**
 * Created by PhpStorm.
 * User: ctz
 * Date: 2021/11/22
 * Time: 10:58
 */
require_once __DIR__ . '/../vendor/autoload.php';
use Pack\Tool\MqTool\Puslar\PulsarClient;
$a=PulsarClient::getInstance()
    ->setConf([
        "url" => "111.111.0.60:8080",
        "tenant"=>"socket",
        "namespace"=>"socket_namespace",
        "topic"=>"socket_topic",
        "subname"=>"ctz_test",
    ])->consumerMsg(function ($msgReceive){
        echo base64_decode($msgReceive['payload']).PHP_EOL;
        return true;
    });
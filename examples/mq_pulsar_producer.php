<?php
/**
 * Created by PhpStorm.
 * User: ctz
 * Date: 2021/11/22
 * Time: 10:52
 */
ini_set('date.timezone','Asia/Shanghai');
require_once __DIR__ . '/../vendor/autoload.php';
use Pack\Tool\MqTool\Puslar\PulsarClient;
$a=PulsarClient::getInstance()
    ->setConf([
        "url" => "111.111.0.60:8080",
        "tenant"=>"socket",
        "namespace"=>"socket_namespace",
        "topic"=>"socket_topic",
    ]);
for($i=0;$i<40;$i++) {
    $a->producerMsg(["websocket"=>$i,"ddd" => date("Y-m-d H:i:s")]);
}

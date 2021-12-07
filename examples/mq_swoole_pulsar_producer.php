<?php
/**
 * Created by PhpStorm.
 * User: ctz
 * Date: 2021/12/3
 * Time: 15:42
 */
ini_set('date.timezone','Asia/Shanghai');
require_once __DIR__ . '/../vendor/autoload.php';
use Pack\Tool\MqTool\Puslar\SwoolePulsarClient;

$a=SwoolePulsarClient::getInstance()
    ->setConf([
        "host" => "111.111.0.60",
        "port"=>  "8080",
        "tenant"=>"socket",
        "namespace"=>"socket_namespace",
        "topic"=>"socket_topic",
        "subname"=>"ctz_test",
    ]);
for($i=0;$i<30;$i++) {
    $a->producerMsg(["ddd2"=>$i,"ddd" => date("Y-m-d H:i:s")]);
}
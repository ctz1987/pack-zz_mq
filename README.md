# php mq utils
#### How To Use PulsarClient(pulsar websocket)
```php
  $a=PulsarClient::getInstance()
    ->setConf([
        "url" => "111.111.0.60:8080",
        "tenant"=>"socket",
        "namespace"=>"socket_namespace",
        "topic"=>"socket_topic",
    ]);
  for($i=0;$i<40;$i++) {
    $a->producerMsg(["d"=>$i,"ddd" => time()]);
    sleep(1);
  }

 PulsarClient::getInstance()
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


```

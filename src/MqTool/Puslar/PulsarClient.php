<?php
/**
 * Created by PhpStorm.
 * User: ctz
 * Date: 2021/11/22
 * Time: 09:42
 */

namespace Pack\Tool\MqTool\Puslar;
use WebSocket\Client;

/**
 * pulsar web socket
 * Class PulsarClient
 * @package Pack\Tool\MqTool\Puslar
 * @resource https://pulsar.apache.org/docs/en/client-libraries-websocket/
 */
final class PulsarClient
{
    protected $pulsarConf=[];

    private static $_instance = NULL;

    public static function getInstance()
    {
        if (is_null(self::$_instance)) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    public function checkConfig($configArray = []) {
        if ( ! array_key_exists("url", $configArray)) {
            return false;
        }

        if ( ! array_key_exists("tenant", $configArray)) {
            return false;
        }

        if ( ! array_key_exists("namespace", $configArray)) {
            return false;
        }


        if ( ! array_key_exists("topic", $configArray)) {
            return false;
        }

        return true;
    }

    /**
     * @param $pulsarConfig pulsar配置
     * @return $this
     */
    public function setConf($pulsarConfig){
        if($this->checkConfig($pulsarConfig)){
            $this->pulsarConf=$pulsarConfig;
        }
        return $this;
    }

    /**
     * 获取配置
     * @return array
     */
    public function getConf(){
        return $this->pulsarConf;
    }

    /**
     * 生产者 pulsar ws链接
     * @param $config
     * @return string
     */
    protected function getProducerWsUrl(){
        if($this->checkConfig($this->pulsarConf)){
            return sprintf(
                "ws://%s/ws/v2/producer/persistent/%s/%s/%s",
                $this->pulsarConf['url'],$this->pulsarConf['tenant'],$this->pulsarConf['namespace'],$this->pulsarConf['topic']
            );
        }
        return "";
    }

    /**
     * 消费者 pulsar ws链接
     * @return string
     */
    protected function getConsumerWSUrl(){
        if($this->checkConfig($this->pulsarConf)){
            return sprintf(
                "ws://%s/ws/v2/consumer/persistent/%s/%s/%s/%s",
                $this->pulsarConf['url'],$this->pulsarConf['tenant'],$this->pulsarConf['namespace'],$this->pulsarConf['topic'],$this->pulsarConf['subname'] ?? ""
            );
        }
        return "";
    }

    /**
     * 生产者端
     * @param $config
     * @return null|Client
     */
    protected function producerClient(){
        $wsUrl=$this->getProducerWsUrl($this->pulsarConf);
        static $pulsarProducerClients=[];
        if($wsUrl){
            $wsUrlHash=md5($wsUrl);
            if(isset($pulsarProducerClients[$wsUrlHash])){
                return $pulsarProducerClients[$wsUrlHash];
            }else{
                $pulsarProducerClients[$wsUrlHash]=new Client($wsUrl);
                return $pulsarProducerClients[$wsUrlHash];
            }
        }
        return null;
    }

    /**
     * @param $config
     * @return Client
     * @throws \Exception
     */
    protected function consumerClient(){
        $wsUrl=$this->getConsumerWSUrl($this->pulsarConf);
        static $pulsarConsumerClients=[];
        if($wsUrl){
            $wsUrlHash=md5($wsUrl);
            if(isset($pulsarConsumerClients[$wsUrlHash])){
                return $pulsarConsumerClients[$wsUrlHash];
            }else{
                $pulsarConsumerClients[$wsUrlHash]=new Client($wsUrl);
                return $pulsarConsumerClients[$wsUrlHash];
            }
        }
        return null;
    }

    public function producerMsg($message){
        $producerClient=$this->producerClient();
        if($producerClient==null){
            throw new \Exception("pulsar_producerMsg_exception:".json_encode($this->getConf()));
        }
        //判断是否是数组类型
        if (is_array($message)) {
            $message = json_encode($message,JSON_UNESCAPED_UNICODE);
        }

        //发送前需要base64
        $base64Message = base64_encode($message);

        $prepareSendMessage = [
            "payload"    => $base64Message,
            "properties" => new \stdClass(),
            "context"    => "",
            "key"        => "",
        ];

        $sendMessage = json_encode($prepareSendMessage);
        $producerClient->text($sendMessage);
        $sendResult = $producerClient->receive();
        try {
            $result = json_decode($sendResult, true);
            if (array_key_exists("result", $result) &&
                $result['result'] == "ok") {
                return true;

            } else {
                throw new \Exception("pulsar_websocket_send:".
                    $sendResult);
            }
        } catch (\Exception $e) {
            throw new \Exception("pulsar_send_exception:".$e->getMessage());
        }

    }

    public function consumerMsg(callable $func){
        $consumerClient=$this->consumerClient();
        if($consumerClient==null){
            throw new \Exception("pulsar_consumerMsg_exception:".json_encode($this->getConf()));
        }
        while (true) {
            try {
                $result = $consumerClient->receive();
                $resultArray = json_decode($result, true);
                $functionResult = call_user_func($func, $resultArray);
                if ($functionResult) {
                    $ackMsg = ['messageId' => $resultArray['messageId']];
                    $consumerClient->text(json_encode($ackMsg));
                }
            } catch (\Exception $e) {
                continue;
                //throw new \Exception("pulsar_consumeer_exception:".$e->getMessage());
                //exit;
            }
        }
    }
}
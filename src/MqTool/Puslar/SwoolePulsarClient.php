<?php
/**
 * Created by PhpStorm.
 * User: ctz
 * Date: 2021/11/22
 * Time: 09:42
 */

namespace Pack\Tool\MqTool\Puslar;
use function Sodium\crypto_aead_chacha20poly1305_encrypt;
use \Swoole\Coroutine\Http\Client;
use function Swoole\Coroutine\run;

/**
 * pulsar web socket
 * Class PulsarClient
 * @package Pack\Tool\MqTool\Puslar
 * @resource https://pulsar.apache.org/docs/en/client-libraries-websocket/
 */
final class SwoolePulsarClient
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
        if ( ! array_key_exists("host", $configArray)) {
            return false;
        }
        if ( ! array_key_exists("port", $configArray)) {
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
        !isset($pulsarConfig) && $pulsarConfig['timer_tick']=60;//默认60s
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
     * 生产者 pulsar 链接
     * @return string
     */
    protected function getProducerWsUrl(){
        if($this->checkConfig($this->pulsarConf)){
            return sprintf(
                "/ws/v2/producer/persistent/%s/%s/%s",
                $this->pulsarConf['tenant'],$this->pulsarConf['namespace'],$this->pulsarConf['topic']
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
                "/ws/v2/consumer/persistent/%s/%s/%s/%s",
               $this->pulsarConf['tenant'],$this->pulsarConf['namespace'],$this->pulsarConf['topic'],$this->pulsarConf['subname'] ?? ""
            );
        }
        return "";
    }

    /**
     * 生产者端
     * @return null|Client
     */
    protected function producerClient(){
        static $pulsarProducerClients=[];
        $wsUrlHash=md5($this->pulsarConf['host'].$this->pulsarConf['port']);
        if(isset($pulsarProducerClients[$wsUrlHash])){
            return $pulsarProducerClients[$wsUrlHash];
        }else{
            $pulsarProducerClients[$wsUrlHash]=new Client(
                $this->pulsarConf['host'],
                $this->pulsarConf['port'],
                false
                );
            $client=new Client(
                $this->pulsarConf['host'],
                $this->pulsarConf['port'],
                false
            );
            $client->setHeaders([
                'User-Agent' => sprintf('PHP %s/Swoole %s', PHP_VERSION, SWOOLE_VERSION),
            ]);
            $client->set([ 'timeout' => 1]);
            $pulsarProducerClients[$wsUrlHash]=$client;
            return $pulsarProducerClients[$wsUrlHash];
        }

    }

    /**
     * @param bool $isReConnect 是否重连
     * @return Client
     * @throws \Exception
     */
    protected function consumerClient(){
        static $pulsarConsumerClients=[];
        $wsUrlHash=md5($this->pulsarConf['host'].$this->pulsarConf['port']);
        if(isset($pulsarConsumerClients[$wsUrlHash])){
            return $pulsarConsumerClients[$wsUrlHash];
        }else{
            $client=new Client(
                $this->pulsarConf['host'],
                $this->pulsarConf['port'],
                false
            );
            $client->setHeaders([
                'User-Agent' => sprintf('PHP %s/Swoole %s', PHP_VERSION, SWOOLE_VERSION),
            ]);
            $client->set([
                'timeout' => 1,
                'websocket_compression'=>true
            ]);
            $pulsarConsumerClients[$wsUrlHash]=$client;
            return $pulsarConsumerClients[$wsUrlHash];
        }
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
        $wsUrl=$this->getProducerWsUrl();
        run(function ()use($producerClient,$sendMessage,$wsUrl){
            $producerClient->upgrade($wsUrl);
            $producerClient->push($sendMessage);
            $producerClient->recv();
            $producerClient->close();
        });
    }

    public function consumerMsg(callable $func){
        $consumerClient=$this->consumerClient();
        if($consumerClient==null){
            throw new \Exception("swoole_pulsar_consumerMsg_exception:".json_encode($this->getConf()));
        }
        $wsUrl=$this->getConsumerWSUrl();
        run(function () use ($consumerClient,$func,$wsUrl){
            upgrade:
            $ret=$consumerClient->upgrade($wsUrl);
            if($ret){
                //每分钟ping一下ws连接
                \Swoole\Timer::tick($this->pulsarConfig['timer_tick']*1000, function() use($consumerClient){
                    $pingFrame = new \Swoole\WebSocket\Frame;
                    $pingFrame->opcode = WEBSOCKET_OPCODE_PING;
                    // 发送 PING
                    $consumerClient->push($pingFrame);
                });
                while(true) {
                    try {
                        $result = $consumerClient->recv();
                        if(!$result){
                            $consumerClient->errCode;
                            if(!$consumerClient->connected){
                                goto upgrade;
                            }
                            continue;
                        }else{
                            // 接收 PONG
                            if($result->opcode === WEBSOCKET_OPCODE_PONG){
                                continue;
                            }
                        }
                        $resultArray = json_decode($result->data, true);
                        $functionResult = call_user_func($func, $resultArray);
                        if ($functionResult) {
                            $ackMsg = ['messageId' => $resultArray['messageId']];
                            $consumerClient->push(json_encode($ackMsg));
                        }
                        \Swoole\Coroutine::sleep(0.1);
                    }catch (\Swoole\Exception $e){
                        continue;
                    }
                }
            }
        });
    }
}
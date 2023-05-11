<?php


namespace Zzmed\ZzMqTools\Pulsar;
use WebSocket\Client;
use WebSocket\TimeoutException;

/**
 * Class PulsarTool
 * @package Zzmed\ZzMqTools\Pulsar
 * @example 见 example/Pulsar/
 */
class PulsarTool
{
    /**
     * @var bool 是否设置log
     */
    public static $isSetLog=true;
    /**
     * 是否 "否定确认消息"
     * @var bool
     */
    public static $isNAckMsg=false;
    /**
     * socket 连接超时时间(秒)
     * @var int
     */
    public static $wsTimeOut=30;
    /**
     * 未定义消费者名称的时候 是否使用hostName true:使用hostName false:pulsar自动生成的消费者名称
     * @var bool
     */
    public static $consumerNameUseHostName=true;

    /**
     * @var int
     */
    public static $wsFragmentSize=40960;
    /**
     * 生产者端
     * @param $wsUrl
     * @return null|Client
     */
    protected static function producerClient($wsUrl)
    {
        static $pulsarProducerClients = [];
        if ($wsUrl) {
            $wsUrlHash = md5($wsUrl);
            if (isset($pulsarProducerClients[$wsUrlHash])) {
                return $pulsarProducerClients[$wsUrlHash];
            } else {
                $pulsarProducerClients[$wsUrlHash] = new Client($wsUrl,['timeout'=>self::$wsTimeOut,'fragment_size'=>self::$wsFragmentSize]);
                return $pulsarProducerClients[$wsUrlHash];
            }
        }
        return null;
    }

    /**
     * 生成消费者名称
     * @return string
     */
    protected static function generateConsumerName(){
        if (function_exists('gethostname')) {
            $hostName=gethostname();
        }else if(function_exists('php_uname')){
            $hostName=php_uname('n');
        }else{
            $hostName="UnknownHostname";
        }
        $pid=getmypid()?:uniqid();
        return $hostName."-".$pid;
    }

    /**
     * 消费者端
     * @param $wsUrl
     * @return Client
     * @throws \Exception
     */
    protected static function consumerClient($wsUrl)
    {
        static $pulsarConsumerClients = [];
        if ($wsUrl) {
            if(self::$consumerNameUseHostName){
                $parseUrl=parse_url($wsUrl);
                parse_str($parseUrl["query"]??"",$wsParams);
                $consumerName=$wsParams['consumerName']??"";
                if(!$consumerName){
                    $wsParams['consumerName']=self::generateConsumerName();
                    $port=(isset($parseUrl["port"]) && $parseUrl["port"])?":".$parseUrl["port"]:"";
                    $wsUrl=$parseUrl["scheme"]."://".$parseUrl["host"].$port.$parseUrl["path"]."?".http_build_query($wsParams);
                }
            }
            $wsUrlHash = md5($wsUrl);
            if (isset($pulsarConsumerClients[$wsUrlHash])) {
                return $pulsarConsumerClients[$wsUrlHash];
            } else {
                $pulsarConsumerClients[$wsUrlHash] = new Client($wsUrl,["timeout"=>self::$wsTimeOut]);
                return $pulsarConsumerClients[$wsUrlHash];
            }
        }
        return null;
    }

    /**
     * 生产者
     * @param $pulsarProducerWsUrl pulsar websocket生产者地址
     * @param $message
     * @param int $deliverAfterMs 延迟消息 线上pulsar版本2.7.1 不支持
     * @return bool
     * @throws \Exception
     * @links https://pulsar.apache.org/docs/client-libraries-websocket/#producer-endpoint
     */
    public static function producerMsg($pulsarProducerWsUrl,$message,$deliverAfterMs=0)
    {
        $producerClient = self::producerClient($pulsarProducerWsUrl);
        if ($producerClient == null) {
            throw new \Exception("pulsar_producerMsg_exception:" . $pulsarProducerWsUrl);
        }
        //判断是否是数组类型
        if (is_array($message)) {
            $message = json_encode($message, JSON_UNESCAPED_UNICODE);
        }

        //发送前需要base64
        $base64Message = base64_encode($message);

        $prepareSendMessage = [
            "payload"    => $base64Message,
            "properties" => new \stdClass(),
            "context"    => "",
            "key"        => "",
        ];
        if($deliverAfterMs){
            $prepareSendMessage["deliverAfterMs"]=$deliverAfterMs;
        }

        $sendMessage = json_encode($prepareSendMessage);
        try {
            $producerClient->text($sendMessage);
            $sendResult=$producerClient->receive();
            $producerClient->close();
            $result = json_decode($sendResult, true);
            if ($result && array_key_exists("result", $result) &&
                $result['result'] == "ok") {
                return true;
            } else {
                throw new \Exception("pulsar_websocket_send:" .
                    $sendResult);
            }
        } catch (\Exception $e) {
            throw new \Exception("pulsar_send_exception:" . $e->getMessage());
        } catch (\Throwable $e2){
            throw new \Exception("pulsar_send_exception1:" . $e2->getMessage());
        }

    }

    /**
     * 消费者
     * @param $pulsarConsumerWsUrl pulsar websocket生产者地址
     * @param callable $func
     * @throws \Exception
     * @links https://pulsar.apache.org/docs/client-libraries-websocket/#consumer-endpoint
     */
    public static function consumerMsg($pulsarConsumerWsUrl,callable $func)
    {
        $consumerClient = self::consumerClient($pulsarConsumerWsUrl);
        if ($consumerClient == null) {
            throw new \Exception("pulsar_consumerMsg_exception:" . $pulsarConsumerWsUrl);
        }
        if(self::$isSetLog) {
            $baseLogInfo = [
                "app_env"  => config("app.env"),
                "app_name" => config("app.name"),
            ];
        }
        while (true) {
            try {
                $result = $consumerClient->receive();
                if($result==null){
                    continue;
                }
                if(self::$isSetLog) {
                    \Log::info("pulsar_receive_msg:",
                        array_merge($baseLogInfo, [
                            "receive_time" => date("Y-m-d H:i:s"),
                            "receive_data" => $result
                        ])
                    );
                }
                $resultArray = json_decode($result, true);
                $functionResult = call_user_func($func, $resultArray);
                $ackMsg = ['messageId' => $resultArray['messageId']];
                //确认消息
                if ($functionResult) {
                    $consumerClient->text(json_encode($ackMsg));
                }
                //否定确认消息
                if(self::$isNAckMsg && !$functionResult){
                    $ackMsg['type']="negativeAcknowledge";
                    $consumerClient->text(json_encode($ackMsg));
                    print_r($ackMsg);
                }
            }
            catch (TimeoutException $e1){
                echo date("Y-m-d H:i:s")."-".$e1->getMessage().PHP_EOL;
                //超时
                continue;
            } catch (\Throwable $e) {
                //连接超时 默认5S连接超时
                if($e->getCode()==1024){
                    echo date("Y-m-d H:i:s")."|".$e->getMessage().PHP_EOL;
                    continue;
                }
                if(self::$isSetLog) {
                    \Log::error("pulsar_receive_error:", [
                        $e->getMessage(),
                        $e->getCode(),
                        $e->getLine()
                    ]);
                }
                exit($e->getMessage());
            }
        }
    }
}
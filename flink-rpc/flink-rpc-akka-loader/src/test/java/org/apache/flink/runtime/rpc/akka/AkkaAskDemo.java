package org.apache.flink.runtime.rpc.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class AkkaAskDemo {
    private static final Logger log = LoggerFactory.getLogger(AkkaAskDemo.class);

    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create();
        ActorRef actor = actorSystem.actorOf(Props.create(ActorNormal.class), "actorNormal");
        Timeout timeout = new Timeout(Duration.create(2, TimeUnit.SECONDS));
        Future<Object> future = Patterns.ask(actor, "我是接收游戏返回值", timeout);
        try {
            Object obj = Await.result(future, timeout.duration());
            String reply = obj.toString();
            System.out.println("回复的消息: " + reply);//返回值获取不到
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

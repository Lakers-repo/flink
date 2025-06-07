package org.apache.flink.runtime.rpc.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class AkkaTellDemo {

    private static final Logger log = LoggerFactory.getLogger(AkkaTellDemo.class);

    public static void main(String[] args) {

        //创建所有管理actor的系统管理对象
        ActorSystem actorSystem = ActorSystem.create();
        //通过这个系统管理对象创建actor，并返回当前actor的地址，可以理解成现实生活中用户的一个邮箱地址
        //使用actorSystem.actorOf定义一个名为actorNormal的ActorRef
        ActorRef actor = actorSystem.actorOf(Props.create(ActorNormal.class), "actorNormal");
        //发送消息Object msg(发送消息的内容，任何类型的数据), final ActorRef sender(表示没有发送者(其实是一个叫做deadLetters的Actor))
        actor.tell("kiba", ActorRef.noSender());
    }
}

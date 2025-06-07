package org.apache.flink.runtime.rpc.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class AkkaForwardExample {

    // 计算 Actor
    static class CalculatorActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Integer.class, n -> {
                        // 计算平方
                        int result = n * n;
                        // 将结果发送回去
                        System.out.println(result);
//                        getSender().tell(String.valueOf(result), getSelf());
                    })
                    .build();
        }
    }

    // 转发 Actor
    static class ForwarderActor extends AbstractActor {
        private final ActorRef calculatorActor;

        public ForwarderActor(ActorRef calculatorActor) {
            this.calculatorActor = calculatorActor;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Integer.class, n -> {
                        // 将消息转发给 CalculatorActor
                        calculatorActor.forward(n, getContext());
                    })
                    .build();
        }
    }

    // 主程序
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("MyActorSystem");

        // 创建 CalculatorActor 和 ForwarderActor
        ActorRef calculatorActor = system.actorOf(Props.create(CalculatorActor.class), "calculatorActor");
        ActorRef forwarderActor = system.actorOf(Props.create(ForwarderActor.class, calculatorActor), "forwarderActor");

        // 发送请求给 ForwarderActor
        forwarderActor.tell(5, ActorRef.noSender()); // 请求计算 5 的平方

        system.terminate();
    }
}


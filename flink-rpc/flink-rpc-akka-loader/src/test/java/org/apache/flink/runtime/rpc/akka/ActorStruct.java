package org.apache.flink.runtime.rpc.akka;


import akka.actor.AbstractActor;

public class ActorStruct extends AbstractActor {
    private final User user;

    public ActorStruct(User userModel) {
        this.user = userModel;
    }

    //处理消息
    @Override
    public Receive createReceive() {
        //处理一个具体类型的消息，比如是字符串类型的消息
        return receiveBuilder().match(String.class, (msg) -> {
            System.out.println(msg);
            sender().tell("我是ActorStruct返回结果", self());
        }).match(Integer.class, (msg) -> {
            System.out.println(msg + "1");
        }).build();
    }

    class User {
        private String name;
        private int userIdentifier;

        public User() {
        }

        public User(int userIdentifier, String name) {
            this.name = name;
            this.userIdentifier = userIdentifier;
        }

        public String getName() {
            return name;
        }
    }
}

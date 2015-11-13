/**
 * Created by fwang1 on 3/25/15.
 */
module.exports = function(RED) {
    /*
     *   Kafka Producer
     *   Parameters:
     - topics 
     - zkquorum(example: zkquorum = “[host]:2181")
     */
    function kafkaNode(config) {
        RED.nodes.createNode(this,config);
        var topic = config.topic;
        var clusterZookeeper = config.zkquorum;
        var node = this;
        var kafka = require('kafka-node');
        var HighLevelProducer = kafka.HighLevelProducer;
        var Client = kafka.Client;
        var topics = config.topics;
        var client = new Client(clusterZookeeper);

        try {
            this.on("input", function(msg) {
                var payloads = [];

                // check if multiple topics
                if (topics.indexOf(",") > -1){
                    var topicArry = topics.split(',');

                    for (var i = 0; i < topicArry.length; i++) {
                        payloads.push({topic: topicArry[i], messages: msg.payload});
                    }
                }
                else {
                    payloads = [{topic: topics, messages: msg.payload}];
                }

                producer.send(payloads, function(err, data){
                    if (err){
                        node.error(err);
                    }
                    node.log("Message Sent: " + data);
                });
            });
        }
        catch(e) {
            node.error(e);
        }
        var producer = new HighLevelProducer(client);
    }

    RED.nodes.registerType("kafka",kafkaNode);


    /*
     *   Kafka Consumer
     *   Parameters:
     - topics
     - groupId
     - zkquorum(example: zkquorum = “[host]:2181")
     */
    function kafkaInNode(config) {
        RED.nodes.createNode(this,config);

        var node = this;

        var kafka = require('kafka-node');
        var topics = String(config.topics);
        var clusterZookeeper = config.zkquorum;
        var groupId = config.groupId;


        var topicJSONArry = [];

        // check if multiple topics
        if (topics.indexOf(",") > -1){
            var topicArry = topics.split(',');
            console.log(topicArry);
            console.log(topicArry.length);


            for (var i = 0; i < topicArry.length; i++) {
                console.log(topicArry[i]);
                topicJSONArry.push({topic: topicArry[i]});
            }
            topics = topicJSONArry;
        }
        else {
            topics = [{topic:topics}];
        }

        var options = {
            groupId: groupId,
            autoCommit: config.autoCommit,
            autoCommitMsgCount: 10
        };

        var currentConsumer = new retryConsumer(node, kafka, topics, clusterZookeeper, options);

        currentConsumer.run();




    }

    retryConsumer.prototype.kickstart= function(){
        var self= this;
        self.dead = true;//make sure


        setTimeout(function(){
            self.node.currentConsumer = new retryConsumer(self.node, self.kafka, self.topics, self.clusterZookeeper, self.options);
            self.node.currentConsumer.run();

        },self.restartWaitTime);

        try {
            if (self.consumer) {
                var tc = self.consumer; //switchroo make sure we set it to null before doing close
                self.consumer = null;
                tc.close();

            }
        } catch (e) {
            console.log('exception while closing:' +e);
        }

    };

    function retryConsumer(node, kafka, topics, clusterZookeeper, options){
        var self = this;

        self.node = node;
        self.clusterZookeeper = clusterZookeeper;
        self.kafka = kafka;

        self.client = new kafka.Client(clusterZookeeper);
        self.dead = false;  //a safety check to make sure that once we think this instance is dead we don't use it anymore.
        self.topics = topics;
        self.options = options;
        self.HighLevelConsumer = kafka.HighLevelConsumer;
        self.restartWaitTime = 5000;


    }

    retryConsumer.prototype.run= function run(){
        var self = this;

        try {
            var consumer = new self.HighLevelConsumer(self.client, self.topics, self.options);
            self.consumer = consumer;
            console.log("Consumer created...");

            //helps to uncomment these for issues.
            //consumer.on('done', function(message){console.log('done:'+ message);})
            //consumer.on('ready', function(message){console.log('ready:'+ message);})
            //consumer.on('connect', function(message){console.log('connect:'+ message);})
            //consumer.on('close', function(message){console.log('close:'+ message);})




            consumer.on('message', function (message) {
                if(self.dead) return;
                console.log(message);
                self.node.log(message);
                var msg = {payload: message};
                self.node.send(msg);
            });

            
            consumer.on('error', function (err) {
                if(self.dead)return;
                self.dead = true;
                console.log("CONSOLE ERROR" +err);
                self.node.error("CONSOLE ERROR" +err);
                self.kickstart();


            });
        }
        catch(e){
            if(self.dead)return;
            self.dead = true;
            console.log("catch" +e);
            self.node.error("Catch error" +e);
            self.kickstart();
        }
    };

    RED.nodes.registerType("kafka in", kafkaInNode);
};

import amqp from "amqp-connection-manager";
import { Message } from "amqplib";

class MessageQ {
  private count = 0;
    MessageQConnector: amqp.AmqpConnectionManager;
    MessageQChannel: amqp.ChannelWrapper | undefined;
    
    private url = "yourampurl";
    private options = {
      contentEncoding: "utf8",
      contentType: "application/json",
      persistent: true,
    };
    constructor() {
        this.MessageQConnector = amqp.connect([this.url], { findServers: () => this.url });
        this.MessageQConnector.on("connect", () => { console.log("Message broker connected");
        });
        this.MessageQConnector.on("disconnect", (error: any) => {  
          console.log("Message broker disconnected due to " + error.err.stack);
        });
       
    }
     
    public async init(){
        this.MessageQChannel = await this.createChannel(this.MessageQConnector);
        this.addChannelEventListeners();
    }

      private async createChannel(MessageQConnector: amqp.AmqpConnectionManager) {
        const self = this;
        return MessageQConnector.createChannel({
          json: true,
          setup: async function(channel: any) {
            return Promise.all([
                channel.assertExchange("events", "topic", { durable: true }), // listening/attaching to global topic
                channel.assertExchange("DLX_MyServiceQueue", 'fanout', {durable: true,noAck: true}), // noAck says acknowledgement messages are not required for dead letter messages 
                channel.assertExchange("TLX_MyServiceQueue", 'direct', {durable: true}), // dummy exchange to publish failed messages


                channel.assertQueue("MyServiceQueue", {durable: true}),
                channel.assertQueue("TTL-Retry-10S", {durable: true , deadLetterExchange: "DLX_MyServiceQueue", messageTtl: 10000}), // recevie message with 10sec delay
                channel.assertQueue("TTL-Retry-30S", {durable: true , deadLetterExchange: "DLX_MyServiceQueue", messageTtl: 30000}), // recevie message with 30sec delay
                channel.assertQueue("TTL-Retry-50S", {durable: true , deadLetterExchange: "DLX_MyServiceQueue", messageTtl: 50000}), // recevie message with 50sec delay


                channel.bindQueue("MyServiceQueue", "events", "event.user.#"), // listening on specefic routes
                channel.bindQueue("MyServiceQueue", "DLX_MyServiceQueue"), // attaching link between original queue and dead letter queue

                channel.bindQueue("TTL-Retry-10S","TLX_MyServiceQueue","Retry-once"), // queue for retrying failed messages
                channel.bindQueue("TTL-Retry-30S","TLX_MyServiceQueue","Retry-twice"),
                channel.bindQueue("TTL-Retry-50S","TLX_MyServiceQueue","Retry-thrice"),

                channel.consume("MyServiceQueue", self.onUserEvent.bind(self)), // consume the message
            ]);
          }
        });
      }
    
      private addChannelEventListeners() {
        if (this.MessageQChannel) {
          this.MessageQChannel.on("connect", function() {
            console.log("Message broker events for charger is connected!");
          });
          this.MessageQChannel.on("close", function() {
            console.log("Message broker events for charger is closed by .close() code call");
          });
          this.MessageQChannel.on("disconnect", function(error: Error) {
            console.log("Message broker events for charger is disconnected! Error: ", error);
          });
        }
      }

      private onUserEvent(data: Message){
        try {
          console.log(data.fields.routingKey + " key received at "+ new Date().toUTCString());
          if(this.MessageQChannel){
            // throw error to test retry mechanisms 
            throw new ErrorEvent("error")
          }
        }
        catch(error){
          this.count+= 1;
          if(this.MessageQChannel){
            // if(data.fields.routingKey !== "Retry-1" && data.fields.routingKey !== "Retry-2"  && data.fields.routingKey !== "Retry-3" ){
              this.MessageQChannel.ack(data);
            // }
           
            if(this.count === 1){
              this.MessageQChannel.publish("TLX_MyServiceQueue","Retry-once",data,this.options)
              console.log("First attempt at  " + new Date().toUTCString());
              
            }
            if(this.count === 2){
              this.MessageQChannel.publish("TLX_MyServiceQueue","Retry-twice",data,this.options)
              console.log("Second attempt at  " + new Date().toUTCString());
            }
            if(this.count === 3){
              this.MessageQChannel.publish("TLX_MyServiceQueue","Retry-thrice",data,this.options)
              console.log("third attept at  " + new Date().toUTCString());
            } 
            if(this.count>3){
              console.log("All the attempts are exceeded hence discarding the message");
              this.count = 0
            }
            return ;
          }
          return ;
        }
         

      }

}

const messageQ = new MessageQ();
export default messageQ;
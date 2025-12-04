use amqprs::channel::{BasicAckArguments, Channel};
use amqprs::consumer::AsyncConsumer;
use amqprs::{BasicProperties, Deliver};
use amqprs::FieldValue::u;
use async_trait::async_trait;
use uuid::{Bytes, Error, Uuid};
use crate::inference::SubtitleInference;

pub struct AudioConsumer {
    inference: Box<dyn SubtitleInference + Send>
}

impl AudioConsumer {
    pub fn new(inference: Box<dyn SubtitleInference + Send>) -> Self {
        Self { inference }
    }
}

#[async_trait]
impl AsyncConsumer for AudioConsumer {
    async fn consume(&mut self, channel: &Channel, deliver: Deliver, basic_properties: BasicProperties, content: Vec<u8>) {
        let uuid_result = Uuid::from_slice(&content);
        let delivery_tag = deliver.delivery_tag();

        println!("Consuming task");
        
        let task_id = match uuid_result {
            Ok(uuid) => uuid,
            Err(err) => {
                let args = amqprs::channel::BasicNackArguments::new(
                    delivery_tag,
                    false, // multiple: don't nack other messages
                    true   // requeue: put back in queue
                );
                if let Err(e) = channel.basic_nack(args).await {
                    eprintln!("Failed to send nack for {}: {}", delivery_tag, e);
                };
                return;
            }
        };

        println!("Received task for data: {}", task_id);



        if let Err(e) = channel. basic_ack(BasicAckArguments::new(delivery_tag, false)).await {
            eprintln! ("❌ Failed to ACK message: {:?}", e);
            // Message will be redelivered since ACK failed
        }
    }
}
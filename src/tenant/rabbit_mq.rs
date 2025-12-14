///! MIT License
///!
///! Copyright (c) 2025 Keorapetse Finger
///!
///! Permission is hereby granted, free of charge, to any person obtaining a copy
///! of this software and associated documentation files (the "Software"), to deal
///! in the Software without restriction, including without limitation the rights
///! to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
///! copies of the Software, and to permit persons to whom the Software is
///! furnished to do so, subject to the following conditions:
///!
///! The above copyright notice and this permission notice shall be included in all
///! copies or substantial portions of the Software.
///!
///! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
///! IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
///! FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
///! AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
///! LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
///! OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
///! SOFTWARE.

use lapin::{ 
    Channel, 
    Connection, 
    ConnectionProperties,
    BasicProperties,
    ExchangeKind,
    types::FieldTable,
    options::{ 
        ExchangeDeclareOptions,
        BasicPublishOptions,
        QueueDeclareOptions
    }
};

/// Create a RabbitMQ channel.
///
/// # Example
/// ```ignore
/// use lapin::Channel;
/// use tenant::rabbit_mq;
/// async fn create_channel_example() {
///     let channel = 
///         rabbit_mq::create_channel("amqp://guest:guest@127.0.0.1:5672/%2f")
///             .await;
/// }
/// ```
pub async fn create_channel(mq_endpoint: &str) -> Channel {
    let conn = Connection::connect(mq_endpoint, ConnectionProperties::default())
        .await
        .expect("failed to connect to RabbitMQ");

    conn.create_channel()
        .await
        .expect("failed to create RabbitMQ channel")
}

/// Create a RabbitMQ exchange for a tenant.
///
/// # Example
/// ```ignore
/// use lapin::Channel;
/// use tenant::rabbit_mq;
/// async fn create_exchange_example(channel: &Channel) {
///     rabbit_mq::create_exchange(
///         "tenant_exchange_1",
///         channel
///     ).await;
/// }
/// ```
pub async fn create_exchange(
    exchange_name: &str,
    channel: &Channel,
) {
    let options = ExchangeDeclareOptions {
        passive: false,
        durable: true,
        auto_delete: false,
        internal: false,
        nowait: false,
    };

    channel.exchange_declare(
        exchange_name, 
        ExchangeKind::Fanout,
        options,
        FieldTable::default()
    )
    .await
    .expect("failed to create RabbitMQ exchange");
}


/// Create a queue for an agent to receive messages from RabbitMQ.
///
/// # Example
/// ```ignore
/// use lapin::Channel;
/// use tenant::rabbit_mq;
/// async fn create_queue_example(channel: &Channel) {
///     rabbit_mq::create_agent_queue(
///         "agent_queue_1",
///         channel
///     ).await;
/// }
/// ```
pub async fn create_agent_queue(
    queue_name: &str,
    channel: &Channel,
) {
    channel.queue_declare(
        queue_name, 
        QueueDeclareOptions::default(),
        FieldTable::default()
    )
    .await
    .expect("failed to create RabbitMQ queue for agent");
}

/// Publish a message to a RabbitMQ exchange with a routing key.
///
/// # Example
/// ```ignore
/// use lapin::Channel;
/// use tenant::rabbit_mq;
/// 
/// async fn publish_example(channel: &Channel) {
///     rabbit_mq::publish_case(
///         "my_exchange",
///         channel,
///         "my_routing_key",
///         b"Hello, RabbitMQ!"
///     ).await;
/// }
/// ```
pub async fn publish_case(
    exchange_name: &str,
    channel: &Channel,
    routing_key: &str,
    payload: &[u8],
) {
    channel.basic_publish(
        exchange_name,
        routing_key,
        BasicPublishOptions::default(),
        payload,
        BasicProperties::default(),
    )
    .await
    .expect("failed to publish to RabbitMQ queue");
}

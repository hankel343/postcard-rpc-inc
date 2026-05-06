use postcard_rpc::TopicDirection::{ToClient, ToServer};
use postcard_rpc::postcard_schema::Schema;
use postcard_rpc::{endpoints, topics};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Schema)]
pub struct IncrementRequest {
    pub value: i32,
}

#[derive(Serialize, Deserialize, Schema)]
pub struct IncrementResponse {
    pub value: i32,
}

endpoints! {
    list = ENDPOINT_LIST;
    | EndpointTy | RequestTy | ResponseTy | Path |
    | ---------  | --------  | ---------- | ---- |
    | IncrementValue | IncrementRequest | IncrementResponse | "inc" |
}

topics! {
    list = TOPICS_IN_LIST;
    direction = ToServer;
    | TopicTy | MessageTy | Path | Cfg |
    | ------- | --------- | ---- | --- |
}

topics! {
    list = TOPICS_OUT_LIST;
    direction = ToClient;
    | TopicTy | MessageTy | Path | Cfg |
    | ------- | --------- | ---- | --- |
}

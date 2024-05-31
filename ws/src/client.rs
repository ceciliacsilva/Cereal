//! A Client for `RepositoryWS`.
//!
//! With:
//! - [`Client`] to handle `single repository` transactions and;
//! - [`Clients`] to manipulate `multi repository` transactions.
use futures_util::{SinkExt as _, StreamExt as _};
use std::net::Ipv4Addr;

use actix_web::http::Uri;
use awc::ws;
use cereal_core::{
    operations::{Arguments, Operation, Table},
    runtime::Runtime,
};
use uuid::Uuid;

use crate::MessageWs;

/// ClientBuilder.
///
/// A auxiliary Type to build `RepositoryWs` connections.
#[derive(Debug)]
pub(crate) struct ClientBuilder {
    uri: Uri,
    runtime: Runtime,
}

impl ClientBuilder {
    /// ClientBuilder::new.
    ///
    /// Creates a [ClientBuilder] from a `ip` and `port`.
    pub(crate) fn new(ip: Ipv4Addr, port: u16) -> Self {
        let uri = Uri::builder()
            .authority(format!("{ip}:{port}"))
            .scheme("http")
            .path_and_query("/ws/")
            .build()
            .unwrap();

        let runtime = Runtime::new();

        ClientBuilder { uri, runtime }
    }

    /// Create a [Client] `build`ing a the current [ClientBuilder].
    pub(crate) async fn build(self) -> Client {
        let (_resp, connection) = awc::Client::new()
            .ws(self.uri.clone())
            .connect()
            .await
            .unwrap();

        Client {
            connection,
            runtime: self.runtime,
            uri: self.uri,
        }
    }
}

/// Client.
///
/// Holds a WebSocket `connection`.
pub(crate) struct Client {
    connection: actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
    runtime: Runtime,
    uri: Uri,
}

impl Client {
    /// Sends operations to a single repository as a `single repository transaction`.
    pub(crate) async fn send_single(
        &mut self,
        operations: Vec<Operation>,
    ) -> anyhow::Result<Option<Table>> {
        let tid = Uuid::new_v4();
        let args = Arguments {
            timestamp: self.runtime.now(),
            operations,
        };

        let msg = serde_json::to_string(&MessageWs::Single { tid, args })
            .expect("this can be serialized");

        self.connection
            .send(ws::Message::Text(msg.into()))
            .await
            .unwrap();

        let res = self.connection.next().await.unwrap()?;
        let vote = decoder::frame_to_commit_vote(&res)?;
        log::info!("Result from {:?} single: {:?}", tid, vote);

        self.get_result(&tid).await
    }

    /// Sends a `GetResult` message to a `repository` asking to the result of
    /// transaction with the given `tid`.
    async fn get_result(&mut self, tid: &Uuid) -> anyhow::Result<Option<Table>> {
        let msg = serde_json::to_string(&MessageWs::GetResult { tid: *tid })
            .expect("this can be serialized");

        self.connection
            .send(ws::Message::Text(msg.into()))
            .await
            .unwrap();

        let result = self.connection.next().await.unwrap()?;

        log::info!("Result from get_result: {:?}", result);

        let table = decoder::frame_to_table(&result)?;

        Ok(table)
    }
}

/// For multi-repository transactions.
///
/// Holds a [std::vec::Vec] with a list of [Client] s that will
/// participate in a `multi repository transaction`.
pub(crate) struct Clients<'a> {
    pub(crate) participants: Vec<&'a mut Client>,
}

impl<'a> Clients<'a> {
    /// Sends the needed messages for all the participants of a `independent` transaction.
    pub(crate) async fn send_indep(
        &'a mut self,
        operations: Vec<Vec<Operation>>,
    ) -> anyhow::Result<Vec<Option<Table>>> {
        let tid = Uuid::new_v4();
        let participants_len = self.participants.len();
        let participants_address: Vec<String> = self
            .participants
            .iter()
            .map(|p| p.uri.to_string())
            .collect();

        let mut votes = vec![];

        for (participant, operations) in self.participants.iter_mut().zip(operations) {
            let args = Arguments {
                timestamp: participant.runtime.now(),
                operations,
            };

            let msg = serde_json::to_string(&MessageWs::Indep {
                tid,
                args,
                participants_size: participants_len,
            })
            .expect("this can be serialized");

            participant
                .connection
                .send(ws::Message::Text(msg.into()))
                .await
                .unwrap();

            let result = participant.connection.next().await.unwrap()?;
            let vote = decoder::frame_to_commit_vote(&result)?;
            log::info!("Result from {:?} indep: {:?}", tid, vote);
            votes.push(vote);
        }

        for (participant, vote) in self.participants.iter_mut().zip(votes) {
            log::debug!("participant: {:?}", participant.uri);
            let msg = serde_json::to_string(&MessageWs::IndepParticipants {
                tid,
                vote,
                participants: participants_address.clone(),
            })
            .expect("this can be serialized");

            participant
                .connection
                .send(ws::Message::Text(msg.into()))
                .await
                .unwrap();

            let res = participant.connection.next().await.unwrap()?;
            log::info!("Result from {:?} indep participants: {:?}", tid, res);
        }

        let mut results = vec![];
        for participant in self.participants.iter_mut() {
            let result = participant.get_result(&tid).await?;
            log::debug!("`get_result` from indep, {:?}: {:?}", tid, result);
            results.push(result);
        }

        Ok(results)
    }

    /// Sends the needed messages for a `coordinated` transaction.
    pub(crate) async fn send_coord(
        &'a mut self,
        operations: Vec<Vec<Operation>>,
    ) -> anyhow::Result<Vec<Option<Table>>> {
        let tid = Uuid::new_v4();
        let participants_len = self.participants.len();
        let participants_address: Vec<String> = self
            .participants
            .iter()
            .map(|p| p.uri.to_string())
            .collect();

        let mut votes = vec![];

        for (participant, operations) in self.participants.iter_mut().zip(operations) {
            let args = Arguments {
                timestamp: participant.runtime.now(),
                operations,
            };

            let msg = serde_json::to_string(&MessageWs::Coord {
                tid,
                args,
                participants_size: participants_len,
            })
            .expect("this can be serialized");

            participant
                .connection
                .send(ws::Message::Text(msg.into()))
                .await
                .unwrap();

            let result = participant.connection.next().await.unwrap()?;
            let vote = decoder::frame_to_commit_vote(&result)?;
            log::info!("Result from {:?} coord: {:?}", tid, vote);
            votes.push(vote);
        }

        for (participant, vote) in self.participants.iter_mut().zip(votes) {
            log::debug!("participant: {:?}", participant.uri);
            let msg = serde_json::to_string(&MessageWs::CoordParticipants {
                tid,
                vote,
                participants: participants_address.clone(),
            })
            .expect("this can be serialized");

            participant
                .connection
                .send(ws::Message::Text(msg.into()))
                .await
                .unwrap();

            let res = participant.connection.next().await.unwrap()?;
            log::info!("Result from {:?} coord participants: {:?}", tid, res);
        }

        let mut results = vec![];
        for participant in self.participants.iter_mut() {
            let result = participant.get_result(&tid).await?;
            log::debug!("`get_result` from coord, {:?}: {:?}", tid, result);
            results.push(result);
        }

        Ok(results)
    }
}

// TODO: this deserves a better implementation.
//
// It's very easy to encode/decode messages in the wrong way.
mod decoder {
    use actix_web_actors::ws::Frame;
    use awc::ws;
    use cereal_core::{messages::CommitVote, operations::Table};
    use std::str;

    use crate::GetResultResponse;

    /// Try to decode a `Frame` as a [cereal_core::messages::CommitVote].
    pub(crate) fn frame_to_commit_vote(frame: &Frame) -> anyhow::Result<CommitVote> {
        let vote = match frame {
            ws::Frame::Text(text) => Ok(text),
            ws::Frame::Binary(_)
            | ws::Frame::Continuation(_)
            | ws::Frame::Ping(_)
            | ws::Frame::Pong(_)
            | ws::Frame::Close(_) => anyhow::bail!("Not a `ws::Frame::Text`"),
        };

        vote.and_then(|vote| Ok(serde_json::from_str(str::from_utf8(&vote)?)?))
    }

    /// Try to decode a `Frame` as a [cereal_core::operations::Table].
    pub(crate) fn frame_to_table(frame: &Frame) -> anyhow::Result<Option<Table>> {
        let vote: anyhow::Result<&actix_web::web::Bytes> = match frame {
            ws::Frame::Text(text) => Ok(text),
            ws::Frame::Binary(_)
            | ws::Frame::Continuation(_)
            | ws::Frame::Ping(_)
            | ws::Frame::Pong(_)
            | ws::Frame::Close(_) => anyhow::bail!("Not a `ws::Frame::Text`"),
        };

        let err_or_table: GetResultResponse = vote.and_then(|vote| {
            Ok(serde_json::from_str::<GetResultResponse>(str::from_utf8(
                &vote,
            )?)?)
        })?;
        log::debug!("{:?}", err_or_table);
        match err_or_table {
            GetResultResponse::Ok(table) => Ok(table),
            GetResultResponse::Err(err) => anyhow::bail!(err),
        }
    }
}

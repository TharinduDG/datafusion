use std::sync::Arc;
use arrow::array::Array;
use arrow::ipc::root_as_message;

use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use arrow_flight::{PollInfo, SchemaAsIpc};
use arrow_flight::{
    Action,
    ActionType, Criteria, Empty, flight_service_server::FlightService, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tonic::codegen::tokio_stream;
use tonic::codegen::tokio_stream::StreamExt;

use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::prelude::*;

#[derive(Clone)]
pub struct FlightServiceImpl {}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let request = request.into_inner();

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let table_path =
            ListingTableUrl::parse(&request.path[0]).map_err(to_tonic_err)?;

        let ctx = SessionContext::new();
        let schema = listing_options
            .infer_schema(&ctx.state(), &table_path)
            .await
            .unwrap();

        let options = IpcWriteOptions::default();
        let schema_result = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        match std::str::from_utf8(&ticket.ticket) {
            Ok(sql) => {
                println!("do_get: {sql}");

                // create local execution context
                let session_config = SessionConfig::new().with_batch_size(4096);
                let ctx = SessionContext::new_with_config(session_config);

                let testdata = datafusion::test_util::parquet_test_data();

                ctx.register_parquet(
                    "taxi",
                    &format!("{testdata}/nyc-taxi"),
                    ParquetReadOptions::default(),
                )
                .await
                .map_err(to_tonic_err)?;

                // create the DataFrame
                let df = ctx.sql(sql).await.map_err(to_tonic_err)?;

                // execute the query
                let schema = df.schema().clone().into();
                let mut stream = df.execute_stream().await.map_err(to_tonic_err)?;
                let mut batch_count = 0;
                let mut header_sent = false;

                let (tx, rx) = mpsc::channel::<Result<FlightData, Status>>(5);

                // Spawn a new task to convert RecordBatch into FlightData
                tokio::spawn(async move {
                    while let Some(batch) = stream.next().await {
                        match batch {
                            Ok(batch) => {
                                let columns = batch.columns().iter().map(|c| c.data_type().to_string()).collect::<Vec<_>>().join(", ");
                                // println!("columns: {:?}", columns);

                                // add an initial FlightData message that sends schema
                                let options = IpcWriteOptions::default();
                                let schema_flight_data = SchemaAsIpc::new(&schema, &options);
                                let mut flights = if header_sent {
                                    vec![]
                                } else {
                                    header_sent = true;
                                    vec![FlightData::from(schema_flight_data)]
                                };

                                let encoder = IpcDataGenerator::default();
                                let mut tracker = DictionaryTracker::new(false);

                                match encoder.encoded_batch(&batch, &mut tracker, &options) {
                                    Ok((flight_dictionaries, flight_batch)) => {
                                        flights.extend(flight_dictionaries.into_iter().map(Into::into));
                                        flights.push(flight_batch.into());

                                        for flight in flights {
                                            // println!("Descriptor: {:?}", flight.flight_descriptor);
                                            // println!("Header: {:?}", root_as_message(flight.data_header[..].as_ref()).unwrap());

                                            if let Err(e) = tx.send(Ok(flight)).await {
                                                eprintln!("Error sending flight data: {}", e);
                                                return;
                                            } else {
                                                batch_count += 1;
                                                println!("Streamed batch {}", batch_count)
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Error encoding batches: {}", e);
                                    }
                                }

                            }
                            Err(e) => {
                                eprintln!("Error executing query: {}", e);
                                return;
                            }
                        }
                    }
                });

                Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
        }
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

fn to_tonic_err(e: datafusion::error::DataFusionError) -> Status {
    Status::internal(format!("{e:?}"))
}


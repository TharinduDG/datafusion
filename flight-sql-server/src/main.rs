use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;
use crate::flight_server::FlightServiceImpl;

mod flight_sql_server;
mod flight_server;

/// This example shows how to wrap DataFusion with `FlightService` to support looking up schema information for
/// Parquet files and executing SQL queries against them on a remote server.
/// This example is run along-side the example `flight_client`.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = FlightServiceImpl {};

    let svc = FlightServiceServer::new(service);

    println!("Listening on {addr:?}");

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

use std::collections::HashMap;
use std::sync::Arc;

use arrow::ipc::root_as_message;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use arrow_flight::utils::flight_data_to_arrow_batch;

use datafusion::arrow::datatypes::Schema;

/// This example shows how to wrap DataFusion with `FlightService` to support looking up schema information for
/// Parquet files and executing SQL queries against them on a remote server.
/// This example is run along-side the example `flight_server`.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let testdata = datafusion::test_util::parquet_test_data();

    // Create Flight client
    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    // Call do_get to execute a SQL query and receive results
    let request = tonic::Request::new(Ticket {
        ticket: "SELECT * FROM taxi".into(),
    });

    let mut stream = client.do_get(request).await?.into_inner();

    // the schema should be the first message returned, else client should error
    let flight_data = stream.message().await?.unwrap();
    // convert FlightData to a stream
    let schema = Arc::new(Schema::try_from(&flight_data)?);
    println!("Schema: {schema:?}");

    // all the remaining stream messages should be dictionary and record batches
    let mut results = vec![];
    let dictionaries_by_field = HashMap::new();
    while let Some(flight_data) = stream.message().await? {
        let message = root_as_message(&flight_data.data_header[..]);
        let header = message.ok().iter().flat_map(|m| m.header_type().variant_name()).collect::<String>();

        // println!("Header: {:?}", header);
        match flight_data_to_arrow_batch(
            &flight_data,
            schema.clone(),
            &dictionaries_by_field,
        ) {
            Ok(record_batch) => results.push(record_batch),
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
        println!("Fetched {} rows", results.iter().map(|b| b.num_rows()).sum::<usize>());
    }

    // print the results
    // pretty::print_batches(&results)?;

    Ok(())
}

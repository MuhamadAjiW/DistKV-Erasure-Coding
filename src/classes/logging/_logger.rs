// // Work in progress

// use crate::base_libs::network::_address::Address;
// use std::io;
// use tracing_subscriber::{
//     fmt::{format::FmtSpan, layer},
//     layer::SubscriberExt,
//     registry,
//     util::SubscriberInitExt,
//     Layer, // <--- IMPORT THE LAYER TRAIT HERE
// };
// // Assuming tracing_appender is a dependency in Cargo.toml

// pub fn setup_node_logger(addr: Address) {
//     let log_filename = format!("node_{}_{}.log", addr.ip, addr.port);

//     let file_appender = tracing_appender::rolling::never("./log", &log_filename);

//     let stdio_layer = layer()
//         .with_file(true)
//         .with_line_number(true)
//         .with_thread_ids(true)
//         .with_target(false)
//         .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
//         // Stdio specific settings
//         .with_writer(io::stdout)
//         .with_filter(tracing::Level::INFO); // .with_filter requires the Layer trait

//     // --- Configure File Layer ---
//     // Configure this layer independently
//     let file_layer = layer() // Start configuration *again* for the file layer
//         .with_file(true)
//         .with_line_number(true)
//         .with_thread_ids(true)
//         .with_target(false)
//         .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
//         // File specific settings
//         .with_ansi(false) // Disable ANSI codes in file
//         .with_writer(file_appender)
//         .with_filter(tracing::Level::DEBUG); // .with_filter requires the Layer trait

//     // --- Combine and Initialize ---
//     registry().with(stdio_layer).with(file_layer).init();

//     println!(
//         "[INIT] Logger initialized. Logging INFO+ to stdout and DEBUG+ to file: ./log/{}", // Adjusted path
//         log_filename
//     );
// }

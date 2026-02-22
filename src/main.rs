
use log::{debug, error, info, warn};
use argparse::{ArgumentParser, Store, StoreTrue};
use env_logger::Env;


mod utils;
use utils::process_ntfs_info::*;

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let mut input = String::new();
    let mut output = String::new();
    let mut scan_depth: i32 = 2;  // Default value for scan_depth

    let mut ntfs_info_pattern = "^NTFSInfo.*csv$".to_string();  // Default value for ntfs_info_pattern
    let mut endpoint_name = String::new(); // Default empty hostname
    let mut use_getthis = false; // flag to toggle GetThis processing

    {
        // this block limits scope of borrows by ap.refer() method
        let mut ap = ArgumentParser::new();
        ap.set_description("Command line tool for processing files.");

        ap.refer(&mut input)
            .add_option(&["-i", "--input"], Store, "Input file path")
            .required();

        ap.refer(&mut output)
            .add_option(&["-o", "--output"], Store, "Output directory path")
            .required();

        ap.refer(&mut scan_depth)
            .add_option(&["-d", "--depth"], Store, "Optional: Depth for scanning (integer)");

        ap.refer(&mut ntfs_info_pattern)
            .add_option(&["--ntfs-info-pattern"], Store, "Optional: NTFS (or GetThis if --use-getthis) info pattern");

        ap.refer(&mut endpoint_name)
            .add_option(&["-e", "--endpoint_name"], Store, "Optional: Endpoint Name. Usefull for offline ORC collection. Will replace hosntame in volstats.csv. Work only for single triage.");
        ap.refer(&mut use_getthis)
            .add_option(&["--use-getthis"], StoreTrue, "Use GetThis CSV files instead of NTFSInfo files to restore the filesystem structure");

        ap.parse_args_or_exit();
    }
    // If GetThis mode is enabled, override the default pattern so that find_ntfs_info searches GetThis CSVs
    if use_getthis {
        ntfs_info_pattern = "GetThis.*\\.csv$".to_string();
    }

    info!("Input: {}", input);
    info!("Output: {}", output);
    info!("Scan depth: {:?}", scan_depth);
    info!("NTFS Info Pattern: {}", ntfs_info_pattern);
    info!("Offline hostname: {}", endpoint_name);
    info!("Use GetThis mode: {}", use_getthis);

    let mut dirs_containing_ntfsinfo = find_ntfs_info(&input, &ntfs_info_pattern).unwrap();  // identify the directories contains NTFSInfo files
    info!("Directories containing matching ({:}) files : {:?}", ntfs_info_pattern, dirs_containing_ntfsinfo);

    for dir_containing_ntfsinfo in dirs_containing_ntfsinfo{
        let result = process_ORC_triage(dir_containing_ntfsinfo, &ntfs_info_pattern, scan_depth, &output, &endpoint_name, use_getthis);
        match result {
            Ok(_) => {
                info!("Filesystem structure successfuly restored");
            }
            Err(mut e) => {
                error!("Failed to restored filesystem structure. Error: {}", e);
            }
        };
    }
}
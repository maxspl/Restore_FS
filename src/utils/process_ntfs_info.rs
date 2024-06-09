use walkdir::WalkDir;
use regex::Regex;
use std::collections::HashSet;
use std::path::PathBuf;
use log::{debug, error, info, warn};
use polars::prelude::*;
use glob::{glob_with, MatchOptions};
use std::fs::File;
use std::any::type_name;
use std::fs;
use std::path::Path;

const DEBUG_ENABLED: bool = cfg!(debug_assertions);

pub fn find_ntfs_info(input: &str, ntfs_info_pattern: &str) -> Result<HashSet<PathBuf>, walkdir::Error> {
    // Return directories containing NTFSInfo files
    let regex = Regex::new(ntfs_info_pattern).expect("Invalid regex pattern");
    let mut matching_dirs = HashSet::new();
    for entry in WalkDir::new(input) {
        let entry = entry?;
        if entry.file_type().is_file() && regex.is_match(entry.file_name().to_string_lossy().as_ref()) {
            matching_dirs.insert(entry.path().parent().unwrap_or(entry.path()).to_path_buf());
        }
    }

    Ok(matching_dirs)
}

pub fn process_ORC_triage(dir: PathBuf, ntfs_info_pattern: &str, depth: i32, output: &str) -> Result<HashSet<PathBuf>, Box<dyn std::error::Error>> {
    info!("Processing directory: {:?}", dir);
    let regex = Regex::new(ntfs_info_pattern).expect("Invalid regex pattern");
    let mut matching_files = HashSet::new();
    for entry in WalkDir::new(dir.clone()) {
        let entry = entry?;
        if entry.file_type().is_file() && regex.is_match(entry.file_name().to_string_lossy().as_ref()) {
            matching_files.insert(entry.path().to_path_buf());
        }
    }
    info!("matching files : {:?}", matching_files);

    // Construct Dataframe from files on disk
    let parent_dir = get_parent_directory(&dir, depth); // Path containing ORC extraction
    info!{"parent_dir (this path should contain extracted file of an endpoint): {:?}", parent_dir};
    let mut files_extracted_df = build_dataframe_from_extracted_triage(&parent_dir.unwrap())?;
    // if DEBUG_ENABLED {
    //     debug!("Dataframe containing scanned files on disk is writed to scanned_files.csv");
    //     let mut file = std::fs::File::create("scanned_files.csv").unwrap();
    //     CsvWriter::new(&mut file).finish(&mut files_extracted_df).unwrap();
    // }

    // Construct Dataframe from volstats.csv
    let volstats_df = build_volstats_df(&dir);

    // Construct Dataframes from each NTFSInfo file
    let mut ntfs_dataframes_result = build_dataframes_from_ntfs(matching_files.clone(), volstats_df);

    // Join each ntfsinfo DF with the scanned files DF
    if let Ok(ntfs_dataframes) = &ntfs_dataframes_result {        
        for (i, ntfs_dataframe) in ntfs_dataframes.iter().enumerate() {
            // Join both DF
            let mut df_joined = ntfs_dataframe
            .clone()
            .lazy()
            .join(
                files_extracted_df.clone().lazy(),
                vec![col("VolumeID"), col("FRN"), col("ParentFRN")],
                vec![col("VolumeID"), col("FRN"), col("ParentFRN")],
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
            
            // Add a column: concat(MountPoint (remove trailing \ of mountpoint), ParentName (replace \ with / and remove first /), File)
            df_joined = df_joined
            .clone()
            .lazy()
            .with_columns([
                col("ParentName").str().replace_all(lit("\\\\"), lit("/"), false),
                concat_str([
                    lit(output),
                    col("ComputerName"),
                    col("MountPoint").str().replace_all(lit("\\\\"), lit(""), false), 
                    col("ParentName").str().extract(lit(r".(.*)"), 1).str().replace_all(lit("\\\\"), lit("/"), false), 
                    col("File")], "/", true)
                .alias("restored_path")
            ])
            .collect()?;

            if DEBUG_ENABLED {
                debug!("Final df : {}", df_joined);
                // debug!("Final dataframe containing joined scanned_df and scanned_df is written to {:?}", format!("final_df_{}.csv", i,));
                // let mut file = std::fs::File::create(format!("final_df_{}.csv", i,)).unwrap();
                // CsvWriter::new(&mut file).finish(&mut df_joined).unwrap();
            }

            let result = restore_fs(df_joined);
            match result{
                Ok(_) => {
                    debug!("Restored action successful.");
                } 
                Err(e) => {
                    error!("Failed to restored filesystem structure. Error: {}", e);
                }
            }
        }
    } else if let Err(e) = &ntfs_dataframes_result {
        error!("failed to construct DataFrames NTFSInfo files. Error: {}", e);
    }


    Ok(matching_files)
}


fn restore_fs(df: DataFrame) -> Result<bool, Box<dyn std::error::Error>> {
    // Return true if successfully restored the filesystem structure by copying files to new destination
    let source_paths = df.column("file_path")?;
    let destination_paths = df.column("restored_path")?;

    for (source, destination) in source_paths.str()?.into_iter().zip(destination_paths.str()?.into_iter()) {
        if let (Some(src), Some(dest)) = (source, destination) {
            let src_path = Path::new(src);
            let dest_path = Path::new(dest);

            // Create parent directories if they don't exist
            if let Some(parent) = dest_path.parent() {
                if let Err(e) = fs::create_dir_all(parent) {
                    debug!("Failed to create directory {:?}: {}", parent, e);
                    continue; // Skip to the next file
                }
            }

            // Copy the file
            if let Err(e) = fs::copy(src_path, dest_path) {
                debug!("Failed to copy file from {:?} to {:?}: {}", src_path, dest_path, e);
                continue; // Skip to the next file
            }
        }
    }
    Ok(true)
}

fn get_parent_directory(path: &PathBuf, depth: i32) -> Option<PathBuf> {
    // Returns the parent directory of given depth
    let mut current = path.to_path_buf();
    for _ in 0..depth {
        if let Some(parent) = current.parent() {
            current = parent.to_path_buf();
        } else {
            return None; // Return None if we try to go beyond the root
        }
    }

    Some(current)
}

fn build_dataframe_from_extracted_triage(path: &PathBuf) -> Result<DataFrame, walkdir::Error> {
    // Returns a dataframe containing all files in the path to scan on disk
    // Regex pattern to match files collected by DFIR ORC
    let regex = Regex::new("^[a-fA-F0-9]+_[a-fA-F0-9]+_[a-fA-F0-9]+_\\w+").expect("Invalid regex pattern"); // Pattern match VolumeID_FRN_PARENTFRN
    // Regex pattern to capture VolumeID, ParentFRN, and FRN
    let regex_extract = Regex::new(r"^([a-zA-Z0-9]+)_([a-zA-Z0-9]+)_([a-zA-Z0-9]+)").expect("Invalid regex pattern");
    let mut file_paths: Vec<String> = Vec::new();
    let mut file_names: Vec<String> = Vec::new();
    let mut volume_ids: Vec<String> = Vec::new();
    let mut parent_frns: Vec<String> = Vec::new();
    let mut frns: Vec<String> = Vec::new();
    
    for entry in WalkDir::new(path) {
        let entry = entry?;
        if entry.file_type().is_file()  && regex.is_match(entry.file_name().to_string_lossy().as_ref()) {
            let file_name = entry.file_name().to_string_lossy().to_string();
            if let Some(caps) = regex_extract.captures(&file_name) {
                file_paths.push(entry.path().display().to_string());
                file_names.push(file_name.clone());

                // Append "0x" at the beginning of each string
                volume_ids.push(format!("0x{}", caps.get(1).unwrap().as_str()));
                parent_frns.push(format!("0x{}", caps.get(2).unwrap().as_str()));
                frns.push(format!("0x{}", caps.get(3).unwrap().as_str()));
            }
        }

    }

    let file_paths_series = Series::new("file_path", file_paths);
    let file_names_series = Series::new("file", file_names);
    let volume_ids_series = Series::new("VolumeID", volume_ids);
    let parent_frns_series = Series::new("ParentFRN", parent_frns);
    let frns_series = Series::new("FRN", frns);

    let df = DataFrame::new(vec![
        file_paths_series,
        file_names_series,
        volume_ids_series,
        parent_frns_series,
        frns_series,
    ]);
    
    let mut scanned_files_df = df.unwrap().clone();
    let mut cols_to_convert = vec!["VolumeID", "ParentFRN", "FRN"];
    let mut scanned_files_df_transformed = from_strHex_to_int(&scanned_files_df, cols_to_convert);
    Ok(scanned_files_df_transformed.unwrap())
}

fn build_volstats_df(dir: &PathBuf) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Returns dataframe containing volstats data
    let mut volstats_path = dir.clone();
    volstats_path.push("volstats.csv");

    // Check if the file exists
    if !volstats_path.exists() {
        return Err(format!("File {:?} does not exist.", volstats_path).into());
    }

    // Construct DataFrame
    let q = LazyCsvReader::new(volstats_path)
        .with_has_header(true)
        .finish()?;
    
    let mut df = q.collect()?;
    df = df.lazy().select([cols(["VolumeID", "MountPoint"])]).collect()?;
    let mut df_back = df.clone();


    // Convert VolumeID column to int    
    let mut cols_to_convert = vec!["VolumeID"];
    let mut df_modified = from_strHex_to_int(&df_back, cols_to_convert);
    debug!("volstats df : {:?}", df_modified);

    Ok(df_modified.unwrap().clone())

}

fn from_strHex_to_int(df: &DataFrame, columns: Vec<&str>) -> Result<DataFrame, Box<dyn std::error::Error>>{
    // Returns dataframe modified: all given columns are convert from 0x hex to int
    // Extracting the "VolumeID" column as a vector of string slices
    let mut new_df = df.clone(); ;
    for col in columns {
        let mut column_values: Vec<&str> = new_df
            .column(col)
            .unwrap() // Handle possible error when fetching the column
            .str() // Convert the datatype to Utf8, which contains string data
            .unwrap() // Handle possible error from conversion
            .into_iter()
            .flatten() // Convert Option<&str> to &str, removing None values
            .collect(); // Collect into a vector
        
        // Transform hex values to int
        let mut column_transformed: Vec<u64> = 
        column_values.iter()
            .map(|x: &&str| { 
                let mut trimmed = x.trim_start_matches("0x").to_string();
                let z = u64::from_str_radix(&trimmed, 16).unwrap();
                z
            }).collect();

        // Create a new Series from the modified values
        let mut tranformed_column_series = Series::new(col, column_transformed);
       
        // Replace the old `VolumeID` column in the DataFrame with the new one
        new_df.with_column(tranformed_column_series)?;
    }

    Ok(new_df.clone())
}

fn build_dataframes_from_ntfs(matching_files: HashSet<PathBuf>, volstats_result: Result<DataFrame, Box<dyn std::error::Error>>) -> Result<Vec<DataFrame>, Box<dyn std::error::Error>>{
    // Returns list of dataframes from each NTFSInfo file
    let mut ntfs_dataframes: Vec<DataFrame> = Vec::new();
    for file_path in &matching_files {
        
        // Open NTFSInfo csv
        let q = LazyCsvReader::new((file_path))
            .with_has_header(true)
            .finish()?;
        let mut df_ntfsinfo = q.collect()?;
        df_ntfsinfo = df_ntfsinfo.clone().lazy().select([cols([
                                            "ComputerName",
                                            "VolumeID",
                                            "File",
                                            "ParentName",
                                            "FullName",
                                            "FRN",
                                            "ParentFRN"])]).collect()?;
        
        // Convert DataFrame columm
        let mut cols_to_convert = vec!["VolumeID", "ParentFRN", "FRN"];
        let mut df_ntfsinfo_transformed = from_strHex_to_int(&df_ntfsinfo, cols_to_convert);
        let mut df_ntfsinfo_transformed_cloned = df_ntfsinfo_transformed.unwrap().clone();

        // If volstats exists and loaded, try to map VolumeID with MountPoint
        if let Ok(volstats_df) = &volstats_result { 
            debug!("Volstats : {:?}", volstats_df);
            // Only attempt to use volstats_df if it is Ok, otherwise skip the join
            df_ntfsinfo_transformed_cloned = df_ntfsinfo_transformed_cloned
            .clone()
            .lazy()
            .join(
                volstats_df.clone().lazy(),
                [col("VolumeID")],
                [col("VolumeID")],
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
            

        } else if let Err(e) = &volstats_result {
            error!("Error: {}", e);
        }

        debug!("df_ntfsinfo_transformed_cloned {}", df_ntfsinfo_transformed_cloned);


        ntfs_dataframes.push(df_ntfsinfo_transformed_cloned);
    }
    Ok(ntfs_dataframes)
}

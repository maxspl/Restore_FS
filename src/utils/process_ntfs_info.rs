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

pub fn process_ORC_triage(dir: PathBuf, ntfs_info_pattern: &str, depth: i32, output: &str, endpoint_name: &str, use_getthis: bool) -> Result<HashSet<PathBuf>, Box<dyn std::error::Error>> {
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
    let endpoint_root = parent_dir.clone().ok_or("Failed to determine endpoint root directory")?;

    info!{"parent_dir (this path should contain extracted file of an endpoint): {:?}", parent_dir};
    let files_extracted_df = build_dataframe_from_extracted_triage(&endpoint_root)?;
    // Construct Dataframe from volstats.csv
    let volstats_df = build_volstats_df(&endpoint_root);

    // Construct Dataframes from each NTFSInfo file
    // Depending on whether we are processing GetThis or NTFSInfo files, use the appropriate builder
    let ntfs_dataframes_result = if use_getthis {
        build_dataframes_from_getthis(matching_files.clone(), volstats_df)
    } else {
        build_dataframes_from_ntfs(matching_files.clone(), volstats_df)
    };
    // Join each ntfsinfo DF with the scanned files DF
    if let Ok(ntfs_dataframes) = &ntfs_dataframes_result {        
        for ntfs_dataframe in ntfs_dataframes.iter() {
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
            // Build the restored_path column.  Use the provided endpoint_name to
            // override ComputerName when non-empty.
            // For GetThis we rely on the FullName-derived ParentName and File
            // columns.  In both modes we convert backslashes to forward slashes
            // and drop leading separators.
            let endpoint_expr = if endpoint_name.is_empty() {
                col("ComputerName")
            } else {
                lit(endpoint_name)
            };
            df_joined = df_joined
                .clone()
                .lazy()
                .with_columns([
                    col("ParentName").str().replace_all(lit("\\\\"), lit("/"), false),
                    // Build restored_path by concatenating output, endpoint name, mount point (without trailing colon/backslash), parent name and file
                    concat_str(
                        [
                            lit(output),
                            endpoint_expr.clone(),
                            col("MountPoint").str().replace_all(lit(":\\\\"), lit(""), false),
                            col("ParentName")
                                .str()
                                .replace(lit(r"^\\\\"), lit(""), false)     // only drop leading '\'
                                .str()
                                .replace_all(lit("\\\\"), lit("/"), false), // then normalize slashes
                            col("File")
                        ],
                        "/",
                        true,
                    )
                    .alias("restored_path"),
                ])
                .sort(
                    ["restored_path"],
                    SortMultipleOptions::default()
                        .with_order_descending(true)
                        .with_nulls_last(true),
                )
                .collect()?;

            if DEBUG_ENABLED {
                debug!("Final df : {}", df_joined);
            }

            let result = restore_fs(df_joined);
            match result {
                Ok(_) => {
                    debug!("Restored action successful.");
                } 
                Err(e) => {
                    error!("Failed to restored filesystem structure. Error: {}", e);
                }
            }
        }
    } else if let Err(e) = &ntfs_dataframes_result {
        error!("failed to construct DataFrames: {}", e);
    }


    Ok(matching_files)
}
/// Restore the filesystem structure on disk.  For every source file path found
/// in the joined DataFrame, copy it to its corresponding destination under
/// restored_path.  Directories are created on demand.  Files residing in
/// extAttrs are skipped as a best-effort to avoid copying extended attribute
/// files that are not part of the regular filesystem structure.
fn restore_fs(df: DataFrame) -> Result<bool, Box<dyn std::error::Error>> {
    // Return true if successfully restored the filesystem structure by copying files to new destination
    let source_paths = df.column("file_path")?;
    let destination_paths = df.column("restored_path")?;

    for (source, destination) in source_paths.str()?.into_iter().zip(destination_paths.str()?.into_iter()) {
        if let (Some(src), Some(dest)) = (source, destination) {
            let src_path = Path::new(src);
            let dest_path = Path::new(dest);

            //Skip files in the dir exAttrs 
            if let Some(parent) = src_path.parent() {
                if let Some(parent_str) = parent.to_str() {
                    if parent_str.ends_with("extAttrs") {
                        debug!("Skipping file in 'extAttrs' directory: {:?}", src_path);
                        continue; // Skip this file and move to the next one
                    }
                }
            }

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

/// Compute the ancestor of a given path at the specified depth.  Used to
/// identify the root directory of the ORC triage relative to a NTFSInfo or
/// GetThis CSV file.
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

/// Build a DataFrame representing the set of files extracted by DFIR ORC.  The
/// file names follow a strict pattern of `VolumeID_FRN_PARENTFRN_*`.  From each
/// filename we extract the VolumeID, FRN and ParentFRN and convert them from
/// hexadecimal strings to integers.  This DataFrame is used later to join
/// against the NTFSInfo/GetThis dataframes.
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
        if entry.file_type().is_file() && regex.is_match(entry.file_name().to_string_lossy().as_ref()) {
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
        frns_series
    ]);
    
    let scanned_files_df = df.unwrap().clone();
    let cols_to_convert = vec!["VolumeID", "ParentFRN", "FRN"];
    let scanned_files_df_transformed = from_strHex_to_int(&scanned_files_df, cols_to_convert);
    Ok(scanned_files_df_transformed.unwrap())
}

fn find_volstats_path(base_dir: &PathBuf) -> Option<PathBuf> {
    for entry in WalkDir::new(base_dir) {
        if let Ok(e) = entry {
            if e.file_type().is_file()
                && e.file_name().to_string_lossy().eq_ignore_ascii_case("volstats.csv")
            {
                return Some(e.path().to_path_buf());
            }
        }
    }
    None
}

/// Build a DataFrame from volstats.csv if present.  The volume statistics file
/// maps a volume ID to its mount point.  We extract only those two columns
/// (VolumeID and MountPoint) and convert the VolumeID from hex string to
/// integer.  If the file does not exist, an error is returned.
fn build_volstats_df(base_dir: &PathBuf) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let volstats_path = find_volstats_path(base_dir)
        .ok_or_else(|| format!("volstats.csv not found under {:?}", base_dir))?;

    let q = LazyCsvReader::new(volstats_path)
        .with_has_header(true)
        .finish()?;

    let mut df = q.collect()?;
    df = df.lazy().select([cols(["VolumeID", "MountPoint"])]).collect()?;

    let cols_to_convert = vec!["VolumeID"];
    let df_modified = from_strHex_to_int(&df, cols_to_convert)?;
    Ok(df_modified)
}

/// Convert a list of columns from hexadecimal strings (prefixed with 0x) into
/// integers.  The returned DataFrame is a clone of the input but with the
/// specified columns replaced by integer-typed columns.
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

/// Build dataframes from NTFSInfo CSV files.  For each file we select the
/// relevant columns, fill in missing VolumeID values from the filename when
/// necessary, convert the FRN-related fields from hex to integers and join
/// with the volstats DataFrame to attach mount points.
fn build_dataframes_from_ntfs(matching_files: HashSet<PathBuf>, volstats_result: Result<DataFrame, Box<dyn std::error::Error>>) -> Result<Vec<DataFrame>, Box<dyn std::error::Error>>{
    // Returns list of dataframes from each NTFSInfo file
    let mut ntfs_dataframes: Vec<DataFrame> = Vec::new();
    for file_path in &matching_files {
        debug!("Processing NTFSInfo file : {}", file_path.display());
        // Open NTFSInfo csv
        let q = LazyCsvReader::new((file_path))
            .with_has_header(true)
            .with_ignore_errors(true)
            .finish()?;
        let mut df_ntfsinfo = q.collect()?;
    df_ntfsinfo = df_ntfsinfo
            .lazy()
            .select([cols([
                "ComputerName",
                "VolumeID",
                "File",
                "ParentName",
                "FullName",
                "FRN",
                "ParentFRN",
            ])])
            .collect()?;
        // Fill missing VolumeID values by parsing the filename
        let filename = file_path.file_name().unwrap().to_string_lossy().to_string();
        let re_volid = Regex::new(r"0x[0-9a-fA-F]+").unwrap();
        let volume_id_from_filename = re_volid.find(&filename).map(|m| m.as_str().to_string());
        if let Some(ref vol_fallback) = volume_id_from_filename {
            let vol_str = vol_fallback.clone();
            let volume_series: Vec<String> = df_ntfsinfo
                .column("VolumeID")
                .unwrap()
                .str()
                .unwrap()
                .into_iter()
                .map(|opt: Option<&str>| match opt {
                    Some(v) if !v.is_empty() => v.to_string(),
                    _ => vol_str.clone(),
                })
                .collect();
            let series = Series::new("VolumeID", volume_series);
            df_ntfsinfo.with_column(series)?;
        }
        // Convert FRN-related columns to integers
        let cols_to_convert = vec!["VolumeID", "ParentFRN", "FRN"];
        let df_ntfsinfo_transformed = from_strHex_to_int(&df_ntfsinfo, cols_to_convert)?;
        let mut df_transformed_cloned = df_ntfsinfo_transformed.clone();
        // Map VolumeID to MountPoint when volstats exists
        if let Ok(volstats_df) = &volstats_result {
            df_transformed_cloned = df_transformed_cloned
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
        ntfs_dataframes.push(df_transformed_cloned);
    }
    Ok(ntfs_dataframes)
}


/// Build dataframes from GetThis CSV files.  Each GetThis file has a similar
/// schema to NTFSInfo but does not provide separate ParentName and File
/// columns.  Instead we derive those fields from the FullName column.  We
/// also handle missing VolumeID values by parsing the filename and map the
/// VolumeID to mount points when possible.
fn build_dataframes_from_getthis(
    matching_files: HashSet<PathBuf>,
    volstats_result: Result<DataFrame, Box<dyn std::error::Error>>,
) -> Result<Vec<DataFrame>, Box<dyn std::error::Error>> {
    let mut dataframes: Vec<DataFrame> = Vec::new();
    for file_path in &matching_files {
        debug!("Processing GetThis file : {}", file_path.display());
        // Open the GetThis CSV
        let q = LazyCsvReader::new(file_path)
            .with_has_header(true)
            .with_ignore_errors(true)
            .finish()?;
        let mut df_get = q.collect()?;
        // Trim to the columns we need.  Some GetThis files may not include all of
        // these columns, so missing columns will be filled with nulls by select().
        df_get = df_get
            .lazy()
            .select([cols([
                "ComputerName",
                "VolumeID",
                "ParentFRN",
                "FRN",
                "FullName",
            ])])
            .collect()?;
        // Parse fallback VolumeID from the filename
        let filename = file_path.file_name().unwrap().to_string_lossy().to_string();
        let re_volid = Regex::new(r"0x[0-9a-fA-F]+").unwrap();
        let volume_id_from_filename = re_volid.find(&filename).map(|m| m.as_str().to_string());
        if let Some(ref vol_fallback) = volume_id_from_filename {
            let vol_str = vol_fallback.clone();
            let volume_series: Vec<String> = df_get
                .column("VolumeID")
                .unwrap()
                .str()
                .unwrap()
                .into_iter()
                .map(|opt: Option<&str>| match opt {
                    Some(v) if !v.is_empty() => v.to_string(),
                    _ => vol_str.clone(),
                })
                .collect();
            let series = Series::new("VolumeID", volume_series);
            df_get.with_column(series)?;
        }
        // Derive ParentName and File columns from FullName.  FullName is a
        // Windows path prefixed with a backslash.  Remove the leading
        // backslash, split on the last backslash and assign the parts accordingly.
        let full_names: Vec<Option<&str>> = df_get
            .column("FullName")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        let mut parent_names: Vec<String> = Vec::new();
        let mut file_names: Vec<String> = Vec::new();
        for maybe in full_names {
            if let Some(name) = maybe {
                let trimmed = name.trim_start_matches('\\');
                if let Some(pos) = trimmed.rfind('\\') {
                    let (parent, file) = trimmed.split_at(pos);
                    let file_name = &trimmed[pos + 1..];
                    parent_names.push(parent.to_string());
                    file_names.push(file_name.to_string());
                } else {
                    parent_names.push(String::new());
                    file_names.push(trimmed.to_string());
                }
            } else {
                parent_names.push(String::new());
                file_names.push(String::new());
            }
        }
        df_get.with_column(Series::new("ParentName", parent_names))?;
        df_get.with_column(Series::new("File", file_names))?;
        // Convert hex columns to integers
        let cols_to_convert = vec!["VolumeID", "ParentFRN", "FRN"];
        let df_transformed = from_strHex_to_int(&df_get, cols_to_convert)?;
        let mut df_transformed_cloned = df_transformed.clone();
        // Join with volstats to map MountPoint
        if let Ok(volstats_df) = &volstats_result {
            df_transformed_cloned = df_transformed_cloned
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
        dataframes.push(df_transformed_cloned);
    }
    Ok(dataframes)
}
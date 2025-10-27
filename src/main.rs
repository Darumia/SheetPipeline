use csv::ReaderBuilder;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashMap, fs, path::PathBuf};

#[derive(Deserialize)]
struct AppConfig {
    watch_path: String,
    output_path:String,
    root_name: Option<String>,
    mappings: HashMap<String, String>,
}

fn load_config() -> AppConfig {
    let content = fs::read_to_string("Config.toml")
        .expect("Could not find config.toml in the working directory");
    toml::from_str(&content).expect("TOML is invalid")
}

fn csv_to_json(file_path: &PathBuf, config: &AppConfig) -> String{
    // ReaderBuilder needs more options, like delimiter, encoding,and if the csv is has key horizontal or
    // vertial 
    let mut rdr = ReaderBuilder::new()
        .flexible(true)
        .from_path(file_path)
        .expect("Unable to read CSV file");
    let headers = rdr.headers().unwrap().clone();

    let mut root = json!({});
    for res in rdr.records() {
        let record = res.unwrap();
        for (i, value) in record.iter().enumerate() {
            let header = &headers[i];
            let mapped_key = config.mappings.get(header).unwrap(); // TODO proper error handling
            // Function here to make the actual record and add to root.
            insert_in_root(&mut root, mapped_key, value);
        }
    }
        let json = serde_json::to_string_pretty(&root).unwrap();
        return json
}

fn insert_in_root(root: &mut Value, key: &str, value: &str) {
    let mut root = root;
    let header_parts: Vec<&str> = key.split('.').collect();
    for (i, key) in header_parts.iter().enumerate() {
        //check if its the last, then it needs to add value and not just a new json
        if i == header_parts.len() - 1 {
            if let Some(obj) = root.as_object_mut() {
                obj.insert((*key).to_string(), json!(value));
            }
        } else {
            if !root.get(*key).is_some() {
                if let Some(obj) = root.as_object_mut() {
                    obj.insert((*key).to_string(), json!({}));
                }
            }
            // Make sure to get the root as the new made root to nest it.
            root = root.get_mut(*key).unwrap();
        }
    }
}

fn files_in_input(config: &AppConfig) {
    let x = fs::read_dir(&config.watch_path).expect("Cannot read files in /input");
    for path in x {
        //println!("{:?}",path.unwrap().path().display());
        let file_path = path.unwrap().path();
        let mut file_name = file_path.file_name().unwrap();
        let file_ = file_name.to_str().unwrap().replace(".csv", "");
        let config = load_config();
        let json = csv_to_json(&file_path, &config);
        fs::write(format!("{}.json", config.output_path+"/"+&file_), json).unwrap()
    }
}

#[tokio::main]
async fn main() {
    let config = load_config();
    // If program has restarted and there are files added to folder in meantime they need to be
    // processed.
    files_in_input(&config);

    println!("{}", config.watch_path);
}

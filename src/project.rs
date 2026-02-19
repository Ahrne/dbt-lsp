use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use dashmap::DashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct DbtProjectConfig {
    pub name: String,
    #[serde(rename = "model-paths", default = "default_model_paths")]
    pub model_paths: Vec<String>,
    #[serde(rename = "seed-paths", default = "default_seed_paths")]
    pub seed_paths: Vec<String>,
    #[serde(rename = "macro-paths", default = "default_macro_paths")]
    pub macro_paths: Vec<String>,
}

fn default_model_paths() -> Vec<String> {
    vec!["models".to_string()]
}
fn default_seed_paths() -> Vec<String> {
    vec!["seeds".to_string()]
}
fn default_macro_paths() -> Vec<String> {
    vec!["macros".to_string()]
}

#[derive(Debug, Clone)]
pub struct MacroDef {
    pub path: PathBuf,
    pub line: usize,
}

#[derive(Debug, Clone)]
pub struct ProjectManifest {
    pub root_dir: PathBuf,
    pub config: DbtProjectConfig,
    pub models: DashMap<String, PathBuf>,
    pub sources: DashMap<String, PathBuf>, // source.table -> yml path
    pub seeds: DashMap<String, PathBuf>,
    pub macros: DashMap<String, MacroDef>,
}

impl ProjectManifest {
    pub fn load(root_dir: PathBuf) -> anyhow::Result<Self> {
        let config_path = root_dir.join("dbt_project.yml");
        let content = std::fs::read_to_string(&config_path)?;
        let config: DbtProjectConfig = serde_yaml::from_str(&content)?;

        let manifest = Self {
            root_dir: root_dir.clone(),
            config: config.clone(),
            models: DashMap::new(),
            sources: DashMap::new(),
            seeds: DashMap::new(),
            macros: DashMap::new(),
        };

        manifest.scan_models();
        manifest.scan_seeds();
        manifest.scan_macros();
        manifest.scan_sources();
        Ok(manifest)
    }

    pub fn scan_models(&self) {
        self.models.clear();
        for path in &self.config.model_paths {
            let full_path = self.root_dir.join(path);
            eprintln!("Scanning models in: {:?}", full_path);
            for entry in WalkDir::new(full_path).into_iter().filter_map(|e| e.ok()) {
                if entry.path().extension().map_or(false, |ext| ext == "sql") {
                    if let Some(stem) = entry.path().file_stem() {
                        let model_name = stem.to_string_lossy().to_string();
                        self.models.insert(model_name, entry.path().to_path_buf());
                    }
                }
            }
        }
        eprintln!("Found {} models", self.models.len());
    }

    pub fn scan_seeds(&self) {
        self.seeds.clear();
        for path in &self.config.seed_paths {
            let full_path = self.root_dir.join(path);
            eprintln!("Scanning seeds in: {:?}", full_path);
            for entry in WalkDir::new(full_path).into_iter().filter_map(|e| e.ok()) {
                if entry.path().extension().map_or(false, |ext| ext == "csv") {
                    if let Some(stem) = entry.path().file_stem() {
                        let seed_name = stem.to_string_lossy().to_string();
                        self.seeds.insert(seed_name, entry.path().to_path_buf());
                    }
                }
            }
        }
        eprintln!("Found {} seeds", self.seeds.len());
    }

    pub fn scan_macros(&self) {
        self.macros.clear();
        let macro_regex = regex::Regex::new(r#"(?s)\{%\s*macro\s+([a-zA-Z0-9_]+)\s*\("#).unwrap();

        for path in &self.config.macro_paths {
            let full_path = self.root_dir.join(path);
            eprintln!("Scanning macros in: {:?}", full_path);
            for entry in WalkDir::new(full_path).into_iter().filter_map(|e| e.ok()) {
                if entry.path().extension().map_or(false, |ext| ext == "sql" || ext == "jinja") {
                    if let Ok(content) = std::fs::read_to_string(entry.path()) {
                        for cap in macro_regex.captures_iter(&content) {
                            if let Some(m) = cap.get(1) {
                                let name = m.as_str().to_string();
                                // Calculate line number (naive but works)
                                let line = content[..m.start()].lines().count().saturating_sub(1);
                                self.macros.insert(name, MacroDef {
                                    path: entry.path().to_path_buf(),
                                    line,
                                });
                            }
                        }
                    }
                }
            }
        }
        eprintln!("Found {} macros", self.macros.len());
    }

    pub fn scan_sources(&self) {
        self.sources.clear();
        for path in &self.config.model_paths {
            let full_path = self.root_dir.join(path);
            eprintln!("Scanning sources (YML) in: {:?}", full_path);
            for entry in WalkDir::new(full_path).into_iter().filter_map(|e| e.ok()) {
                if entry.path().extension().map_or(false, |ext| ext == "yml" || ext == "yaml") {
                    if let Ok(content) = std::fs::read_to_string(entry.path()) {
                        if let Ok(val) = serde_yaml::from_str::<serde_yaml::Value>(&content) {
                            if let Some(sources) = val.get("sources").and_then(|s| s.as_sequence()) {
                                for src in sources {
                                    if let Some(src_name) = src.get("name").and_then(|n| n.as_str()) {
                                        if let Some(tables) = src.get("tables").and_then(|t| t.as_sequence()) {
                                            for tbl in tables {
                                                if let Some(tbl_name) = tbl.get("name").and_then(|n| n.as_str()) {
                                                    let full_src_name = format!("{}.{}", src_name, tbl_name);
                                                    self.sources.insert(full_src_name, entry.path().to_path_buf());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("Found {} sources", self.sources.len());
    }
}

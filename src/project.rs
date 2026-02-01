use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use dashmap::DashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct DbtProjectConfig {
    pub name: String,
    #[serde(rename = "model-paths", default = "default_model_paths")]
    pub model_paths: Vec<String>,
    // Add other fields as needed
}

fn default_model_paths() -> Vec<String> {
    vec!["models".to_string()]
}

#[derive(Debug, Clone)]
pub struct ProjectManifest {
    pub root_dir: PathBuf,
    pub config: DbtProjectConfig,
    pub models: DashMap<String, PathBuf>, // Model Name -> Absolute Path
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
        };

        manifest.scan_models();
        Ok(manifest)
    }

    pub fn scan_models(&self) {
        self.models.clear();
        for path in &self.config.model_paths {
            let full_path = self.root_dir.join(path);
            for entry in WalkDir::new(full_path).into_iter().filter_map(|e| e.ok()) {
                if entry.path().extension().map_or(false, |ext| ext == "sql") {
                    if let Some(stem) = entry.path().file_stem() {
                        let model_name = stem.to_string_lossy().to_string();
                        self.models.insert(model_name, entry.path().to_path_buf());
                    }
                }
            }
        }
    }
}

use crate::project::ProjectManifest;
use crate::jinja::DbtRef;
use std::sync::Arc;
use tokio::sync::RwLock;
use dashmap::DashMap;
use ropey::Rope;
use tree_sitter::Tree;
use tower_lsp::lsp_types::{Url, Diagnostic};

#[derive(Debug)]
pub struct DocumentState {
    pub text: Rope,
    pub tree: Option<Tree>,
    pub refs: Vec<(DbtRef, std::ops::Range<usize>)>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Default)]
pub struct GlobalState {
    pub manifest: RwLock<Option<Arc<ProjectManifest>>>,
    pub documents: DashMap<Url, DocumentState>,
}
